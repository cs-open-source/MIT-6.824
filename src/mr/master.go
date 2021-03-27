package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type State int

// The state of the machine (Reduce and Map machine)
const (
	_          State = iota
	Idle            // 空闲
	InProgress       // 正在运行中
	Completed        // 完成
	Error
)


type Phase int

const (
	Map Phase = iota
	Reduce
)

// The task
type Task struct {

	Y int
	// The id of task
	Id int
	// The type of task Map[0] or Reduce[1]
	Type Phase
	// The state of this task, type State
	WorkerState State
    //
    Files []string

	// The number of Reduce worker
	NReduce int

	// The start time of this worker
	StartTime int64

}

type Master struct {

	sync.Mutex

	// 待处理的任务列表
	IdleTasks chan*Task

	// 正在处理的任务列表, key:TaskId, value:The identity of this task
	InProcessTasks map[int]*Task

	// 没有已完成列表，已完成的直接删掉处理

	Intermediates [][]string

	FinalFiles []string

	// the files of all map worker
	Files []string

	// The state of Master worker
	MasterStatus State

	// The current number of Map Workers
	M int64
	// The current number of Reduce Workers
	R int64
	// The  number of Map Workers
	NMap int64
	// The  number of Reduce Workers
	NReduce int64

}


func (m *Master) GetTask(args *TaskArgs, reply *TaskReply) error{

	// if true 代表已经完成
	if reply.MasterState != Completed{

		reply.MasterState = InProgress
		task , ok := <-m.IdleTasks
		if ok{
			task.StartTime = time.Now().Unix()
			task.Id = args.X + 1
			reply.T = task

			m.Lock()
			m.InProcessTasks[task.Y] = task
			m.Unlock()
		}

	}
	return nil
}

func (m *Master) GenerateReduceTasks() {

	for i:=0;i< int(m.NReduce);i++{
		task := Task{Y:i,Type: Reduce,WorkerState: Idle,Files:m.Intermediates[i],NReduce: int(m.NReduce)}
		m.IdleTasks <- &task
	}

}


func (m *Master) GenerateRMapTasks() {

	for i:=0;i< int(m.NMap);i++{
		task := Task{Y:i,Type: Map,WorkerState: Idle,Files:[]string{m.Files[i]},NReduce: int(m.NReduce)}
		m.IdleTasks <- &task
	}

}


func (m *Master) TaskNotify(args *TaskNotifyArgs, reply *TaskNotifyReply) error {

	//fmt.Printf("%v, %v task has finised! \n", args.Y, args.IsReduce)

	m.Lock()
	if val, ok := m.InProcessTasks[args.Y]; ok && val.Id == args.TaskId {
		delete(m.InProcessTasks, args.Y)
		if !args.IsReduce{
			m.M += 1
			for i := 0; i < int(m.NReduce); i++ {
				m.Intermediates[i] = append(m.Intermediates[i], args.Files[i])
			}
			if m.M == m.NMap{
				go m.GenerateReduceTasks()
			}
		}else{
			m.R += 1
			m.FinalFiles = append(m.FinalFiles, args.Files...)
			if m.R == m.NReduce{
				close(m.IdleTasks)
				m.MasterStatus = Completed
			}
		}
	}
	m.Unlock()
	reply.Ok = true
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	//fmt.Printf("Master listen on prot:%v \n", sockname)
	if e != nil {
		log.Fatal("listen error: \n", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false
	for m.MasterStatus != Completed{
        // Stop the world
		m.Lock()
		for key, task := range m.InProcessTasks{

			if time.Now().Unix() - task.StartTime > 10{
				// 超时将其放入空闲链表
				task.WorkerState = Idle
				m.IdleTasks <- task
				delete(m.InProcessTasks,key)
			}
		}
		m.Unlock()

		time.Sleep(1000)
	}

	//fmt.Printf("All tasks finished, and result are %v \n", m.FinalFiles)
	ret = true
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {

	// Init Master
	m := Master{}
	// Your code here.
	m.Files = files
	m.M = 0
	m.R = 0
	m.NMap = int64(len(files))
	m.NReduce = int64(nReduce)
	for i := 0; i < nReduce; i++ {
		m.Intermediates = append(m.Intermediates, []string{})
	}
	m.MasterStatus = Idle
	m.InProcessTasks = make(map[int]*Task)
	m.IdleTasks = make(chan*Task,m.NReduce)

	go m.GenerateRMapTasks()

	m.server()
	return &m
}
