package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type State int

// The state of the machine (Reduce and Map machine)
const (
	_          State = iota
	Idle             // 空闲
	InProgress       // 正在运行中
	Completed        // 完成
)

type Master struct {
	sync.Mutex

	MapTaskIntervals map[int]int64
	// Map Task and it's file
	MapTasks map[int]string

	ReduceTasks map[int][]string
	//
	ReduceTaskIntervals map[int]int64
	//
	Intermediates [][]string

	FinalFiles []string
	// the files of all map worker
	Files []string
	// The current number of Map Workers
	M int64
	// The current number of Reduce Workers
	R int64
	// The current number of Map Workers
	NMap int64
	// The current number of Reduce Workers
	NReduce int64

	MFinish int64

	RFinish int64
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) GetReduceTask(args *ReduceTaskArgs, reply *ReduceTaskReply) error {
	k := atomic.AddInt64(&m.R, 1)
	if k <= m.NReduce {
		m.Lock()
		log.Printf(" GetReduceTask %v", k)
		reply.Files = m.Intermediates[0]
		m.Intermediates = m.Intermediates[1:]
		m.ReduceTasks[int(k)] = reply.Files
		m.ReduceTaskIntervals[int(k)] = time.Now().Unix()
		reply.Y = int(k)
		reply.Finished = false
		m.Unlock()
	} else {
		atomic.AddInt64(&m.R, -1)
		reply.Finished = atomic.CompareAndSwapInt64(&m.RFinish, m.NReduce, m.NReduce)
	}
	//log.Printf("ReduceTaskReply : %v", reply)
	return nil
}

func (m *Master) GetMapTask(args *MapTaskArgs, reply *MapTaskReply) error {
	k := atomic.AddInt64(&m.M, 1)
	if k <= m.NMap {
		m.Lock()
		log.Printf(" GetMapTask %v", k)
		reply.File = m.Files[0]
		m.Files = m.Files[1:]
		m.MapTasks[int(k)] = reply.File
		m.MapTaskIntervals[int(k)] = time.Now().Unix()
		reply.Y = int(k)
		reply.NReduce = int(m.NReduce)
		reply.Finished = false
		m.Unlock()
	} else {
		atomic.AddInt64(&m.M,-1)
		reply.Finished = atomic.CompareAndSwapInt64(&m.MFinish, m.NMap, m.NMap)
	}
	return nil
}

func (m *Master) TaskNotify(args *TaskNotifyArgs, reply *TaskNotifyReply) error {

	log.Printf("%v, %v task has finised", args.X, args.IsReduce)
	if args.IsReduce {
		m.Lock()
		if val, ok := m.ReduceTaskIntervals[args.X]; ok {
			log.Printf("%v Reduce worker %v Finished ", args.X, val)
			m.RFinish += 1
			delete(m.ReduceTaskIntervals, args.X)
			m.FinalFiles = append(m.FinalFiles, args.Files...)
		}
		m.Unlock()
	} else {
		m.Lock()
		if val, ok := m.MapTaskIntervals[args.X]; ok {
			log.Printf("%v Map worker %v Finished ", args.X, val)
			m.MFinish += 1
			delete(m.MapTaskIntervals, args.X)
			for i := 0; i < int(m.NReduce); i++ {
				m.Intermediates[i] = append(m.Intermediates[i], args.Files[i])
			}
		}
		m.Unlock()
	}
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
	fmt.Printf("Master listen on prot:%v", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false
	for !atomic.CompareAndSwapInt64(&m.RFinish, m.NReduce, m.NReduce) {
		m.Lock()
		hasTimeout := false
		var timeout []int
		if m.MFinish != m.NMap {
			for k, v := range m.MapTaskIntervals {
				if time.Now().Unix()-v > 10 {
					log.Printf(" %v, Map worker timeout, try to allocate another machine to run  it", k)
					// restore the files
					m.M = m.M - 1
					m.Files = append(m.Files, m.MapTasks[k])
					delete(m.MapTasks, k)
					timeout = append(timeout, k)
				}
			}
			for _, k := range timeout {
				hasTimeout = true
				delete(m.MapTaskIntervals, k)
			}
		} else {
			for k, v := range m.ReduceTaskIntervals {
				if time.Now().Unix()-v > 10 {
					log.Printf(" %v, Reduce worker timeout, try to allocate another machine to run  it", k)

					// restore the files
					m.R = m.R - 1
					m.Intermediates = append(m.Intermediates, m.ReduceTasks[k])
					delete(m.ReduceTasks, k)
					timeout = append(timeout, k)
				}
			}
			for _, k := range timeout {
				hasTimeout = true
				delete(m.ReduceTaskIntervals, k)
			}
		}

		if hasTimeout{
			log.Printf(" m.Reduce %v. m.Map %v, m.Files %v, m.Intermediate %v",m.R,m.M,len(m.Files),len(m.Intermediates))
		}

		m.Unlock()

		time.Sleep(1000)
	}
	// Your code here.

	fmt.Printf("All tasks finished, and result are %v", m.FinalFiles)
	ret = true
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.Files = files
	m.M = 0
	m.R = 0
	m.NMap = int64(len(files))
	m.NReduce = int64(nReduce)
	m.MFinish = 0
	m.RFinish = 0
	m.ReduceTaskIntervals = make(map[int]int64)
	m.ReduceTasks = make(map[int][]string)
	m.MapTaskIntervals = make(map[int]int64)
	m.MapTasks = make(map[int]string)

	for i := 0; i < nReduce; i++ {
		m.Intermediates = append(m.Intermediates, []string{})
	}

	log.Printf(" Master is %v", m)

	m.server()
	return &m
}
