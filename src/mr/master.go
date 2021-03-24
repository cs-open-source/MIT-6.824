package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
    "fmt"
)

type State int

// The state of the machine (Reduce and Map machine)
const (
	_          State = iota
	Idle             // 空闲状态
	InProgress       // 正在运行中
	Completed        // 完成
)

type Master struct {

	Interval map[int]int
	States   map[int]State
	sync.Mutex
	// the files of all map worker
	Files  []string
	M      int64
	R      int64
	Finish bool

	// The Intermediate k/v file's name
	Intermediates map[int][]string

}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	if args.X == -1 {
		m.Finish = true
		return nil
	}

	// 如果Map操作已经执行完毕
	k := atomic.AddInt64(&m.M, -1)
	if k >= 0 {
		reply.File = m.Files[k]
		reply.IsReduce = false
		reply.Y = args.X + 1
		reply.NReduce = int(m.R)
		reply.NMap = int(m.M) + 1
	} else {
		k := atomic.AddInt64(&m.R, -1)
		if k >= 0 {
			reply.IsReduce = true
			reply.Y = int(k)
		} else {
			reply.Y = -1
		}
	}
	return nil
}


func (m *Master) GetReduceTask(args *ReduceArgs, reply *ReduceReply) error{

	k := atomic.AddInt64(&m.M, -1)
	if k >= 0{
		reply.Files = []string{m.Files[k]}
		reply.Y = int(k + 1)
	}else { reply.Y = -1}
	return nil
}

func (m *Master) GetMapTask(args *MapArgs, reply *MapReply) error {
	k := atomic.AddInt64(&m.R, -1)
	if k >= 0 {
		for i:=0;i<len(m.Files);i++{
			reply.Files = append(reply.Files, fmt.Sprintf("reduce_%d_%d", i, k-1))
		}
		reply.Y = int(k)
	} else {
		reply.Y = -1
	}
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
    fmt.Printf("Master listen on prot:%v",sockname)
    if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
// todo handle timeout problem
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	// m.Lock()

	// m.Unlock()

	for !m.Finish {
	}
    fmt.Println("All tasks finished")
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
	m.M = int64(len(files))
	m.R = int64(nReduce)
	m.Finish = false
	m.server()
	return &m
}
