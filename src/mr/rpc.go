package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//


type TaskArgs struct {
	X int
}


type TaskReply struct {
	T *Task
	MasterState State
}


type TaskNotifyArgs struct {
	TaskId        int
	Y             int
	Files    []string
	IsReduce bool
}

type TaskNotifyReply struct {
	Ok bool
}


type RegisterWorkerArgs struct {
	Pid int
}

type RegisterWorkerReply struct {
	Ok bool
}


type DeleteWorkerArgs struct {
	Pid int
}

type DeleteWorkerReply struct {
	Ok bool
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
