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
type ReduceTaskArgs struct {
	X int
}

type ReduceTaskReply struct {
	Y        int
	Files    []string
	Finished bool
}

type MapTaskArgs struct {
	X int
}

type MapTaskReply struct {
	Y        int
	File     string
	Finished bool
	NReduce  int
}

type TaskNotifyArgs struct {
	X        int
	Files    []string
	IsReduce bool
}

type TaskNotifyReply struct {
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
