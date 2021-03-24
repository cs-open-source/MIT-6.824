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

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	// The file to be processed
	File     string
	IsReduce bool
	Y        int
	NReduce  int
	NMap     int
}


type ReduceArgs struct{}
type ReduceReply struct{
	// The file to be processed
	Files []string
	// Next Task Id
	Y int
}


type MapArgs struct{
}

type MapReply struct{
	Y int
	Files []string
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
