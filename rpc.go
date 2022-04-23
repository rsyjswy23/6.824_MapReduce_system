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

const (
	MAP    = "MAP"
	REDUCE = "REDUCE"
	DONE   = "DONE"
)

//
// declare the arguments and reply for an RPC.
//

type RequestTaskArgs struct {
	WorkerID     int
	LastTaskID   int
	LastTaskType string
}

type RequestTaskReply struct {
	TaskID      int
	TaskType     string
	MapInputFile string
	NReduce      int
	NMap         int
}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

