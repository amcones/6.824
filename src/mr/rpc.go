package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"time"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type TaskState int

const (
	Idle = iota
	InProcess
	Completed
)

type TaskOperation int

const (
	ToWait = iota
	Mapper
	Reducer
	ToExit
)

type TaskMeta struct {
	StartTime time.Time
	TaskState
	TaskID  int
	NReduce int
}
type Task struct {
	TaskMeta
	TaskOperation
	Filename              string
	IntermediateFilenames []string
	OutputFilename        string
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
