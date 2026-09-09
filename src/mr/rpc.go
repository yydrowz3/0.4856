package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

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

type TaskType int

const (
	TaskWait TaskType = iota
	TaskMap
	TaskReduce
	TaskExit
)

type RequestTaskArgs struct {
	WorkerID int
}

type RequestTaskReply struct {
	Type     TaskType
	TaskID   int
	Attempt  int
	Filename string
	NReduce  int
	NMap     int
}

type ReportTaskArgs struct {
	WorkerID int
	Type     TaskType
	TaskID   int
	Attempt  int
	Success  bool
}

type ReportTaskReply struct {
	Accepted bool
}
