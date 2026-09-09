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

type TaskStatus int

const (
	StatusIdle TaskStatus = iota
	StatusRunning
	StatusDone
)

type taskMeta struct {
	Status    TaskStatus
	StartedAt time.Time
	Attempt   int
}

type Coordinator struct {
	// Your definitions here.
	files    []string
	nReduce  int
	mapTasks []taskMeta
	mu       sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	reply.Type = TaskWait
	for taskID := range c.mapTasks {
		meta := &c.mapTasks[taskID]
		if meta.Status != StatusIdle {
			continue
		}
		meta.Status = StatusRunning
		meta.StartedAt = time.Now()
		meta.Attempt++

		reply.Type = TaskMap
		reply.TaskID = taskID
		reply.Attempt = meta.Attempt
		reply.Filename = c.files[taskID]
		reply.NReduce = c.nReduce
		reply.NMap = len(c.files)

		log.Printf("assign map task=%d attemp=%d worker=%d file=%s", taskID, meta.Attempt, args.WorkerID, c.files[taskID])
		return nil
	}

	return nil
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c) // 内部导出的方法可以被远程调用
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// for test
	// ret := true
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:    files,
		nReduce:  nReduce,
		mapTasks: make([]taskMeta, len(files)),
	}

	// Your code here.

	c.server(sockname)
	return &c
}
