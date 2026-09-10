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

const taskTimeout = 10 * time.Second

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
	files       []string
	nReduce     int
	mapTasks    []taskMeta
	reduceTasks []taskMeta
	mu          sync.Mutex
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
	now := time.Now()

	// Map phase
	if !c.allMapsDoneLocked() {
		for taskID := range c.mapTasks {
			meta := &c.mapTasks[taskID]

			if !taskAssignable(meta, now) {
				continue
			}

			if meta.Status == StatusRunning {
				log.Printf("map task timeout: task=%d attempt=%d", taskID, meta.Attempt)
			}

			meta.Status = StatusRunning
			meta.StartedAt = now
			meta.Attempt++

			reply.Type = TaskMap
			reply.TaskID = taskID
			reply.Attempt = meta.Attempt
			reply.Filename = c.files[taskID]
			reply.NReduce = c.nReduce
			reply.NMap = len(c.files)

			// log.Printf("assign map task=%d attemp=%d worker=%d file=%s", taskID, meta.Attempt, args.WorkerID, c.files[taskID])
			return nil
		}

		// not job all done, but doing normally
		return nil
	}

	// Reduce phase
	for taskID := range c.reduceTasks {
		meta := &c.reduceTasks[taskID]
		if !taskAssignable(meta, now) {
			continue
		}
		if meta.Status == StatusRunning {
			log.Printf("reduce task timeout: task=%d attempt=%d", taskID, meta.Attempt)
		}

		meta.Status = StatusRunning
		meta.StartedAt = now
		meta.Attempt++

		reply.Type = TaskReduce
		reply.TaskID = taskID
		reply.Attempt = meta.Attempt
		reply.NMap = len(c.files)
		reply.NReduce = c.nReduce

		// log.Printf("assign reduce task=%d attemp=%d worker=%d", taskID, meta.Attempt, args.WorkerID)
		return nil
	}

	if c.allReducesDoneLocked() {
		reply.Type = TaskExit
	}

	return nil
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	reply.Accepted = false

	var meta *taskMeta
	var taskName string
	switch args.Type {
	case TaskMap:
		if args.TaskID < 0 || args.TaskID >= len(c.mapTasks) {
			return nil
		}
		meta = &c.mapTasks[args.TaskID]
		taskName = "map"
	case TaskReduce:
		// Reduce happens only after all map tasks are done
		if !c.allMapsDoneLocked() {
			return nil
		}
		if args.TaskID < 0 || args.TaskID >= len(c.reduceTasks) {
			return nil
		}
		meta = &c.reduceTasks[args.TaskID]
		taskName = "reduce"
	default:
		return nil
	}

	// refuce duplicated report and outdated report
	if meta.Status != StatusRunning {
		return nil
	}
	if meta.Attempt != args.Attempt {
		return nil
	}

	reply.Accepted = true
	meta.StartedAt = time.Time{}

	if args.Success {
		meta.Status = StatusDone
		log.Printf("%s task done: task=%d attempt=%d worker=%d", taskName, args.TaskID, args.Attempt, args.WorkerID)
	} else {
		meta.Status = StatusIdle
		log.Printf("%s task failed: task=%d attempt=%d worker=%d", taskName, args.TaskID, args.Attempt, args.WorkerID)
	}

	return nil
}

func (c *Coordinator) allMapsDoneLocked() bool {
	for i := range c.mapTasks {
		if c.mapTasks[i].Status != StatusDone {
			return false
		}
	}

	return true
}

func (c *Coordinator) allReducesDoneLocked() bool {
	for i := range c.reduceTasks {
		if c.reduceTasks[i].Status != StatusDone {
			return false
		}
	}

	return true
}

func taskAssignable(meta *taskMeta, now time.Time) bool {
	if meta.Status == StatusIdle {
		return true
	}

	return meta.Status == StatusRunning && now.Sub(meta.StartedAt) >= taskTimeout
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
	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.allMapsDoneLocked() && c.allReducesDoneLocked()

}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:       files,
		nReduce:     nReduce,
		mapTasks:    make([]taskMeta, len(files)),
		reduceTasks: make([]taskMeta, nReduce),
	}

	// Your code here.

	c.server(sockname)
	return &c
}
