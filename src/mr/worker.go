package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff) // 清除最高位, 保证int可以转换
}

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	workerID := os.Getpid()
	for {
		args := RequestTaskArgs{
			WorkerID: workerID,
		}
		reply := RequestTaskReply{}
		if !call("Coordinator.RequestTask", &args, &reply) {
			return
		}

		switch reply.Type {
		case TaskMap:
			// handle map task
			fmt.Printf("received map task: id=%d attempt=%d file=%s nReduce=%d nMap=%d\n", reply.TaskID, reply.Attempt, reply.Filename, reply.NReduce, reply.NMap)
			success := runMapTask(reply, mapf)
			if !reportTask(workerID, reply, success) {
				return
			}
		case TaskReduce:
			// handle reduce task
			fmt.Printf("received reduce task: id=%d attempt=%d nMap=%d\n", reply.TaskID, reply.Attempt, reply.NMap)
			success := runReduceTask(reply, reducef)
			if !reportTask(workerID, reply, success) {
				return
			}
		case TaskWait:
			time.Sleep(time.Second)
		case TaskExit:
			return
		}
	}
}

func reportTask(workerID int, task RequestTaskReply, success bool) bool {
	args := ReportTaskArgs{
		WorkerID: workerID,
		Type:     task.Type,
		TaskID:   task.TaskID,
		Attempt:  task.Attempt,
		Success:  success,
	}
	reply := ReportTaskReply{}

	if !call("Coordinator.ReportTask", &args, &reply) {
		return false
	}
	if !reply.Accepted {
		log.Printf("task report rejected: type=%d task=%d attempt=%d", task.Type, task.TaskID, task.Attempt)
	}

	return true
}

func runMapTask(task RequestTaskReply, mapf func(string, string) []KeyValue) bool {
	if task.NReduce <= 0 {
		log.Printf("map task %d: invalid nReduce %d", task.TaskID, task.NReduce)
		return false
	}

	// 1. Read the entire input file into memory.
	content, err := os.ReadFile(task.Filename)
	if err != nil {
		log.Printf("map task %d: read %s: %v", task.TaskID, task.Filename, err)
		return false
	}

	// 2. invoke the user-defined map function
	kva := mapf(task.Filename, string(content))

	// 3. partition the output into nReduce intermediate files
	tempFiles := make([]*os.File, task.NReduce)
	tempNames := make([]string, task.NReduce)
	encoders := make([]*json.Encoder, task.NReduce) // json encoder 是写入文件，所以一个文件句柄对应一个 encoder

	// remove temp files on exit (if fail on the way)
	defer func() {
		for _, file := range tempFiles {
			if file != nil {
				_ = file.Close()
			}
		}
		for _, name := range tempNames {
			if name != "" {
				_ = os.Remove(name)
			}
		}
	}()

	// create temp file
	for reduceID := 0; reduceID < task.NReduce; reduceID++ {
		pattern := fmt.Sprintf(".mr-%d-%d-attempt-%d-*", task.TaskID, reduceID, task.Attempt)
		file, err := os.CreateTemp(".", pattern)
		if err != nil {
			log.Printf("map task %d: create temp file: %v", task.TaskID, err)
			return false
		}
		tempFiles[reduceID] = file
		tempNames[reduceID] = file.Name()
		encoders[reduceID] = json.NewEncoder(file)
	}

	// 4. dump file content
	for _, kv := range kva {
		reduceID := ihash(kv.Key) % task.NReduce
		if err := encoders[reduceID].Encode(&kv); err != nil {
			log.Printf("map task %d: encode reduce partition %d: %v", task.TaskID, reduceID, err)
			return false
		}
	}

	// close temp file + rename
	for reduceID, file := range tempFiles {
		if err := file.Close(); err != nil {
			log.Printf("map task %d: close partition %d: %v", task.TaskID, reduceID, err)
			return false
		}
		tempFiles[reduceID] = nil
	}

	for reduceID, tempName := range tempNames {
		finalName := fmt.Sprintf("mr-%d-%d", task.TaskID, reduceID)
		if err := os.Rename(tempName, finalName); err != nil {
			log.Printf("map task %d: rename %s to %s: %v", task.TaskID, tempName, finalName, err)
			return false
		}
		tempNames[reduceID] = ""
	}

	return true
}

func runReduceTask(task RequestTaskReply, reducef func(string, []string) string) bool {
	intermediate := make([]KeyValue, 0)

	// 1. read intermediate file
	for mapID := 0; mapID < task.NMap; mapID++ {
		filename := fmt.Sprintf("mr-%d-%d", mapID, task.TaskID)
		file, err := os.Open(filename)
		if err != nil {
			log.Printf("reduce task %d: open %s: %v", task.TaskID, filename, err)
			return false
		}
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := decoder.Decode(&kv)
			if err == io.EOF {
				break
			}
			if err != nil {
				_ = file.Close()
				log.Printf("reduce task %d: decode %s: %v", task.TaskID, filename, err)
				return false
			}
			intermediate = append(intermediate, kv)
		}
		if err := file.Close(); err != nil {
			log.Printf("reduce task %d: close %s: %v", task.TaskID, filename, err)
			return false
		}
	}

	// 2. sort intermediate by key
	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	// 3. create temp output file
	tempFile, err := os.CreateTemp(".", fmt.Sprintf(".mr-out-%d-attempt-%d-*", task.TaskID, task.Attempt))
	if err != nil {
		log.Printf("reduce task %d: create temp output: %v", task.TaskID, err)
		return false
	}

	tempName := tempFile.Name()
	defer func() {
		if tempFile != nil {
			_ = tempFile.Close()
		}
		if tempName != "" {
			_ = os.Remove(tempName)
		}
	}()

	// 4. apply reduce function to intermedate with same key
	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		values := make([]string, 0, j-i)
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		if _, err := fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output); err != nil {
			log.Printf("reduce task %d: write output: %v", task.TaskID, err)
			return false
		}
		i = j
	}

	// 5. close tempFile + rename
	if err := tempFile.Close(); err != nil {
		log.Printf("reduce task %d: close output: %v", task.TaskID, err)
		return false
	}
	tempFile = nil

	finalName := fmt.Sprintf("mr-out-%d", task.TaskID)
	if err := os.Rename(tempName, finalName); err != nil {
		log.Printf("reduce task %d: rename %s to %s: %v", task.TaskID, tempName, finalName, err)
		return false
	}
	tempName = ""
	return true
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
