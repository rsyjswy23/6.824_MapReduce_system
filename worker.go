package mr

import (
	"fmt"
	"log"
	"net/rpc"
	"hash/fnv"
	"os"
	"io/ioutil"
	"time"
	"sort"
	"strings"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	// New32 returns a new 32-bit FNV-1 hash.Hash. Its Sum method will lay the value 
	// out in big-endian byte order.
	h := fnv.New32a()         
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
// in this function:
//  (1) create rpc args & reply and periodically make rpc call (RequestJob) to coordinator
//	(2) if reply.Type is MAP, then doMapTask(), 
//  (3) elif reply.Type is REDUCE, then doReduceTask()
//  (4) update lastTaskId & lastTaskType for coordinator to commit write later
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// use PID as WorkerID on a single machine, easy to debug
	id := os.Getpid()
	log.Printf("Worker %d is ready：\n", id)
	lastTaskID := -1
	lastTaskType := ""
	// (1)
	for {
		args := RequestTaskArgs{
			WorkerID:     id,
			LastTaskID:   lastTaskID,
			LastTaskType: lastTaskType,
		}
		reply := RequestTaskReply{}
		call("Coordinator.RequestTask", &args, &reply)
		// (2)-(3)
		switch reply.TaskType {
		case "":
			log.Printf("All Tasks Done")
			goto End
		case MAP:
			doMapTask(id, reply.TaskID, reply.MapInputFile, reply.NReduce, mapf)
		case REDUCE:
			doReduceTask(id, reply.TaskID, reply.NMap, reducef)
		}
		// (4)
		lastTaskID = reply.TaskID 
		lastTaskType = reply.TaskType
		log.Printf("completed type %s task %d", reply.TaskType, reply.TaskID)
	}
End: 
	log.Printf("Worker %d completed task \n", id)
}

//
// doMapTask():
// (1) open and read input file
// (2) call mapf to create kva: [{a 1} {coffee 1} {happy 1} ...]
// (3) hash(key) and partition intermediate kv pairs into nRecuce buckets
// (4) write tmpMapOutFile, later will allow coordinator to write and commit finalMapOutFile
//
func doMapTask(id int, taskID int, fileName string, nReduce int, mapf func(string, string) []KeyValue) {
	// (1)
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("%s fail to open file！", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("%s fail to read content", fileName)
	}
	file.Close()
	// (2)
	kva := mapf(fileName, string(content))
	hashedKva := make(map[int][]KeyValue)
	// (3)
	for _, kv := range kva {
		hashed := ihash(kv.Key) % nReduce
		hashedKva[hashed] = append(hashedKva[hashed], kv)
	}
	// (4)
	for i := 0; i < nReduce; i++ {
		outFile, _ := os.Create(tmpMapOutFile(id, taskID, i))
		for _, kv := range hashedKva[i] {
			 fmt.Fprintf(outFile, "%v\t%v\n", kv.Key, kv.Value)
		}
		outFile.Close()
	}

}


//
// doReduceTask():
// (1) open and read intermediate files
// (2) sort kva
// (3) call reducef and write output to tmpReduceOutFile
//
func doReduceTask(id int, taskID int, nMap int, reducef func(string, []string) string) {
	var lines []string
	for i := 0; i < nMap; i++ {
		file, err := os.Open(finalMapOutFile(i, taskID))
		if err != nil {
			log.Fatalf("%s fail to open file！", finalMapOutFile(i, taskID))
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("%s fail to read content！", finalMapOutFile(i, taskID))
		}
		lines = append(lines, strings.Split(string(content), "\n")...)
	}
	var kva []KeyValue
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		split := strings.Split(line, "\t")
		kva = append(kva, KeyValue{
			Key:   split[0],
			Value: split[1],
		})
	}
	sort.Sort(ByKey(kva))
	outFile, _ := os.Create(tmpReduceOutFile(id, taskID))
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		fmt.Fprintf(outFile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	outFile.Close()
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	// DialHTTP connects to an HTTP RPC server at the specified network address listening on the default HTTP RPC path.
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	time.Sleep(time.Second * 1)
	defer c.Close()
	// Synchronous call: Perform a procedure call (core.HandlerName == Handler.Execute)
	// with the Request as specified and a pointer to a response
	// to have our response back.
	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}