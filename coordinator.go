package mr

import (
	"log"
	"net"
	"os"
	"net/rpc"
	"net/http"
	"sync"
	"math"
	"time"
	"strconv"
	"fmt"
)

type Coordinator struct {
	mu			sync.Mutex		// synchronize access to tasksMap and update it concurrently from multiple go routines
	stage		string			// 3 stages: MAP --> REDUCE --> ALLDONE
	nMap		int 			// numbers of MAP tasks
	nReduce 	int				// numbers of REDUCE tasks
	tasks		map[string]Task	// map that store each task's status
	toDoTasks	chan Task		// use channel to pass toDoTasks to worker, use lock to prevent multiple workers get the same task
}

type Task struct {
	ID           int
	Type         string
	MapInputFile string
	WorkerID     int
	Deadline     time.Time
}
//
// RPC handlers for the worker to call
// the RPC argument and reply types are defined in rpc.go.
// in this function:
// (1) check worker's LastTask:
// 		if completed by deadline, then commit:
// 			for MapTask: rename tmp intermediate file "mr-X-Y", where X is TaskID, Y is reduceID (hash(word) % nReduce)
// 			for ReduceTask: write and save files "mr-out-Y", where Y is reduceID
// 		else: pass to ToDoTask channel and wait for re-assignment to other worker
// (2) get an un-assigned task from ToDoTask channel
// (3) update reply struct and send back to worker
//
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
		// (1)
		if args.LastTaskID != -1 {
			c.mu.Lock()
			taskID := args.LastTaskType + "-" + strconv.Itoa(args.LastTaskID)
			// only allow coordinator to commit file when this file is completed by deadline
			if task, ok := c.tasks[taskID]; ok && task.WorkerID == args.WorkerID { 
				log.Printf("%d completed %s-%d task", args.WorkerID, args.LastTaskType, args.LastTaskID)
				// commit method for MapTask 
				if args.LastTaskType == MAP {
					for i := 0; i < c.nReduce; i++ {
						err := os.Rename(
							tmpMapOutFile(args.WorkerID, args.LastTaskID, i),
							finalMapOutFile(args.LastTaskID, i))
						if err != nil {
							log.Fatalf(
								"Failed to mark map output file `%s` as final: %e",
								tmpMapOutFile(args.WorkerID, args.LastTaskID, i), err)
						}
					}
				// commit method for ReduceTask
				} else if args.LastTaskType == REDUCE {
					err := os.Rename(
						tmpReduceOutFile(args.WorkerID, args.LastTaskID),
						finalReduceOutFile(args.LastTaskID))
					if err != nil {
						log.Fatalf(
							"Failed to mark reduce output file `%s` as final: %e",
							tmpReduceOutFile(args.WorkerID, args.LastTaskID), err)
					}
				}
				delete(c.tasks, taskID)
				if len(c.tasks) == 0 {
					c.cutover()
				}
			}
			c.mu.Unlock()
		}
	
		// (2)
		task, ok := <-c.toDoTasks
		// if channel is empty, means all jobs are done, EXIT.
		if !ok {
			return nil
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		log.Printf("Assign %s task %d to worker %dls"+
			"\n", task.Type, task.ID, args.WorkerID)
		// (3)
		// update task parameters
		task.WorkerID = args.WorkerID
		task.Deadline = time.Now().Add(10 * time.Second)
		taskName := task.Type + "-" + strconv.Itoa(task.ID)
		c.tasks[taskName] = task
		// update reply and send back to worker as response
		reply.TaskID = task.ID
		reply.TaskType = task.Type
		reply.MapInputFile = task.MapInputFile
		reply.NMap = c.nMap
		reply.NReduce = c.nReduce
		return nil
}


// save tmp files & commit final files of both Mapfunc & Reducefunc
func tmpMapOutFile(workerID int, mapID int, reduceID int) string {
	return fmt.Sprintf("tmp-worker-%d-%d-%d", workerID, mapID, reduceID)
 }
 
 func finalMapOutFile(mapID int, reduceID int) string {
	return fmt.Sprintf("mr-%d-%d", mapID, reduceID)
 }
 
 func tmpReduceOutFile(workerID int, reduceID int) string {
	return fmt.Sprintf("tmp-worker-%d-out-%d", workerID, reduceID)
 }
 
 func finalReduceOutFile(reduceID int) string {
	return fmt.Sprintf("mr-out-%d", reduceID)
 }

func (c *Coordinator) cutover() {
	if c.stage == MAP {
		log.Printf("All MapTasks are done now. Let's start with ReduceTasks!")
		c.stage = REDUCE
		for i := 0; i < c.nReduce; i++ {
			task := Task{ID: i, Type: REDUCE, WorkerID: -1}
			taskName := task.Type + "-" + strconv.Itoa(task.ID)
			c.tasks[taskName] = task
			c.toDoTasks <- task
		}
	} else if c.stage == REDUCE {
		log.Printf("All ReduceTask are done!")
		close(c.toDoTasks)
		c.stage = DONE
	}
}
//
// start a thread that listens for RPCs from worker
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	ret := c.stage == DONE
	defer c.mu.Unlock()
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// create instance of coordinator
	c := Coordinator{
		stage:		MAP,
		nMap:		len(files),
		nReduce:	nReduce,
		tasks:		make(map[string]Task),
		toDoTasks:	make(chan Task, int(math.Max(float64(len(files)), float64(nReduce)))),
	}
	// fmt.Println("here at coordinator")
	// pre-process each pg-*.txt file to a task object, update on map[task] and pass each task to channel
	for i, file := range files {
		task := Task {
			ID:		      i,
			Type: 	      MAP,
			WorkerID:	  -1,
			MapInputFile: file,
		}
		
		taskName := task.Type + "-" + strconv.Itoa(task.ID)
		log.Printf("Type: %s, ID: %v, taskName: %s", task.Type, task.ID, taskName)
		c.tasks[taskName] = task
	//	fmt.Println(c.tasks)
		c.toDoTasks <- task
	//  fmt.Println(c.toDoTasks)
	}
	log.Printf("Coordinator starts\n")
	c.server()

	// go routine: periodically check if each task completed before deadline(10s), if not, unmark task status and pass it to todo channel and wait for worker again
	go func() {
		log.Printf("Coordinator listens to rpc from worker \n")
		for {
			time.Sleep(500 * time.Millisecond)
			c.mu.Lock()
			for _, task := range c.tasks {
				if task.WorkerID != -1 && time.Now().After(task.Deadline) {
					log.Printf("%d worker fails at %s %d, unmark task status & bring it back to channel", task.WorkerID, task.Type, task.ID)
					task.WorkerID = -1
					c.toDoTasks <- task
				}
			}
			c.mu.Unlock()
		}
	} ()
	return &c
}