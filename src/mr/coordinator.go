package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type JobParse int

const (
	Mapping = iota
	Reducing
	Finishing
)

type Coordinator struct {
	// Your definitions here.
	mu                    sync.Mutex
	NReduce               int
	TaskQueue             chan *Task
	TaskTemp              map[int]*Task
	MaxTaskID             int
	IntermediateFilenames [][]string
	JobParse
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
func (c *Coordinator) RequestTask(args *ExampleArgs, reply *Task) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.JobParse == Finishing {
		*reply = Task{TaskOperation: ToExit}
	} else if len(c.TaskQueue) > 0 {
		*reply = *<-c.TaskQueue
		reply.StartTime = time.Now()
		reply.TaskState = InProcess
		c.TaskTemp[reply.TaskID] = reply
	} else {
		*reply = Task{TaskOperation: ToWait}
	}
	return nil
}
func (c *Coordinator) CompleteTask(task *Task, reply *ExampleReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	//log.Print("[coordinator]Completing task ", task.TaskID)
	if c.TaskTemp[task.TaskID].TaskState == Completed ||
		task.TaskOperation == Mapper && c.JobParse != Mapping ||
		task.TaskOperation == Reducer && c.JobParse != Reducing {
		//log.Printf("[coordinator]Task %d is completed, throw", task.TaskID)
		return nil
	}
	//time.Sleep(2 * time.Second)
	go c.ProcessTaskResult(task)
	return nil
}

//
// start a thread that listens for RPCs from worker.go
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
func (c *Coordinator) ProcessTaskResult(task *Task) {
	c.mu.Lock()
	defer c.mu.Unlock()
	//log.Printf("[coordinator]processing task %d result", task.TaskID)
	c.TaskTemp[task.TaskID].TaskState = Completed
	switch task.TaskOperation {
	case Mapper:
		{
			//log.Print("[coordinator]Process map task")
			for idx, file := range task.IntermediateFilenames {
				//log.Print("[coordinator]map index:", idx, ":", file)
				c.IntermediateFilenames[idx] = append(c.IntermediateFilenames[idx], file)
			}
			//log.Print("(mapper)check all tasks done")
			if c.AllTaskDone() {
				//log.Print("[coordinator]changing to reduce parse")
				c.ProduceReduceTasks()
				c.JobParse = Reducing
			}
		}
	case Reducer:
		{
			//log.Print("Process reduce task")
			//log.Print("(reducer)check all tasks done")
			if c.AllTaskDone() {
				//log.Print("[coordinator]changing to finish parse")
				c.JobParse = Finishing
			}
		}
	}
}
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Your code here.
	ret := c.JobParse == Finishing
	return ret
}
func (c *Coordinator) AllTaskDone() bool {
	//log.Print("checking all task done")
	for _, task := range c.TaskTemp {
		if task.TaskState != Completed {
			return false
		}
	}
	//log.Print("[coordinator]Current all tasks done")
	return len(c.TaskQueue) == 0
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
const TIMEOUT = 4 * time.Second

func (c *Coordinator) CatchTimeOut() {
	//log.Print("Catching time out tasks")
	for {
		time.Sleep(2 * time.Second)
		c.mu.Lock()
		for _, task := range c.TaskTemp {
			if task.TaskState == InProcess && time.Now().Sub(task.StartTime) >= TIMEOUT {
				//log.Printf("[coordinator]task %d is timeout, reloading", task.TaskID)
				task.TaskState = Idle
				c.TaskQueue <- task
			}
		}
		c.mu.Unlock()
	}

}
func (c *Coordinator) ProduceMapTasks(files []string) {
	//log.Print("[coordinator]Producing map tasks")
	for _, file := range files {
		newMapTask := Task{
			TaskMeta: TaskMeta{
				TaskState: Idle,
				TaskID:    c.MaxTaskID,
				NReduce:   c.NReduce,
			},
			TaskOperation: Mapper,
			Filename:      file,
		}
		c.MaxTaskID++
		c.TaskQueue <- &newMapTask
		//log.Printf("[coordinator]Producted new map task %d", newMapTask.TaskID)
	}
}
func (c *Coordinator) ProduceReduceTasks() {
	//log.Print("[coordinator]producing reduce tasks")
	c.TaskTemp = make(map[int]*Task)
	for _, files := range c.IntermediateFilenames {
		newReduceTask := Task{
			TaskMeta: TaskMeta{
				TaskState: Idle,
				TaskID:    c.MaxTaskID,
				NReduce:   c.NReduce,
			},
			TaskOperation:         Reducer,
			IntermediateFilenames: files,
		}
		c.MaxTaskID++
		c.TaskQueue <- &newReduceTask
		//log.Printf("[coordinator]Producted new reduce task %d", newReduceTask.TaskID)
	}
}
func Max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		NReduce:               nReduce,
		TaskQueue:             make(chan *Task, Max(len(files), nReduce)),
		TaskTemp:              make(map[int]*Task),
		MaxTaskID:             1,
		JobParse:              Mapping,
		IntermediateFilenames: make([][]string, nReduce),
	}

	// Your code here.
	c.ProduceMapTasks(files)
	c.server()
	go c.CatchTimeOut()
	return &c
}
