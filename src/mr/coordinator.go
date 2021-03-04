package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type TaskType int

const (
	MAP    TaskType = 1
	REDUCE TaskType = 2
)

type TaskState int

const (
	PREPARING TaskState = 1
	WAITING   TaskState = 2
	WORKING   TaskState = 3
	FAIL      TaskState = 4
	FINISH    TaskState = 5
)

type Task struct {
	Tid       int
	Key       string
	TaskType  TaskType
	TaskState TaskState
	StartTime time.Time
	NReduce   int
}

type Coordinator struct {
	// Your definitions here.
	taskMap     map[string]*Task
	mutex       sync.Mutex
	mapFinishCh chan int
	nReduce     int
	isDone      bool
}

func (c *Coordinator) SetTaskTimer(task *Task, duration time.Duration) {
	go func(task *Task, duration time.Duration) {
		time.Sleep(duration)
		c.mutex.Lock()
		if c.taskMap[task.Key].TaskState != FINISH {
			c.taskMap[task.Key].TaskState = WAITING
		}
		c.mutex.Unlock()
	}(task, duration)
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *EmptyArgs, reply *GetTaskReply) error {
	log.Println("Start send task")
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for _, task := range c.taskMap {
		if task.TaskState == WAITING {
			task.TaskState = WORKING
			reply.Task = *task
			log.Printf("send task %s", task.Key)
			c.SetTaskTimer(task, time.Second*10)
			return nil
		}
	}
	reply.Task = Task{}
	return nil
}

func (c *Coordinator) SetTaskState(args *SetTaskStateArgs, reply *BoolReply) error {
	key := args.Task.Key
	c.mutex.Lock()
	c.taskMap[key].TaskState = args.State
	c.mutex.Unlock()
	if args.State == FINISH {
		log.Printf("task %d finish", args.Task.Tid)
		c.mapFinishCh <- args.Task.Tid
	}
	reply.Result = true
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mutex.Lock()
	ret = c.isDone
	c.mutex.Unlock()

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{make(map[string]*Task), sync.Mutex{}, make(chan int, 1), nReduce, false}

	// Your code here.
	c.mutex.Lock()
	for i, file := range files {
		c.taskMap[file] = &Task{
			i,
			file,
			MAP,
			WAITING,
			time.Time{},
			nReduce,
		}
		log.Printf("%s", file)
	}
	c.mutex.Unlock()

	c.server()
	// wait map task finish
	go func(finishCh chan int) {
		for _ = range files {
			<-finishCh
		}
		log.Printf("map task all finish")
		// create reduce task
		c.mutex.Lock()
		for i := 0; i < nReduce; i++ {
			c.taskMap[strconv.Itoa(i)] = &Task{
				i,
				strconv.Itoa(i),
				REDUCE,
				WAITING,
				time.Time{},
				nReduce,
			}
		}
		c.mutex.Unlock()
		// 等待任务完成
		for i := 0; i < c.nReduce; i++ {
			<-finishCh
		}
		log.Printf("Job finish")
		c.mutex.Lock()
		c.isDone = true
		c.mutex.Unlock()
	}(c.mapFinishCh)
	return &c
}
