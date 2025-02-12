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

type TaskStatus int

const (
	Pending TaskStatus = iota
	InProgress
	Completed
)

type Coordinator struct {
	mu              sync.Mutex
	nMap            int
	nReduce         int
	mapTasks        []Task
	reduceTasks     []Task
	completedMap    int
	completedReduce int
}

type Task struct {
	TaskType   string // "map", "reduce", "exit"
	FileName   string
	TaskNumber int // 任务编号
	status     int // -1未完成 0正在执行 1已完成
}

func startTimer(c *Coordinator, taskType string, taskNumber int) {
	timer := time.NewTimer(10 * time.Second)

	go func() {
		<-timer.C

		if taskType == "map" && c.mapTasks[taskNumber].status == int(InProgress) {
			c.mapTasks[taskNumber].status = int(Pending)
		}
		if taskType == "reduce" && c.reduceTasks[taskNumber].status == int(InProgress) {
			c.reduceTasks[taskNumber].status = int(Pending)
		}
	}()
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) AssignTask(args *CommitAndQueryTask, reply *ReplyTask) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if idx, _ := strconv.Atoi(args.TaskNumber); args.TaskType == "map" && c.mapTasks[idx].status != int(Completed) {
		c.mapTasks[idx].status = int(Completed)
		c.completedMap++
		// fmt.Printf("第%d个map任务已完成\n", idx)
	}
	// w1完成了任务6向c请求，c给了7.然后w2也进来了，请求任务，c也给了7.这里出现并发问题
	if c.completedMap < c.nMap {
		TaskNumToBeAssign := -1
		for k, v := range c.mapTasks {
			if v.status == int(Pending) {
				TaskNumToBeAssign = k
				c.mapTasks[k].status = int(InProgress)
				startTimer(c, "map", k)
				break
			}
		}
		if TaskNumToBeAssign == -1 {
			reply.TaskType = "wait"
			return nil
		}
		// fmt.Printf("分配第%d个map任务\n", TaskNumToBeAssign)
		reply.TaskType = "map"
		reply.TaskNumber = strconv.Itoa(TaskNumToBeAssign)
		reply.FileName = c.mapTasks[TaskNumToBeAssign].FileName
		reply.NMap = c.nMap
		reply.NReduce = c.nReduce
		return nil
	}

	if idx, _ := strconv.Atoi(args.TaskNumber); args.TaskType == "reduce" && c.reduceTasks[idx].status != int(Completed) {
		c.reduceTasks[idx].status = int(Completed)
		c.completedReduce++
		// fmt.Printf("第%d个reduce任务已完成\n", idx)
	}

	if c.completedReduce < c.nReduce {
		TaskNumToBeAssign := -1
		for k, v := range c.reduceTasks {
			if v.status == int(Pending) {
				TaskNumToBeAssign = k
				c.reduceTasks[k].status = int(InProgress)
				startTimer(c, "reduce", k)
				break
			}
		}
		if TaskNumToBeAssign == -1 {
			reply.TaskType = "wait"
			return nil
		}
		// fmt.Printf("分配第%d个reduce任务\n", TaskNumToBeAssign)
		reply.TaskType = "reduce"
		reply.TaskNumber = strconv.Itoa(TaskNumToBeAssign)
		reply.NMap = c.nMap
		reply.NReduce = c.nReduce
		return nil
	}

	reply.TaskType = "exit"
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	if c.completedMap == c.nMap && c.completedReduce == c.nReduce {
		return true
	}
	return false
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	c.nMap = len(files)
	c.nReduce = nReduce
	c.mapTasks = make([]Task, c.nMap)
	c.reduceTasks = make([]Task, c.nReduce)
	for i := range c.mapTasks {
		c.mapTasks[i] = Task{"map", files[i], i, int(Pending)}
	}
	for i := range c.reduceTasks {
		c.reduceTasks[i] = Task{"reduce", "", i, int(Pending)}
	}
	c.completedMap = 0
	c.completedReduce = 0
	c.server()
	return &c
}
