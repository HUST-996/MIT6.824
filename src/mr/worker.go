package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	lastTask := CommitAndQueryTask{}
	// uncomment to send the Example RPC to the coordinator.
	for {
		arg := lastTask
		reply := ReplyTask{}
		// fmt.Printf("上一个任务号是%s,进行下一个任务的请求\n", lastTask.TaskNumber)
		ok := call("Coordinator.AssignTask", &arg, &reply)
		if !ok {
			time.Sleep(time.Second)
			continue
		}
		switch reply.TaskType {
		case "map":
			processMapTask(reply, mapf)
			// fmt.Printf("map任务%s已完成\n", reply.TaskNumber)
		case "reduce":
			processReduceTask(reply, reducef)
		case "wait":
			continue
		case "exit":
			return
		}
		lastTask.TaskNumber = reply.TaskNumber
		lastTask.TaskType = reply.TaskType
	}
}

func processMapTask(reply ReplyTask, mapf func(string, string) []KeyValue) {
	content, err := os.ReadFile(reply.FileName)
	if err != nil {
		log.Fatalf("cannot read %v", reply.FileName)
	}
	kva := mapf(reply.FileName, string(content))
	partitions := make([][]KeyValue, reply.NReduce)
	// 将kv对分到不同的分区
	for _, kv := range kva {
		index := ihash(kv.Key) % reply.NReduce
		partitions[index] = append(partitions[index], kv)
	}

	for i := 0; i < reply.NReduce; i++ {
		oname := fmt.Sprintf("mr-%s-%d", reply.TaskNumber, i)
		file, err := os.Create(oname)
		if err != nil {
			log.Fatalf("无法创建文件 %v: %v", oname, err)
		}
		// 将文件进行json编码
		enc := json.NewEncoder(file)
		for _, kv := range partitions[i] {
			if err := enc.Encode(&kv); err != nil {
				log.Fatalf("写入文件 %v 失败: %v", oname, err)
			}
		}
		file.Close()
	}
}

func processReduceTask(reply ReplyTask, reducef func(string, []string) string) {
	var kva []KeyValue
	for i := 0; i < reply.NMap; i++ {
		filename := fmt.Sprintf("mr-%d-%s", i, reply.TaskNumber)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("无法打开文件 %v: %v", filename, err)
			continue
		}
		// 解码json文件中的kv对
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	intermediate := make(map[string][]string)
	for _, kv := range kva {
		intermediate[kv.Key] = append(intermediate[kv.Key], kv.Value)
	}
	outputFilename := fmt.Sprintf("mr-out-%s", reply.TaskNumber)
	outputFile, err := os.Create(outputFilename)
	if err != nil {
		log.Fatalf("无法创建输出文件%v:%v", outputFilename, err)
	}
	defer outputFile.Close()

	for key, vals := range intermediate {
		result := reducef(key, vals)
		fmt.Fprintf(outputFile, "%v %v\n", key, result)
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
