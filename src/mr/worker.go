package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

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
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func ProcessMapTask(task Task, mapf func(string, string) []KeyValue) {
	filename := task.Key
	log.Print(filename)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	results := make([][]KeyValue, task.NReduce)
	for _, kv := range kva {
		reduceTaskNum := ihash(kv.Key) % task.NReduce
		results[reduceTaskNum] = append(results[reduceTaskNum], kv)
	}
	for i, result := range results {
		suffix := strconv.Itoa(task.Tid) + "-" + strconv.Itoa(i)
		log.Printf(suffix)
		os.MkdirAll("./tmp/", os.ModePerm)
		file, err := ioutil.TempFile("./tmp/", "mr-worker-"+suffix)
		if err != nil {
			log.Fatalf("create tmp file fail")
		}
		enc := json.NewEncoder(file)
		for _, kv := range result {
			enc.Encode(&kv)
		}
		file.Close()
		os.Rename(file.Name(), "./mr-"+suffix)
	}
	MarkDone(task)
}

func ProcessReduceTask(task Task, reducef func(string, []string) string) {
	log.Printf("process reduce task %v", task.Key)
	reduceTaskID := task.Key
	f, err := os.Open(".")
	if err != nil {
		log.Panicf("list file fail: %v", err)
	}
	list, err := f.Readdir(-1)
	f.Close()
	taskFileName := []string{}

	for _, item := range list {
		if strings.HasSuffix(item.Name(), reduceTaskID) {
			taskFileName = append(taskFileName, item.Name())
		}
	}

	kva := []KeyValue{}
	for _, fileName := range taskFileName {
		file, err := os.Open(fileName)
		if err != nil {
			log.Panicf("open file %s error: %v", fileName, err)
		}
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

	sort.Sort(ByKey(kva))
	outName := "mr-out-" + reduceTaskID
	outFile, _ := os.Create(outName)
	defer outFile.Close()

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(outFile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	MarkDone(task)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	//
	// read each input file,
	// pass it to Map,
	// accumulate the intermediate Map output.
	//
	for {
		task := GetTask()
		switch task.TaskType {
		case MAP:
			ProcessMapTask(task, mapf)
		case REDUCE:
			ProcessReduceTask(task, reducef)
		}
		time.Sleep(time.Second)
	}
	// uncomment to send the Example RPC to the coordinator.
	CallExample()
}

func GetTask() (task Task) {
	reply := GetTaskReply{}
	call("Coordinator.GetTask", &EmptyArgs{}, &reply)
	return reply.Task
}

func MarkDone(task Task) bool {
	args := SetTaskStateArgs{
		task,
		FINISH,
	}
	reply := BoolReply{}
	call("Coordinator.SetTaskState", &args, &reply)
	return reply.Result
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
