package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func getTask() *Task {
	args := ExampleArgs{}
	reply := Task{}
	call("Coordinator.RequestTask", &args, &reply)
	//log.Print("[worker]Get Task ", reply.TaskID)
	return &reply
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		task := getTask()
		switch task.TaskOperation {
		case Mapper:
			handleMapper(task, mapf)
		case Reducer:
			handleReducer(task, reducef)
		case ToWait:
			//log.Print("[worker]ToWait")
			time.Sleep(1 * time.Second)
		case ToExit:
			//log.Print("[worker]ToExit")
			return
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}
func handleMapper(task *Task, mapf func(string, string) []KeyValue) error {
	//log.Print("[worker]Worker handling map task:", task.TaskID)
	filename := task.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	defer file.Close()
	kva := mapf(filename, string(content))

	//按key哈希后将kva切成R份
	buffer := make([][]KeyValue, task.NReduce)
	for _, kv := range kva {
		slot := ihash(kv.Key) % task.NReduce
		buffer[slot] = append(buffer[slot], kv)
	}
	for i := 0; i < task.NReduce; i++ {
		dir, _ := os.Getwd()
		file, err := ioutil.TempFile(dir, "mr-tmp-*")
		if err != nil {
			log.Fatal("Failed to create temp file", err)
		}
		//写入键值对到R个文件
		enc := json.NewEncoder(file)

		for _, kv := range buffer[i] {
			if err := enc.Encode(&kv); err != nil {
				log.Fatalf("failed to write kv pair", err)
			}
		}
		outputName := fmt.Sprintf("mr-%d-%d", task.TaskID, i)
		os.Rename(file.Name(), outputName)
		//保存R个文件位置
		task.IntermediateFilenames = append(task.IntermediateFilenames, filepath.Join(dir, outputName))
	}
	call("Coordinator.CompleteTask", &task, &ExampleReply{})
	return nil
}
func handleReducer(task *Task, reducef func(string, []string) string) error {
	//log.Print("[worker]Worker handling reduce task:", task.TaskID)
	var kva []KeyValue
	for _, filename := range task.IntermediateFilenames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatal("[worker]cannot open ", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		err = file.Close()
		if err != nil {
			log.Fatal("[worker]cannot close ", file)
		}
	}
	sort.Sort(ByKey(kva))
	outputName := fmt.Sprintf("mr-out-%d", task.TaskID)
	temp, err := os.CreateTemp(".", outputName)
	if err != nil {
		log.Fatal("[worker]cannot create temp file ", outputName)
	}
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
		_, _ = fmt.Fprintf(temp, "%v %v\n", kva[i].Key, output)

		i = j
	}
	err = os.Rename(temp.Name(), outputName)
	if err != nil {
		return err
	}
	call("Coordinator.CompleteTask", &task, &ExampleReply{})
	return nil
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
