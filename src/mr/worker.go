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
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
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
// use DoMap handle the input files
// filename: the filename of input
// nReduce: the number of Reduce
// m: the id of this map worker
// mapf: the Map Process of this model
//
func DoMap(m int, nReduce int, filename string, mapf func(string, string) []KeyValue) error {

	// Step 1: Read from Input file
	file, err := os.Open(filename)

	if err != nil {
		log.Fatalf(" DoMap cannot open %v", filename)
		return err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("DoMap cannot read %v", filename)
		return err
	}

	file.Close()

	// Step 2: Run Map function
	kva := mapf(filename, string(content))

	// Step 3: Partition the result of this Map process to intermediate k/v
	var intermediate []ByKey
	for i := 0; i < nReduce; i++ {
		intermediate = append(intermediate, ByKey{})
	}

	for _, kv := range kva {
		r := ihash(kv.Key) % nReduce
		intermediate[r] = append(intermediate[r], kv)
	}

	// Step 4: Intermediate to disk
	var intermediates []string

	for i := 0; i < nReduce; i++ {

		reduceName := fmt.Sprintf("/tmp/reduce_%v_%v.txt", m, i)
		intermediates = append(intermediates, reduceName)
		ofile, _ := os.Create(reduceName)
		enc := json.NewEncoder(ofile)
		for _, kv := range intermediate[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("DoMap Step 4: Intermediate to disk error %v", filename)
				return err
			}
		}
		ofile.Close()
	}

	// Notify the master
	CallNotify(m, intermediates, false)

	// Return the related intermediate files
	return nil
}

func DoReduce(r int, intermediates []string, reducef func(string, []string) string) error {

	// Step: Get the intermediates from Map Worker
	var kvas ByKey
	for _, filename := range intermediates {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("DoReduce cannot open %v", filename)
			return err
		}
		var kva ByKey
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		kvas = append(kvas, kva...)
		file.Close()
	}

	// Step 2: sort by key to find all same keys
	sort.Sort(kvas)
	oname := fmt.Sprintf("mr-out-%v", r)
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("DoReduce cannot Create file %v", oname)
		return err
	}

	// Step 3: storage the result to the final file
	i := 0
	for i < len(kvas) {

		j := i + 1
		for j < len(kvas) && kvas[j].Key == kvas[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kvas[k].Value)
		}
		output := reducef(kvas[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kvas[i].Key, output)

		i = j
	}

	ofile.Close()

	CallNotify(r, []string{oname}, true)

	return nil
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	mapProcess := true
	x := 99
	for mapProcess {
		reply := CallGetMapTask(x)
		x += 1
		mapProcess = !reply.Finished
		if mapProcess && reply.Y != 0 {
			DoMap(reply.Y, reply.NReduce, reply.File, mapf)
		}
	}

	fmt.Println("Map Process has finished !")

	reduceProcess := true
	for reduceProcess {
		reply := CallGetReduceTask(x)
		x += 1
		reduceProcess = !reply.Finished
		if reduceProcess && reply.Y != 0 {
			DoReduce(reply.Y, reply.Files, reducef)
		}
	}

	fmt.Println("Reduce Process has finished !")

}

func CallGetReduceTask(x int) ReduceTaskReply {

	args := ReduceTaskArgs{X: x}

	reply := ReduceTaskReply{}

	if call("Master.GetReduceTask", &args, &reply) && reply.Y != 0 {
		//fmt.Printf("CallGetReduceTask reply.Y %v reply.File  %v\n", reply.Y, reply.Files)
	}

	return reply
}

func CallGetMapTask(x int) MapTaskReply {

	args := MapTaskArgs{X: x}

	reply := MapTaskReply{}

	if call("Master.GetMapTask", &args, &reply) && reply.Y != 0{
		//fmt.Printf("CallGetMapTask reply.Y %v reply.File  %v\n", reply.Y, reply.File)
	}

	return reply
}

func CallNotify(x int, files []string, reduce bool) TaskNotifyReply {

	args := TaskNotifyArgs{X: x, Files: files, IsReduce: reduce}
	reply := TaskNotifyReply{}

	if call("Master.TaskNotify", &args, &reply)  {
		//fmt.Printf("CallNotify reply.OK %v\n", reply.Ok)
	}
	return reply
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
