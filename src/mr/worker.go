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
	"sync"
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
// x: the id of this map worker
// mapf: the Map Process of this model
//
func DoMap(x int, nReduce int,filename string,mapf func(string, string) []KeyValue) ([]string,error){

	// Step 1: Read from Input file
	file, err := os.Open(filename)

	if err != nil{
		log.Fatalf("cannot open %v", filename)
		return nil,err
	}
	content, err:= ioutil.ReadAll(file)
	if err!= nil{
		log.Fatalf("cannot read %v", filename)
		return nil, err
	}

	file.Close()

	// Step 2: Run Map function
	kva :=mapf(filename,string(content))

	// Step 3: Partition the result of this Map process to intermediate k/v
	var intermediate []ByKey
	for i:= 0; i< nReduce; i++{
		intermediate = append(intermediate, ByKey{})
	}

	for _,kv := range kva{
		r := ihash(kv.Key) % nReduce
        intermediate[r] = append(intermediate[r], kv)
	}


	// Step 4: Intermediate to disk
	var intermediates []string

	for i := 0; i < nReduce ; i++ {

		reduceName := fmt.Sprintf("/tmp/reduce_%v_%v.txt", x, i)
		intermediates = append(intermediates,reduceName)
		ofile, _ := os.Create(reduceName)
		enc := json.NewEncoder(ofile)
		for _, kv := range intermediate[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("Step 4: Intermediate to disk error %v",filename)
				return nil, err
			}
		}
		ofile.Close()
	}

	// Return the related intermediate files
	return intermediates, nil
}

func DoReduce(r int, intermediates []string, reducef func(string, []string) string) error {

	// Step: Get the intermediates from Map Worker
	var kvas ByKey
	for _, filename := range intermediates{
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
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
		kvas = append(kvas,kva...)
		file.Close()
	}

	// Step 2: sort by key to find all same keys
	sort.Sort(kvas)
	oname := fmt.Sprintf("mr-out-%v", r)
	ofile, err := os.Create(oname)
	if err!= nil{
		log.Fatalf("cannot Create file %v", oname)
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

	return nil
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	var count sync.WaitGroup
	inMap := true
	x := 99
	nReduce := 0
	nMap := 0
	for inMap {
		reply := CallExample(x)
		inMap = !reply.IsReduce
		if inMap {
			count.Add(1)
			x = reply.Y
			if nReduce == 0 {
				nReduce = reply.NReduce
				nMap = reply.NMap
			}
			// Map Worker
			go func(x, nReduce int, filename string) {

				defer count.Done()
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
				sort.Sort(ByKey(kva))

				// todo 优化
				var kvGroup [100]ByKey
				// Partition to R reduce
				for _, kv := range kva {
					k := ihash(kv.Key) % nReduce
					kvGroup[k] = append(kvGroup[k], kv)
				}

				for i := 0; i < nReduce; i++ {
					reduceName := fmt.Sprintf("/tmp/reduce_%v_%v.txt", x, i)
					ofile, _ := os.Create(reduceName)
					enc := json.NewEncoder(ofile)
					for _, kv := range kvGroup[i] {
						err := enc.Encode(&kv)
						if err != nil {
							// todo nothing to do here now!
						}
					}
					ofile.Close()
				}

			}(x, nReduce, reply.File)
		}
	}

	// 等待所有的执行完毕
	count.Wait()

	// Reduce Worker
	for r := 0; r < nReduce; r++ {

		count.Add(1)

		go func(r,nMap,x int) {
			defer count.Done()
			var kvas ByKey
			CallExample(r)
			for i:=0; i<nMap;i++{
				filename := fmt.Sprintf("/tmp/reduce_%d_%d.txt", i+100, r)
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
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
				kvas = append(kvas,kva...)
				file.Close()
			}

			sort.Sort(kvas)
			oname := fmt.Sprintf("mr-out-%v", r)
			ofile, _ := os.Create(oname)

			i := 0
			for i < len(kvas) {
				j := i + 1
				for j < len(kvas) && kvas[j].Key == kvas[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kvas[k].Value)
				}
				output := reducef(kvas[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", kvas[i].Key, output)

				i = j
			}

			ofile.Close()
		}(r,nMap,x)
		count.Wait()
		// notify all tasks has been finished
		CallExample(-1)
		fmt.Println(" all tasks has been finished !")

	}

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample(x int) ExampleReply {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = x

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	if call("Master.Example", &args, &reply) {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
		return reply
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
