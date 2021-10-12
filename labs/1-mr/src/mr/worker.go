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
	"time"
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
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// 使用进程号作为Worker的ID
	id := os.Getpid()
	log.Printf("Worker %d started.\n", id)

	// 不停的循环，向master请求任务并执行，直到没有任务
	var lastTaskIndex int
	lastTaskType := NO
	running := true
	for running {
		args := ReqArgs{
			Worker:        id,
			LastTaskIndex: lastTaskIndex,
			LastTaskType:  lastTaskType,
		}

		reply := ReplyArgs{}

		// 发起RPC调用
		log.Printf("Worker %d call Master.DemandTask with args LastTaskIndex: %d, LastTaskType: %d",
			id, lastTaskIndex, lastTaskType)
		call("Master.DemandTask", &args, &reply)
		log.Printf("Worker %d received reply: TaskIndex: %d, TaskType: %d, Filename: %s, MapNum: %d, ReduceNum: %d",
			id, reply.TaskIndex, reply.TaskType, reply.Filename, reply.MapNum, reply.ReduceNum)

		if reply.TaskType == NO {
			running = false
			log.Printf("Task finished, %d process quit.\n", id)
		} else if reply.TaskType == MAP {
			log.Printf("%d received MAP task %d\n", id, reply.TaskIndex)

			// 执行Map任务
			filename := reply.Filename
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()

			// 经过一次map后，kva中包含的是一个（w，1）的数组，代表每个w出现了1次（每个w可能在数组中多次出现，代表该w在文章中多次出现）
			kva := mapf(filename, string(content))

			// 按 Key 的 Hash 值对中间结果进行分桶
			hashedKva := make(map[int][]KeyValue)
			for _, kv := range kva {
				hashed := ihash(kv.Key) % reply.ReduceNum
				hashedKva[hashed] = append(hashedKva[hashed], kv)
			}

			// 将中间值写到临时文件中，完成提交
			for reduceIdx := 0; reduceIdx < reply.ReduceNum; reduceIdx++ {
				tfilename := getTmpMapFilename(id, reply.TaskIndex, reduceIdx)
				tfile, err := os.Create(tfilename)
				if err != nil {
					log.Fatalf("cannot create %v", tfilename)
					return
				}

				// 以json的形式存储
				enc := json.NewEncoder(tfile)
				for _, kv := range hashedKva[reduceIdx] {
					err := enc.Encode(&kv)
					if err != nil {
						log.Fatalf("cannot encode %v", kv)
					}
				}

				tfile.Close()
			}
		} else {
			log.Printf("%d received REDUCE task %d\n", id, reply.TaskIndex)

			// 执行Reduce任务
			var kva []KeyValue
			for mapIdx := 0; mapIdx < reply.MapNum; mapIdx++ {
				mfilename := getFinalMapFilename(id, mapIdx, reply.TaskIndex)
				mfile, err := os.Open(mfilename)
				if err != nil {
					log.Fatalf("cannot open %v", mfilename)
					return
				}

				dec := json.NewDecoder(mfile)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, KeyValue{Key: kv.Key, Value: kv.Value})
				}

				mfile.Close()
			}

			sort.Sort(ByKey(kva))

			ofile, _ := os.Create(getTmpReduceFilename(id, reply.TaskIndex))

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
				// 返回的是每个Key出现的总次数
				output := reducef(kva[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

				i = j
			}

			ofile.Close()
		}

		// 更新上次任务类型和编号
		lastTaskType = reply.TaskType
		lastTaskIndex = reply.TaskIndex
		log.Printf("Finish task %s", genTaskID(lastTaskType, lastTaskIndex))

		// 休息一下重新发起请求
		time.Sleep(WaitTime)
	}
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

func getTmpMapFilename(worker int, mapIdx int, reduceIdx int) string {
	return fmt.Sprintf("tmp-map-%d-%d-%d", worker, mapIdx, reduceIdx)
}

func getFinalMapFilename(worker int, mapIdx int, reduceIdx int) string {
	return fmt.Sprintf("mr-%d-%d", mapIdx, reduceIdx)
}

func getTmpReduceFilename(worker int, reduceIdx int) string {
	return fmt.Sprintf("tmp-reduce-%d-%d", worker, reduceIdx)
}

func getFinalReduceFilename(worker int, reduceIdx int) string {
	return fmt.Sprintf("mr-out-%d", reduceIdx)
}
