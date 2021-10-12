package mr

import (
	"errors"
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskType int
type TaskState int
type TaskStage int

// 常量
const (
	MaxTaskRunTime = time.Second * 10
	WaitTime       = time.Millisecond * 500
)

const (
	MAP TaskType = iota
	REDUCE
	NO
)

const (
	MAP_STAGE TaskStage = iota
	REDUCE_STAGE
	FINISH
)

const (
	IDLE TaskState = iota
	RUNNING
	FINISHED
)

type Task struct {
	taskType  TaskType  // 任务类型
	taskState TaskState // 任务状态
	taskID    string    // 任务ID，用于索引
	taskIndex int       // 任务编号，顺序编号
	filename  string    // 任务相关文件
	worker    int       // 执行任务的Worker编号
	deadline  time.Time // 任务截止时间
}

type Master struct {
	lock sync.Mutex // 保护共享信息，避免并发冲突

	nMap           int
	nReduce        int
	stage          TaskStage
	tasks          map[string]Task
	availableTasks chan Task // 管道，用于放出还没有被分配的任务
}

// Your code here -- RPC handlers for the Worker to call.
// 因为每个Worker都是一个个任务做下去的，所以只需要附带前一个任务的信息即可说明该任务已经完成
func (m *Master) DemandTask(args *ReqArgs, reply *ReplyArgs) error {
	log.Printf("Received demand from Worker %d with LastTaskType %d, LastTaskIndex %d",
		args.Worker, args.LastTaskType, args.LastTaskIndex)

	// 首先检查该Worker是否为第一次请求
	if args.LastTaskType != NO {
		m.lock.Lock()

		// 该Worker有任务已经做完，对任务进行验收
		lastTaskID := genTaskID(args.LastTaskType, args.LastTaskIndex)

		// 该任务存在而且由当前Worker负责
		if lastTask, exists := m.tasks[lastTaskID]; exists && lastTask.worker == args.Worker && lastTask.taskState != FINISHED {
			if lastTask.taskType == MAP {
				// 将中间文件标记为最终产物
				for reduceIdx := 0; reduceIdx < m.nReduce; reduceIdx++ {
					err := os.Rename(getTmpMapFilename(args.Worker, args.LastTaskIndex, reduceIdx),
						getFinalMapFilename(args.Worker, args.LastTaskIndex, reduceIdx))
					if err != nil {
						log.Fatalf("Rename Map output file failed.")
						return err
					}
				}
			} else if lastTask.taskType == REDUCE {
				// 将中间文件标记为最终产物
				err := os.Rename(
					getTmpReduceFilename(args.Worker, args.LastTaskIndex),
					getFinalReduceFilename(args.Worker, args.LastTaskIndex))
				if err != nil {
					log.Fatalf("Rename Reduce output file failed.")
					return err
				}
			} else {
				log.Printf("Check task type error\n")
				return errors.New("Check task type error")
			}

			// 将该任务标记为完成，如果所有该阶段任务都已经完成，进入下一阶段
			lastTask.taskState = FINISHED
			m.tasks[lastTask.taskID] = lastTask

			if m.CheckAllTaskState() {
				if m.stage == MAP_STAGE {
					log.Printf("All MAP tasks finished, come to REDUCE stage\n")
					m.stage = REDUCE_STAGE

					// 初始化Reduce任务
					for i := 0; i < m.nReduce; i++ {
						task := Task{
							taskType:  REDUCE,
							taskState: IDLE,
							taskIndex: i,
						}
						task.taskID = genTaskID(task.taskType, task.taskIndex)
						m.tasks[task.taskID] = task
						m.availableTasks <- task
					}
				} else if m.stage == REDUCE_STAGE {
					log.Printf("All REDUCE tasks finished, come to FINISH stage.\n")
					close(m.availableTasks)
					m.stage = FINISH
				}
			}
		}

		m.lock.Unlock()
	}

	// 返回新的任务
	if m.stage == FINISH {
		// 当前全部任务已经完成
		reply.TaskType = NO
	} else {
		task, ok := <-m.availableTasks
		if !ok {
			reply.TaskType = NO
		} else {
			m.lock.Lock()
			defer m.lock.Unlock()

			if task.taskType == MAP {
				log.Printf("Allocate MAP task %d to Worker %d\n", task.taskIndex, args.Worker)
			} else {
				log.Printf("分Allocate REDUCE task %d to Worker %d\n", task.taskIndex, args.Worker)
			}

			task.taskState = RUNNING
			task.worker = args.Worker
			task.deadline = time.Now().Add(MaxTaskRunTime)
			m.tasks[task.taskID] = task

			reply.TaskIndex = task.taskIndex
			reply.TaskType = task.taskType
			reply.Filename = task.filename
			reply.MapNum = m.nMap
			reply.ReduceNum = m.nReduce
		}
	}

	return nil
}

//
// start a thread that listens for RPCs from Worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.stage == FINISH
}

func (m *Master) CheckAllTaskState() bool {
	if m.stage == MAP_STAGE {
		for _, task := range m.tasks {
			if task.taskType == MAP && task.taskState != FINISHED {
				return false
			}
		}
	} else if m.stage == REDUCE_STAGE {
		for _, task := range m.tasks {
			if task.taskType == REDUCE && task.taskState != FINISHED {
				return false
			}
		}
	}
	return true
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		nReduce:        nReduce,
		nMap:           len(files),
		stage:          MAP_STAGE,
		tasks:          make(map[string]Task),
		availableTasks: make(chan Task, int(math.Max(float64(len(files)), float64(nReduce)))),
	}
	log.Printf("Master Server initialized already...")

	// 初始化Map任务
	for i, file := range files {
		task := Task{
			taskType:  MAP,
			taskState: IDLE,
			filename:  file,
			taskIndex: i,
		}
		task.taskID = genTaskID(task.taskType, task.taskIndex)
		m.tasks[task.taskID] = task
		m.availableTasks <- task
	}
	log.Printf("Map task generated successfully")

	m.server()

	// 启动 Task 自动回收过程
	go func() {
		for {
			m.lock.Lock()
			for _, task := range m.tasks {
				if task.taskState == RUNNING && time.Now().After(task.deadline) {
					// 回收并重新分配
					log.Printf("Find timeout task %s , garbage collection started.\n", task.taskID)
					task.taskState = IDLE
					m.tasks[task.taskID] = task
					m.availableTasks <- task
				}
			}
			m.lock.Unlock()

			time.Sleep(WaitTime)
		}
	}()

	return &m
}

func genTaskID(taskType TaskType, taskIndex int) string {
	if taskType == MAP {
		return fmt.Sprintf("map-%d", taskIndex)
	} else if taskType == REDUCE {
		return fmt.Sprintf("reduce-%d", taskIndex)
	} else {
		return ""
	}
}
