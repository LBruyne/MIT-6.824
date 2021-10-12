package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// RPC请求参数
type ReqArgs struct {
	Worker int // 请求发起的Worker标志

	// 上一个任务的信息
	LastTaskType  TaskType
	LastTaskIndex int
}

// RPC响应参数
type ReplyArgs struct {
	TaskType  TaskType // task类型
	TaskIndex int      // task编号
	Filename  string   // Map任务涉及的文件
	MapNum    int      // Map任务的数量
	ReduceNum int      // Reduce任务的数量
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
