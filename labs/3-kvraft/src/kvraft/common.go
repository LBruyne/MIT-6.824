package kvraft

import "log"

type Operator = string

const (
	OK               = "OK"
	ErrNoKey         = "ErrNoKey"
	ErrWrongLeader   = "ErrWrongLeader"
	ErrExecTimeout   = "ErrExecTimeout"
	ErrInternalError = "ErrInternalError"

	NONE = -1

	OpPut    Operator = "Put"
	OpAppend Operator = "Append"
	OpGet    Operator = "Get"

	CmdExecTimeout = 2
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    Operator // "Put" or "Append"
	Seq   int
	Clerk int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key   string
	Seq   int
	Clerk int
}

type GetReply struct {
	Err   Err
	Value string
}
