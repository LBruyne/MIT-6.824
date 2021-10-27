package kvraft

import (
	"labrpc"
	"sync"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd

	// Polling, save the id of the last leader
	// if the request failed, change it to the next one:
	// lastLeader = ( lastLeader + 1 ) % len(servers)
	lastLeader int

	// client id
	clerkId int

	// request sequence id
	seqId int

	// mutex
	mu sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.lastLeader = NONE

	ck.clerkId = int(nrand())
	ck.seqId = 0

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	DPrintf("[Client %v]: Try to Get %v.", ck.clerkId, key)

	ck.mu.Lock()
	ck.seqId++
	seqId, clerkId := ck.seqId, ck.clerkId
	ck.mu.Unlock()

	var current int
	if ck.lastLeader == NONE {
		current = 0
	} else {
		current = ck.lastLeader
	}

	for {
		args := GetArgs{
			Key:   key,
			Clerk: ck.clerkId,
			Seq:   ck.seqId,
		}

		reply := GetReply{}

		// Call
		DPrintf("[Client %v]: Call server %v to Get the Key %v, Seq %v.", clerkId, current, key, seqId)
		ok := ck.servers[current].Call("KVServer.Get", &args, &reply)

		if ok && reply.Err != ErrWrongLeader {
			ck.lastLeader = current
			if reply.Err == OK {
				DPrintf("[Client %v]: Call server %v to Get the Key %v, success get value %v", clerkId, current, args.Key, reply.Value)
				return reply.Value
			} else if reply.Err == ErrNoKey {
				DPrintf("[Client %v]: Call server %v to Get the Key %v, indeed a leader, but get a internal ErrNoKey", clerkId, current, args.Key)
				return ""
			} else if reply.Err == ErrInternalError || reply.Err == ErrExecTimeout {
				DPrintf("[Client %v]: Call server %v to Get the Key %v, indeed a leader, but get a internal Error %v, try again.",
					clerkId, current, args.Key, reply.Err)
			}
		} else {
			DPrintf("[Client %v]: Call server %v to Get the Key %v, not a leader or failed, try another server", clerkId, current, args.Key)
			current = (current + 1) % len(ck.servers)
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	DPrintf("[Client %v]: Try to %v the pair Key: %v, Value: %v.", ck.clerkId, op, key, value)

	ck.mu.Lock()
	ck.seqId++
	seqId, clerkId := ck.seqId, ck.clerkId
	ck.mu.Unlock()

	var current int
	if ck.lastLeader == NONE {
		current = 0
	} else {
		current = ck.lastLeader
	}

	for {
		args := PutAppendArgs{
			Key:   key,
			Value: value,
			Clerk: ck.clerkId,
			Seq:   ck.seqId,
			Op:    op,
		}

		reply := PutAppendReply{}

		// Call
		DPrintf("[Client %v]: Call server %v to %v the pair Key: %v, Value: %v, Seq: %v.", clerkId, current, op, key, value, seqId)
		ok := ck.servers[current].Call("KVServer.PutAppend", &args, &reply)

		if ok && reply.Err != ErrWrongLeader {
			ck.lastLeader = current
			if reply.Err == OK {
				DPrintf("[Client %v]: Call server %v to PutAppend the Key %v, Value: %v, success", clerkId, current, args.Key, args.Value)
				return
			} else if reply.Err == ErrInternalError || reply.Err == ErrExecTimeout {
				DPrintf("[Client %v]: Call server %v to PutAppend the Key %v, Value: %v, indeed a leader, but get a internal Error %v, try again.",
					clerkId, current, args.Key, args.Value, reply.Err)
			}
		} else {
			DPrintf("[Client %v]: Call server %v to PutAppend the Key %v, Value: %v, not a leader, try another server", clerkId, current, args.Key, args.Value)
			current = (current + 1) % len(ck.servers)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, OpPut)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, OpAppend)
}
