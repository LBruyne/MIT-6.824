package kvraft

import (
	"labgob"
	"labrpc"
	"raft"
	"sync"
	"sync/atomic"
	"time"
)

type Op struct {
	Key   string
	Value string
	Clerk int
	Seq   int
	Op    Operator
}

func (o Op) equals(cmd2 Op) bool {
	// compare between basic types
	return o == cmd2
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// store
	keyValue map[string]string

	// sequence for each client
	clerkSeq map[int]int

	// channels for each cmd
	cmdChannels map[int]chan Op
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DPrintf("[Server %v]: Received a Get request, Key is %v, Seq is %v, Clerk is %d.", kv.me, args.Key, args.Seq, args.Clerk)

	// process expired Get request.
	kv.mu.Lock()
	if maxSeq, ok := kv.clerkSeq[args.Clerk]; ok && maxSeq > args.Seq {

		DPrintf("[Server %v]: Received a Expired Get request, read-only for it.", kv.me)

		if value, ok := kv.keyValue[args.Key]; ok {
			DPrintf("[Server %v]: Get success, Key %v, Value %v.", kv.me, args.Key, value)
			reply.Err, reply.Value = OK, value
		} else {
			DPrintf("[Server %v]: Key %v is not existed.", kv.me, args.Key)
			reply.Err, reply.Value = ErrNoKey, ""
		}

		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	if _, isLeader := kv.rf.GetState(); isLeader {
		reply.Err = OK

		// create a new Op and let the server Start it
		cmd := Op{
			Key:   args.Key,
			Value: "",
			Op:    OpGet,
			Seq:   args.Seq,
			Clerk: args.Clerk,
		}

		// request to Start this cmd
		index, _, leader := kv.rf.Start(cmd)

		if !leader {
			DPrintf("[Server %v]: Not leader, refuse the request.", kv.me)
			reply.Err, reply.Value = ErrWrongLeader, ""
			return
		}

		cmd2 := Op{}
		select {
		case cmd2 = <-kv.getApplyCh(index):
			// in a limit time duration, this cmd is executed by most of the server.
			DPrintf("[Server %v]: Execute the cmd success.", kv.me)
			reply.Err = OK
		case <-time.After(CmdExecTimeout * time.Second):
			// execute timeout, which means this server may not be a leader or the consensus has not been made.
			DPrintf("[Server %v]: Execute request timeout, refuse it.", kv.me)
			reply.Err, reply.Value = ErrExecTimeout, ""
			return
		}

		// validate
		if cmd.equals(cmd2) {
			// Get
			kv.mu.Lock()
			if value, ok := kv.keyValue[cmd2.Key]; ok {
				DPrintf("[Server %v]: Get success, Key %v, Value %v.", kv.me, cmd2.Key, value)
				reply.Value = value
			} else {
				DPrintf("[Server %v]: Key %v is not existed.", kv.me, cmd2.Key)
				reply.Err, reply.Value = ErrNoKey, ""
			}
			kv.mu.Unlock()
		} else {
			DPrintf("[Server %v]: Internal error, cmd != cmd2.", kv.me)
			reply.Err, reply.Value = ErrInternalError, ""
		}
	} else {
		DPrintf("[Server %v]: Not leader, refuse the request.", kv.me)
		reply.Err, reply.Value = ErrWrongLeader, ""
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("[Server %v]: Received a PutAppend request, Key is %v, Value is %v, Op is %v, Seq is %v, Clerk is %d.",
		kv.me, args.Key, args.Value, args.Op, args.Seq, args.Clerk)

	if _, isLeader := kv.rf.GetState(); isLeader {
		reply.Err = OK

		// create a new Op and let the server Start it
		cmd := Op{
			Key:   args.Key,
			Value: args.Value,
			Op:    args.Op,
			Seq:   args.Seq,
			Clerk: args.Clerk,
		}

		// request to Start this cmd
		index, _, leader := kv.rf.Start(cmd)

		if !leader {
			DPrintf("[Server %v]: Not leader, refuse the request.", kv.me)
			reply.Err = ErrWrongLeader
			return
		}

		cmd2 := Op{}
		select {
		case cmd2 = <-kv.getApplyCh(index):
			// in a limit time duration, this cmd is executed by most of the server.
			DPrintf("[Server %v]: Execute the cmd success.", kv.me)
			reply.Err = OK
		case <-time.After(CmdExecTimeout * time.Second):
			// execute timeout, which means this server may not be a leader or the consensus has not been made.
			DPrintf("[Server %v]: Execute request timeout, refuse it.", kv.me)
			reply.Err = ErrExecTimeout
			return
		}

		// validate
		if cmd.equals(cmd2) {
			// Put or Append has completed.
			DPrintf("[Server %v]: %v success, Key %v, Value %v.", kv.me, args.Op, cmd2.Key, cmd2.Value)
		} else {
			DPrintf("[Server %v]: Internal error, cmd != cmd2.", kv.me)
			reply.Err = ErrInternalError
		}
	} else {
		DPrintf("[Server %v]: Not leader, refuse the request.", kv.me)
		reply.Err = ErrWrongLeader
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// initialize
	kv.keyValue = make(map[string]string)
	kv.clerkSeq = make(map[int]int)
	kv.cmdChannels = make(map[int]chan Op)

	// begin to process applied message
	go kv.processApplyMsg()

	return kv
}

func (kv *KVServer) processApplyMsg() {
	for {
		if kv.killed() {
			return
		} else {
			select {
			// wait for a new applyCh message.
			case msg := <-kv.applyCh:
				id, cmd := msg.CommandIndex, msg.Command.(Op)
				DPrintf("[Server %v]: Applied command %v, command is: Key %v, Value %v, Op %v, Seq %v, Clerk %v.",
					kv.me, id, cmd.Key, cmd.Value, cmd.Op, cmd.Seq, cmd.Clerk)

				kv.mu.Lock()

				// check if this command with this clerk's this seq has been received,
				// if maxSeq doesn't exist (ok == false),
				// or if maxSeq exists and maxSeq < seq,
				// then this command is the newest one. Update.
				key, value, clerk, seq := cmd.Key, cmd.Value, cmd.Clerk, cmd.Seq
				if maxSeq, ok := kv.clerkSeq[clerk]; !ok || maxSeq < seq {
					// execute command to update the KV database
					switch cmd.Op {
					case OpGet:
						// get
					case OpAppend:
						// append
						oldValue, ok := kv.keyValue[key]
						if ok {
							kv.keyValue[key] = oldValue + value
						} else {
							kv.keyValue[key] = value
						}
						DPrintf("[Server %v]: Update database: Key %v, Value %v.",
							kv.me, key, kv.keyValue[key])
					case OpPut:
						// put
						kv.keyValue[key] = value
						DPrintf("[Server %v]: Update database: Key %v, Value %v.",
							kv.me, key, kv.keyValue[key])
					}
					// update the maxSeq
					kv.clerkSeq[clerk] = seq
					DPrintf("[Server %v]: Executed clerk %v 's command index %v.",
						kv.me, clerk, seq)
				} else {
					DPrintf("[Server %v]: Command %v is duplicated, Op %v, Seq %v, Clerk %v, dismiss it.", kv.me, id, cmd.Op, cmd.Seq, cmd.Clerk)
				}

				kv.mu.Unlock()

				// notice the server for applied message
				kv.noticeApplyMsg(id, cmd)
			}
		}
	}
}

func (kv *KVServer) noticeApplyMsg(id int, cmd Op) {
	// get the channel for this command, and push this cmd into channel,
	// so that the RPC process function will know that,
	// the cmd with this index is executed by most state machine.
	kv.getApplyCh(id) <- cmd
}

func (kv *KVServer) getApplyCh(id int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if ch, ok := kv.cmdChannels[id]; !ok {
		kv.cmdChannels[id] = make(chan Op, 1)
		return kv.cmdChannels[id]
	} else {
		return ch
	}
}
