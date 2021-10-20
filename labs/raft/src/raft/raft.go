package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"labgob"
	"labrpc"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// ApplyMsg
// as each Raft peer becomes aware that successive log Entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// simulate a log committer
	applyCh chan ApplyMsg

	// ref to Figure 2 in paper.
	currentState StateType

	// persistent state on all servers.
	currentTerm int        // 当前的Term
	votedFor    int        // 当前投票给谁
	log         []LogEntry // Log Entries, 每一项都是一条给予状态机的命令（包括这条命令被Leader收到时的当前任期）

	// volatile state on all servers.
	commitIndex int // 已知最新被提交的log entry的索引
	lastApplied int // 已经最新被应用在状态机内的log entry的索引

	// volatile state on leaders.
	nextIndex  []int // 对于每个Follower来说，下一个将要被发送到该Server的log entry的索引
	matchIndex []int // 对于每个Follower来说，最新的已知已经被复制到了这个Sever的log entry的索引

	// timeout
	electionTimeout float64

	// timer
	timer *time.Timer

	// vote count
	voteCount int32
}

type LogEntry struct {
	Command interface{}
	Term    int
}

// GetState return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.currentState == StateLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {

	// persist
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)

	DPrintf("%d [term: %d] Persist state finished, currentTerm: %d, votedFor: %d, log length: %d",
		rf.me, rf.currentTerm, rf.currentTerm, rf.votedFor, len(rf.log)-1)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	// read persist
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)

	DPrintf("%d [term: %d] Read persist state successfully, currentTerm: %d, votedFor: %d, log length: %d",
		rf.me, rf.currentTerm, rf.currentTerm, rf.votedFor, len(rf.log)-1)
}

type AppendEntriesArgs struct {
	Term         int        // Leader的任期
	LeaderId     int        // Leader的ID
	PrevLogIndex int        // 候选者上一条log entry
	PrevLogTerm  int        // 候选者上一条log entry的任期
	Entries      []LogEntry // 将要被传送的log entry，如果是纯心跳包，为空
	LeaderCommit int        // Leader的commitIndex
}

type AppendEntriesReply struct {
	Term    int  // 当前任期，用于Leader去自己更新
	Success bool // Follower是否含有吻合prevLogIndex和prevLogTerm的log entry
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("%d [term: %d] Receive a AppendEntries from %d, term is %d, prevLogIndex is %d, prevLogTerm is %d, leaderCommit is %d.",
		rf.me, rf.currentTerm, args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)

	// check term
	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower
	// or,
	// If AppendEntries RPC received from new leader and term T >= currentTerm:
	// it believes the RPC is from the current leader, covert to follower as well
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	reply.Term, reply.Success = rf.currentTerm, false

	prevLogExist := (args.PrevLogIndex <= 0) ||
		((len(rf.log) > args.PrevLogIndex) && (rf.log[args.PrevLogIndex].Term == args.PrevLogTerm))
	if args.Term < rf.currentTerm {
		// 1. check term
		DPrintf("%d [term: %d] Refuse AppendEntries from %d, term is %d due to a lower term (Law 1).",
			rf.me, rf.currentTerm, args.LeaderId, args.Term)
	} else if prevLogExist == false {
		// 2. reply false if log doesn’t contain an entry at prevLogIndex
		//    whose term matches prevLogTerm
		DPrintf("%d [term: %d] Refuse AppendEntries from %d, term is %d due to Law 2.",
			rf.me, rf.currentTerm, args.LeaderId, args.Term)
		// reset timeout
		rf.rstTimer()
	} else {
		// this RPC did from the leader.
		rf.currentTerm = args.Term
		rf.currentState = StateFollower
		// persist
		rf.persist()

		// 3. if an existing entry conflicts with a new one (same index but different terms),
		//    delete the existing entry and all that follow it.
		nextIndex := args.PrevLogIndex + 1

		var appendIndex int
		for appendIndex = 0; appendIndex < len(args.Entries); appendIndex++ {
			entry := args.Entries[appendIndex]
			if nextIndex+appendIndex < len(rf.log) && rf.log[nextIndex+appendIndex].Term != entry.Term {
				DPrintf("%d [term: %d] Find log inconsistency, old log is index: %d [term: %d], new log is index: %d [term: %d], execute a cleaning",
					rf.me, rf.currentTerm, nextIndex+appendIndex, rf.log[nextIndex+appendIndex].Term, appendIndex, entry.Term)
				rf.log = rf.log[:nextIndex+appendIndex]

				// persist
				rf.persist()
				break
			} else if nextIndex+appendIndex >= len(rf.log) {
				break
			} else { // rf.log[nextIndex+i].Term == entry.Term

			}
		}

		// 4. append any new entries not already in the log
		for appendIndex < len(args.Entries) {
			rf.log = append(rf.log, args.Entries[appendIndex])
			DPrintf("%d [term: %d] Add entry to log, appendIndex is %d, index in log is %d",
				rf.me, rf.currentTerm, appendIndex, len(rf.log)-1)
			appendIndex++

			// persist
			rf.persist()
		}

		// 5. if leaderCommit > commitIndex,
		//    set commitIndex = min(leaderCommit, index of last new entry)
		if args.LeaderCommit > rf.commitIndex {
			indexOfLastNewEntry := len(rf.log) - 1
			rf.commitIndex = int(math.Min(float64(indexOfLastNewEntry), float64(args.LeaderCommit)))
			DPrintf("%d [term: %d] Find commitIndex < leaderCommit, update commitIndex to %d = min(leaderCommit %d, index of last new entry %d)",
				rf.me, rf.currentTerm, rf.commitIndex, args.LeaderCommit, indexOfLastNewEntry)
		}

		// apply logs
		go rf.applyLogs()

		DPrintf("%d [term: %d] AppendEntries success from %d, term is %d, current state is: commitIndex %d, lastApplied %d, log length %d.",
			rf.me, rf.currentTerm, args.LeaderId, args.Term, rf.commitIndex, rf.lastApplied, len(rf.log)-1)
		reply.Success = true
		// reset timeout
		rf.rstTimer()
	}
	return
}

func (rf *Raft) handleAppendEntriesReply(success bool, recvTerm int, server int, currentIdx int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// check current state
	if rf.currentState != StateLeader {
		DPrintf("%d [term: %d] Leader state has changed, stop to process the reply of ApplyEntries.",
			rf.me, rf.currentTerm)
		return
	}

	if recvTerm < rf.currentTerm {
		// impossible actually
		DPrintf("%d [term: %d] Receive ApplyEntriesReply with lower term.", rf.me, rf.currentTerm)
		return
	} else if recvTerm > rf.currentTerm {
		DPrintf("%d [term: %d] Receive ApplyEntriesReply with higher term, convert to StateFollower.", rf.me, rf.currentTerm)
		rf.convertToFollower(recvTerm)
		return
	}

	// the param currentIndex is the len(rf.log)-1
	if success {
		// apply entries success,
		// update nextIndex and matchIndex for follower
		DPrintf("%d [term: %d] ApplyEntries to %d success, current index is %d.", rf.me, rf.currentTerm, server, currentIdx)
		rf.nextIndex[server] = currentIdx + 1
		rf.matchIndex[server] = currentIdx

		// If there exists an N such that N > commitIndex,
		// a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:
		// set commitIndex = N
		rf.checkCommitIndex(server)
	} else {
		// apply entries fail,
		// If AppendEntries fails because of log inconsistency,
		// decrement nextIndex and retry
		DPrintf("%d [term: %d] ApplyEntries for entry %d fail because of log inconsistency, decrement the nextIndex.", rf.me, rf.currentTerm, currentIdx)
		rf.nextIndex[server]--
	}
}

type RequestVoteArgs struct {
	Term         int // 候选者的任期
	CandidateId  int // 候选者的ID
	LastLogIndex int // 候选者上一条log entry
	LastLogTerm  int // 候选者上一条log entry的任期
}

type RequestVoteReply struct {
	Term        int  // 当前任期，用于让候选者去自己更新
	VoteGranted bool // 是否投票
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("%d [term: %d] Receive a RequestVote from %d, term is %d, LastTermIndex is %d, LastLogTerm is %d.",
		rf.me, rf.currentTerm, args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm)

	// check term
	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	reply.VoteGranted, reply.Term = false, rf.currentTerm

	// 1. reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		DPrintf("%d [term: %d] Refuse RequestVote from %d, term is %d due to a lower term (Law 1).",
			rf.me, rf.currentTerm, args.CandidateId, args.Term)
		return
	}

	// 2.
	// votedFor is null or candidateId
	canVote := (rf.votedFor == args.CandidateId) || (rf.votedFor == NONE)
	// and candidate’s log is at least as up-to-date as receiver’s log
	if canVote && rf.isLogUpToDate(args.LastLogTerm, args.LastLogIndex) {
		rf.votedFor = args.CandidateId
		// persist
		rf.persist()

		reply.VoteGranted, reply.Term = true, rf.currentTerm
		// reset electionTimeout
		rf.rstTimer()
		DPrintf("%d [term: %d] Grant RequestVote from %d, term is %d.",
			rf.me, rf.currentTerm, args.CandidateId, args.Term)
	} else {
		DPrintf("%d [term: %d] Refuse RequestVote from %d, term is %d due to Law 2.",
			rf.me, rf.currentTerm, args.CandidateId, args.Term)
	}

	return
}

func (rf *Raft) handleRequestVoteResult(recvTerm int, granted bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentState != StateCandidate {
		DPrintf("%d [term: %d] Candidate state has changed, stop to process the reply of RequestVote.",
			rf.me, rf.currentTerm)
		return
	}

	if recvTerm < rf.currentTerm {
		// impossible actually
		DPrintf("%d [term: %d] Receive RequestVote with lower term.", rf.me, rf.currentTerm)
		return
	} else if recvTerm > rf.currentTerm {
		DPrintf("%d [term: %d] Receive RequestVote with higher term, convert to StateFollower.", rf.me, rf.currentTerm)
		rf.convertToFollower(recvTerm)
		return
	}

	if rf.currentState == StateCandidate && granted == true {
		rf.voteCount++
		if rf.isElectionSuccess() {
			DPrintf("%d [term: %d] Win a election.", rf.me, rf.currentTerm)
			rf.convertToLeader()
		}
	}
}

func (rf *Raft) isLogUpToDate(term int, index int) bool {
	// get last log information.
	lastIndex, lastTerm := rf.getLastLogInfo()

	// whether the log is up to date.
	return term > lastTerm || (term == lastTerm && index >= lastIndex)
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// initialize.
	index := -1
	term := -1
	isLeader := true

	if rf.currentState != StateLeader {
		// if it's not the leader
		DPrintf("%d [term: %d] Receive a command %v from client not as leader, refuse it.",
			rf.me, rf.currentTerm, command)
		isLeader = false
	} else {
		// else, transfer and commit the log
		log := LogEntry{
			Command: command,
			Term:    rf.currentTerm,
		}
		rf.log = append(rf.log, log)
		index, term = len(rf.log)-1, log.Term
		DPrintf("%d [term: %d] Receive a command %v from client as leader, append it to log, outer index is %d, index in log is %d.",
			rf.me, rf.currentTerm, command, index, len(rf.log)-1)

		// persist
		rf.persist()
	}
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) sendHeartBeats() {
	for {
		// check current state
		if rf.currentState != StateLeader {
			DPrintf("%d [term: %d] Leader state has changed, stop to send heartbeat package.",
				rf.me, rf.currentTerm)
			return
		}

		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			LeaderCommit: rf.commitIndex,
		}

		for i := range rf.peers {
			if i == rf.me {
				// 如果是自己，不发送心跳包
				continue
			} else {
				// index of log entry immediately preceding new ones
				args.PrevLogIndex = rf.nextIndex[i] - 1

				// term of prevLogIndex entry
				if args.PrevLogIndex <= 0 {
					args.PrevLogTerm = NONE
				} else {
					args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
				}

				// log entries to store
				// (empty for heartbeat; may send more than one for efficiency)
				args.Entries = rf.getEntriesToSend(i)

				go func(server int, args AppendEntriesArgs, rf *Raft) {

					reply := AppendEntriesReply{}

					if rf.currentState != StateLeader {
						DPrintf("%d [term: %d] Leader state has changed, stop to send heartbeat package.",
							rf.me, rf.currentTerm)
						return
					}

					currentIdx := len(rf.log) - 1

					//retry:

					DPrintf("%d [term: %d] Send a AppendEntriesReq to %d, term is %d, PrevLogIndex is %d, PrevLogTerm is %d, Entries' length is %d, commitIndex is %d.",
						rf.me, rf.currentTerm, server, args.Term, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), args.LeaderCommit)
					ok := rf.sendAppendEntries(server, &args, &reply)

					if ok {
						DPrintf("%d [term: %d] Receive AppendEntriesReply from %d, success is %v, term is %d",
							rf.me, rf.currentTerm, server, reply.Success, reply.Term)
						rf.handleAppendEntriesReply(reply.Success, reply.Term, server, currentIdx)
					} else {
						DPrintf("%d [term: %d] Send AppendEntries failed, target is %d, try again.", rf.me, rf.currentTerm, server)
						//goto retry
					}
				}(i, args, rf)
			}

		}

		// sleep for an interval
		time.Sleep(heartBeatInterval)
	}
}

func (rf *Raft) handleElectionTimeout() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	/**
	 * timeout, do something according to the current state.
	 */
	DPrintf("%d [term: %d] ElectionTimeout from %s", rf.me, rf.currentTerm, rf.currentState)

	if rf.currentState == StateLeader {
		// Leader, which is actually impossible
		DPrintf("%d [term: %d] An unexpected exception to timeout in leader state.", rf.me, rf.currentTerm)
	} else {
		if rf.currentState == StateFollower {
			// Follower
			rf.convertToCandidate()
		} else if rf.currentState == StateCandidate {
			// Candidate
			rf.startElection()
		} else {
			// Error
			DPrintf("%d [term: %d] Unexpected Raft state.", rf.me, rf.currentTerm)
			return
		}
	}
}

/**
 * On conversion to candidate, start election:
 * Increment currentTerm
 * Vote for self
 * Reset election timer
 * Send RequestVote RPCs to all other servers
 */
func (rf *Raft) convertToCandidate() {
	DPrintf("%d [term: %d] Convert to StateCandidate", rf.me, rf.currentTerm)

	rf.currentState = StateCandidate

	// persist
	rf.persist()

	rf.startElection()
}

func (rf *Raft) convertToFollower(newTerm int) {
	DPrintf("%d [term: %d] Convert to StateFollower", rf.me, rf.currentTerm)

	rf.currentState = StateFollower
	rf.currentTerm = newTerm
	rf.votedFor = NONE

	// persist
	rf.persist()
}

func (rf *Raft) convertToLeader() {
	DPrintf("%d [term: %d] Convert to StateLeader", rf.me, rf.currentTerm)

	rf.currentState = StateLeader
	rf.stopTimer()

	// reinitialize the volatile state on leaders.
	// nextIndex
	lastIndex, _ := rf.getLastLogInfo()
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		// initialized to leader last log index + 1
		rf.nextIndex[i] = lastIndex + 1
	}

	// matchIndex
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		// initialized to 0
		rf.matchIndex[i] = INIT
	}

	// persist
	rf.persist()

	go rf.sendHeartBeats()
}

func (rf *Raft) startElection() {
	// reset timeout
	rf.rstTimer()

	rf.currentTerm++

	DPrintf("%d [term: %d] Start an election", rf.me, rf.currentTerm)

	// self vote
	rf.votedFor, rf.voteCount = rf.me, 1

	// persist
	rf.persist()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: len(rf.log) - 1,
		}

		if args.LastLogIndex > INIT {
			args.LastLogTerm = rf.log[args.LastLogIndex].Term
		} else {
			args.LastLogTerm, args.LastLogIndex = NONE, INIT
		}

		go func(server int, args RequestVoteArgs) {
			DPrintf("%d [term: %d] Send a RequestVoteRequest to %d, term is %d, LastTermIndex is %d, LastLogTerm is %d.",
				rf.me, rf.currentTerm, server, args.Term, args.LastLogIndex, args.LastLogTerm)

			reply := RequestVoteReply{}

			ok := rf.sendRequestVote(server, &args, &reply)
			if ok {
				DPrintf("%d [term: %d] Receive RequestVoteReply from %d, voteGrant is %v, term is %d",
					rf.me, rf.currentTerm, server, reply.VoteGranted, reply.Term)
				rf.handleRequestVoteResult(reply.Term, reply.VoteGranted)
			} else {
				DPrintf("%d [term: %d] Send RequestVote failed, target is %d.", rf.me, rf.currentTerm, server)
			}
		}(i, args)
	}
}

func (rf *Raft) isElectionSuccess() bool {
	// if the election number exceed half of the peer number.
	return int(rf.voteCount) >= (len(rf.peers)/2 + 1)
}

func (rf *Raft) getLastLogInfo() (int, int) {
	var lastIndex, lastTerm int
	if len(rf.log) > 1 {
		lastIndex = len(rf.log) - 1
	} else {
		lastIndex = INIT
	}

	if lastIndex > INIT {
		lastTerm = rf.log[lastIndex].Term
	} else {
		lastTerm = NONE
	}
	return lastIndex, lastTerm
}

func (rf *Raft) getEntriesToSend(server int) []LogEntry {
	logEntries := make([]LogEntry, 0)
	nextIndex := rf.nextIndex[server]
	for nextIndex < len(rf.log) {
		logEntries = append(logEntries, rf.log[nextIndex])
		nextIndex++
	}
	return logEntries
}

func (rf *Raft) checkCommitIndex(server int) {
	currentIdx := rf.matchIndex[server]
	commitCount := 1
	for i := range rf.peers {
		if i == rf.me {
			continue
		} else {
			if currentIdx <= rf.matchIndex[i] {
				commitCount++
			}
		}
	}

	// 1. N > commitIndex
	// 2. exceed half number
	// 3. and log[N].term == currentTerm
	DPrintf("commitIndex:%d currentIdx:%d", rf.commitIndex, currentIdx)
	if rf.commitIndex < currentIdx && (commitCount >= len(rf.peers)/2+1) &&
		(rf.log[currentIdx].Term == rf.currentTerm) {
		DPrintf("%d [term: %d] log entry %d is committed to above half number of server, apply it.",
			rf.me, rf.currentTerm, currentIdx)
		// update commit index
		rf.commitIndex = currentIdx
		// leader apply
		go rf.applyLogs()
	}
}

func (rf *Raft) applyLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.commitIndex >= len(rf.log) {
		return
	}

	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		DPrintf("%d [term: %d] Apply log into state machine, index is %d.",
			rf.me, rf.currentTerm, rf.lastApplied)

		entry := rf.log[rf.lastApplied]
		// commit
		msg := ApplyMsg{
			CommandIndex: rf.lastApplied,
			Command:      entry.Command,
			CommandValid: true,
		}
		rf.applyCh <- msg
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}

	rf.applyCh = applyCh

	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentState = StateFollower

	// initialize persist state
	// insert one entry to perch index 0
	rf.votedFor = NONE
	rf.currentTerm = 0
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{})

	// initialize from state persisted before a crash,
	// if there is no persist before, it do nothing.
	rf.readPersist(persister.ReadRaftState())
	// persist
	rf.persist()

	rf.commitIndex = INIT
	rf.lastApplied = INIT

	rf.rstTimer()

	return rf
}
