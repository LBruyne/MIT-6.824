package raft

import (
	"log"
	"math/rand"
	"time"
)

// StateType raft server state
type StateType string

const INIT, NONE = 0, -1

const (
	StateLeader    = StateType("StateLeader")
	StateFollower  = StateType("StateFollower")
	StateCandidate = StateType("StateCandidate")
)

const (
	/**
	 * election timeout:
	 * if a follower didn't receive a heartbeat in this time dutation,
	 * it will change to candidate and start a new election.
	 */
	electionTimeoutFloor = 150
	electionTimeoutCeil  = 300

	/**
	 * heartbeat interval:
	 * the interval that a server send out heartbeart package.
	 * heartBeatInterval << electionTimeout
	 */
	heartBeatInterval = time.Millisecond * 50
)

// Debugging
const Debug int = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug != 0 {
		log.Printf(format, a...)
	}
	return
}

func rstRandElectionTimeout() time.Duration {
	return time.Millisecond * time.Duration(rand.Intn(electionTimeoutCeil-electionTimeoutFloor)+electionTimeoutFloor)
}

func (rf *Raft) rstTimer() {
	if rf.timer != nil {
		rf.timer.Stop()
	}
	rf.timer = time.AfterFunc(rstRandElectionTimeout(), rf.handleElectionTimeout)
}

func (rf *Raft) stopTimer() {
	rf.timer.Stop()
}
