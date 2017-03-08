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
	"encoding/gob"
	"fmt"
	"labrpc"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// server state
const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

const HEART_BEETS_INTERVAL = 100 * time.Millisecond
const MIN_ELECTION_TIME_OUT = 300
const MAX_ELECTION_TIME_OUT = 450

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int32               // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// customer state
	state             int32      // server state, can be LEADER, FOLLOWER or CANDIDATE
	heartbeatsChan    chan int64 // get heartbeats from leader, save term
	receivedVotes     int32      // in candidate state, the number of votes received in current term
	gotMajorVotesChan chan int32 // get major votes in current term

	// persistent state on all servers
	currentTerm int64         // latest term server has seen( initialized to 0 on first boot, increases monotonically)
	votedFor    int32         // candidateId that received vode in current term(or null if none)
	log         []interface{} // log entries; each entry contains command for state machine, and term when entry was received by leader

	// volatile state on all servers
	commitIndex int64 // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int64 // index of highest log entry applied to state machine (initialized to 0, incraeses monotonically)

	// volatile state on leaders (reinitialized after election)
	nextIndex  []int // for each server, index of the next log entry to send to that server(initialized to leader last log index + 1)
	matchIndex []int //	for each server, index of highest log entry know to be replicated on server (initialized to 0, incraeses monotonically)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return int(rf.currentTerm), rf.state == LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	writer := new(bytes.Buffer)
	encoder := gob.NewEncoder(writer)
	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.votedFor)
	encoder.Encode(rf.log)
	data := writer.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	reader := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(reader)
	decoder.Decode(&rf.currentTerm)
	decoder.Decode(&rf.votedFor)
	decoder.Decode(&rf.log)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int64 // candidate's term
	CandidateID  int32 // candidate requesting vote
	LastLogIndex int64 // index of candidate;s last log entry
	LastLogTerm  int64 // term of candidate;s lost log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int64 // currentTerm, for candidate to update itself
	VoteGranted bool  // true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	// todo add log
	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		reply.VoteGranted = true
		rf.mu.Lock()
		rf.votedFor = args.CandidateID
		rf.mu.Unlock()
		return
	}
	reply.VoteGranted = false
	rf.mu.Unlock()
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	rf.mu.Lock()
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	if !ok { // get some errors during rpc
		return false
	}
	if rf.currentTerm != args.Term { // rf.currentTerm != args.Term means stale message
		return false
	}
	if reply.VoteGranted != true { // reject votes
		return false
	}
	rf.receivedVotes++
	if rf.receivedVotes > int32(len(rf.peers)/2) { // got major votes
		if len(rf.gotMajorVotesChan) == 0 { // rf.gotMajorVotesChan != 0 means that server has know get major votes
			rf.gotMajorVotesChan <- 1
		}
	}
	rf.mu.Unlock()
	return ok
}

// AppendEntries

// AppendEntriesArgs field names must start with capital letters!
//
type AppendEntriesArgs struct {
	Term         int64         // leader's term
	LeaderID     int32         // so follower can redirect clients
	PrevLogIndex int64         // index of log entry immediately preceding new ones
	PreLogTerm   int64         // term of prevLogIndex entry
	Entries      []interface{} // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int64         // leader's commitIndex
}

// AppendEntriesReply field names must start with capital letters!
//
type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int64 // currentTerm, for candidate to update itself
	Success bool  // if follower contained entry matching prevLogIndex and prevLogTerm
}

// AppendEntries must follow:
// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex
// whose term matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index
// but different terms), delete the existing entry and all that
// follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex =
// min(leaderCommit, index of last new entry)
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	if rf.currentTerm < args.PreLogTerm {
		reply.Success = false
		return
	}
	// 2, 3, 4 todo

	// if rf.currentTerm == args.PreLogTerm && rf.log[args.PrevLogIndex] == nil {
	// 	reply.Success = false
	//	return
	//}

	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > args.PrevLogIndex {
			rf.commitIndex = args.PrevLogIndex
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	}
	reply.Success = true
	rf.heartbeatsChan <- args.Term
	rf.mu.Unlock()
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	return int(rf.commitIndex), int(rf.currentTerm), rf.state == LEADER
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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
	rf.peers = peers
	rf.persister = persister
	rf.me = int32(me)

	// Your initialization code here (2A, 2B, 2C).
	rf.state = FOLLOWER
	rf.heartbeatsChan = make(chan int64)
	rf.receivedVotes = 0
	rf.gotMajorVotesChan = make(chan int32)

	rf.currentTerm = 0
	rf.votedFor = -1
	// rf.log = make([]) todo

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, 0)
	rf.matchIndex = make([]int, 0)

	rf.state = FOLLOWER

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func() {
		for {
			fmt.Println(rf.me, rf.state, rf.currentTerm)

			switch rf.state {
			case FOLLOWER:
				rf.followerStuff()
			case CANDIDATE:
				rf.candidateStuff()
			case LEADER:
				rf.leaderStuff()
			}
		}
	}()
	return rf
}

func (rf *Raft) turnToNextTerm() {
	atomic.AddInt64(&rf.currentTerm, 1)
	atomic.AddInt32(&rf.votedFor, -1)
}

func getElectionTimeout() time.Duration {
	randTime := rand.Intn(MAX_ELECTION_TIME_OUT-MIN_ELECTION_TIME_OUT) + MIN_ELECTION_TIME_OUT
	return time.Duration(randTime) * time.Millisecond
}

// follower receive message from leader, there are two kinds of situation
// (a) received message from master
// (b) receives no communication over a period of time called the election timeout
func (rf *Raft) followerStuff() {
	electionTimeout := getElectionTimeout()
	select {
	// (a)
	// follow the leader
	case <-rf.heartbeatsChan:
		{
			// todo received messages from leader
		}
	// (b)
	// a follower increments its current term and transitions to candidate state.
	case <-time.After(electionTimeout):
		{
			rf.turnToNextTerm()
			atomic.StoreInt32(&rf.state, CANDIDATE)
			// send Request- Vote RPCs in candidateStuff
		}
	}
}

func (rf *Raft) leaderStuff() {
	time.Sleep(HEART_BEETS_INTERVAL)
	rf.mu.Lock()
	appendRntriesArgs := AppendEntriesArgs{
		rf.currentTerm,
		rf.me,
		1,              // PrevLogIndex todo
		rf.currentTerm, // PreLogTerm todo
		nil,            // Entries todo
		rf.commitIndex}
	appendRntriesReply := new(AppendEntriesReply)
	for index := range rf.peers {
		go rf.sendAppendEntries(index, &appendRntriesArgs, appendRntriesReply)
	}
	rf.mu.Unlock()
}

// A candidate continues in this state until one of three things happens:
// (a) it wins the election,
// (b) another server establishes itself as leader, or
// (c) a period of time goes by with no winner.
func (rf *Raft) candidateStuff() {
	electionTimeout := getElectionTimeout()
	rf.mu.Lock()
	requestVoteArgs := RequestVoteArgs{
		rf.currentTerm,
		rf.me,
		rf.commitIndex, // todo
		rf.lastApplied} // todo
	requestVoteReply := new(RequestVoteReply)
	rf.receivedVotes = 0 // clear
	rf.votedFor = rf.me
	for index := range rf.peers { // included voting for itself
		go rf.sendRequestVote(index, &requestVoteArgs, requestVoteReply)
	}
	rf.mu.Unlock()
	select {
	// (a)
	// wins an election if it receives votes from a majority of the servers in the full cluster for the same term.
	case <-rf.gotMajorVotesChan:
		{
			// do not increse the term
			atomic.StoreInt32(&rf.state, LEADER)
		}
	// (b)
	// If the leader’s term (included in its RPC)
	// is at least as large as the candidate’s current term,
	// then the candidate recognizes the leader as legitimate and
	// returns to follower state. If the term in the RPC is smaller than the candidate’s current term,
	// then the candidate rejects the RPC and con- tinues in candidate state.
	case leaderTerm := <-rf.heartbeatsChan:
		{
			rf.mu.Lock()
			if leaderTerm >= rf.currentTerm {
				rf.state = FOLLOWER
			}
			rf.mu.Unlock()
		}
	// (c)
	// start a new election by incrementing its term and initiating another round of Request- Vote RPCs.
	case <-time.After(electionTimeout):
		{
			rf.turnToNextTerm()
		}
	}
}
