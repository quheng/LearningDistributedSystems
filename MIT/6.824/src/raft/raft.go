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
	"labrpc"
	"math/rand"
	"sync"
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
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// customer state
	state              int                    // server state, can be LEADER, FOLLOWER or CANDIDATE
	gotEntriesChan     chan AppendEntriesArgs // got entries
	checkEntriesChan   chan bool              // take or reject this Entries
	gotRequestVoteChan chan RequestVoteArgs   // got request vote
	grantVoteChan      chan bool              // grant vote or not

	// persistent state on all servers
	currentTerm int           // latest term server has seen( initialized to 0 on first boot, increases monotonically)
	votedFor    int           // candidateId that received vode in current term(or null if none)
	log         []interface{} // log entries; each entry contains command for state machine, and term when entry was received by leader

	// volatile state on all servers
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, incraeses monotonically)

	// volatile state on leaders (reinitialized after election)
	nextIndex  []int // for each server, index of the next log entry to send to that server(initialized to leader last log index + 1)
	matchIndex []int //	for each server, index of highest log entry know to be replicated on server (initialized to 0, incraeses monotonically)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LEADER
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

	rf.mu.Lock()
	defer rf.mu.Unlock()
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

	rf.mu.Lock()
	defer rf.mu.Unlock()
	decoder.Decode(&rf.currentTerm)
	decoder.Decode(&rf.votedFor)
	decoder.Decode(&rf.log)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		// rodo
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateID  int // candidate requesting vote
	LastLogIndex int // index of candidate;s last log entry
	LastLogTerm  int // term of candidate;s lost log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	rf.gotRequestVoteChan <- *args
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
	reply.VoteGranted = <-rf.grantVoteChan
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
func (rf *Raft) sendRequestVote(server *labrpc.ClientEnd, args *RequestVoteArgs, reply *RequestVoteReply, ch chan RequestVoteReply) bool {
	ok := server.Call("Raft.RequestVote", args, reply)
	if ok {
		ch <- *reply
	}
	return ok
}

// AppendEntries

// AppendEntriesArgs field names must start with capital letters!
//
type AppendEntriesArgs struct {
	Term         int           // leader's term
	LeaderID     int           // so follower can redirect clients
	PrevLogIndex int           // index of log entry immediately preceding new ones
	PreLogTerm   int           // term of prevLogIndex entry
	Entries      []interface{} // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int           // leader's commitIndex
}

// AppendEntriesReply field names must start with capital letters!
//
type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int  // currentTerm, for candidate to update itself
	Success bool // if follower contained entry matching prevLogIndex and prevLogTerm
}

// AppendEntries got entries, send request to main goroutine and wait for it reply
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	rf.gotEntriesChan <- *args
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
	reply.Success = <-rf.checkEntriesChan
	return
}

func (rf *Raft) sendAppendEntries(server *labrpc.ClientEnd, args *AppendEntriesArgs, reply *AppendEntriesReply, ch chan AppendEntriesReply) bool {
	ok := server.Call("Raft.AppendEntries", args, reply)
	if ok {
		ch <- *reply
	}
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
	// todo
	return rf.commitIndex, rf.currentTerm, rf.state == LEADER
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	// todo
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
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = FOLLOWER
	rf.gotEntriesChan = make(chan AppendEntriesArgs)
	rf.checkEntriesChan = make(chan bool)

	rf.gotRequestVoteChan = make(chan RequestVoteArgs)
	rf.grantVoteChan = make(chan bool)

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

	go func(rf *Raft) {
		for {
			// todo
			// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)
			switch rf.state {
			case FOLLOWER:
				rf.followerStuff()
			case CANDIDATE:
				rf.candidateStuff()
			case LEADER:
				rf.leaderStuff()
			}
		}
	}(rf)
	return rf
}

func getElectionTimeout() <-chan time.Time {
	randTime := rand.Intn(MAX_ELECTION_TIME_OUT-MIN_ELECTION_TIME_OUT) + MIN_ELECTION_TIME_OUT
	return time.After(time.Duration(randTime) * time.Millisecond)
}

func (rf *Raft) sendEntriesToServers() <-chan AppendEntriesReply {
	replayChan := make(chan AppendEntriesReply)
	rf.mu.Lock()
	appendRntriesArgs := AppendEntriesArgs{
		rf.currentTerm,
		rf.me,
		1,              // PrevLogIndex todo
		rf.currentTerm, // PreLogTerm todo
		nil,            // Entries todo
		rf.commitIndex}
	rf.mu.Unlock()
	for _, server := range rf.peers {
		appendRntriesReply := new(AppendEntriesReply)
		go rf.sendAppendEntries(server, &appendRntriesArgs, appendRntriesReply, replayChan)
	}
	return replayChan
}

func (rf *Raft) sendRequestVoteToServers() <-chan RequestVoteReply {
	rf.mu.Lock()
	requestVoteArgs := RequestVoteArgs{
		rf.currentTerm,
		rf.me,
		rf.commitIndex, // todo
		rf.lastApplied} // todo
	rf.mu.Unlock()
	replayChan := make(chan RequestVoteReply)
	for _, server := range rf.peers {
		requestVoteReply := new(RequestVoteReply)
		go rf.sendRequestVote(server, &requestVoteArgs, requestVoteReply, replayChan)
	}
	return replayChan
}

// follower state
func (rf *Raft) followerStuff() {
	electionTimeout := getElectionTimeout()
	select {
	// get entries from leader
	case appendEntriesArgs := <-rf.gotEntriesChan:
		{
			// 1. Reply false if term < currentTerm (§5.1)
			// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
			// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
			// 4. Append any new entries not already in the log
			// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)

			// 1
			if appendEntriesArgs.Term < rf.currentTerm {
				rf.checkEntriesChan <- false
				return
			}
			// todo 2345
			rf.mu.Lock()
			rf.currentTerm = appendEntriesArgs.Term
			rf.checkEntriesChan <- true
			rf.mu.Unlock()
		}
	// got an vote request from candidate
	case requestVoteArgs := <-rf.gotRequestVoteChan:
		{
			// 1. Reply false if term < currentTerm (§5.1)
			// 2.
			//  a.If votedFor is null or candidateId
			//  b. candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)

			// 1
			if requestVoteArgs.Term < rf.currentTerm {
				rf.grantVoteChan <- false
				return
			}

			// 2 todo 2b
			rf.mu.Lock()
			if rf.votedFor == -1 || rf.votedFor == requestVoteArgs.CandidateID {
				rf.grantVoteChan <- true
			} else {
				rf.grantVoteChan <- false
			}
			rf.mu.Unlock()
		}
	// If election timeout elapses without receiving AppendEntries
	// RPC from current leader or granting vote to candidate: convert to candidate
	case <-electionTimeout:
		{
			rf.mu.Lock()
			rf.state = LEADER
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) leaderStuff() {
	for {
		heartbeatTimeout := time.After(HEART_BEETS_INTERVAL)
		replyChan := rf.sendEntriesToServers()
		select {
		case <-replyChan:
			{
				// todo
			}
		case <-rf.gotEntriesChan:
			{
				// todo
			}
		case <-rf.gotRequestVoteChan:
			{
				// todo
			}
		case <-heartbeatTimeout:
			{

			}
		}
	}
}

// A candidate continues in this state until one of three things happens:
// (a) it wins the election,
// (b) another server establishes itself as leader, or
// (c) a period of time goes by with no winner.
func (rf *Raft) candidateStuff() {
	//On conversion to candidate, start election:
	//1. Increment currentTerm
	//2. Vote for self
	//3. Reset election timer
	//4. Send RequestVote RPCs to all other servers
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	electionTimeout := getElectionTimeout()
	rf.mu.Unlock()
	replayChan := rf.sendRequestVoteToServers()
	gotVotes := 0
LOOP:
	for {
		select {
		// (a)
		// wins an election if it receives votes from a majority of the servers in the full cluster for the same term.
		case <-replayChan:
			{
				gotVotes++
				if gotVotes > len(rf.peers)/2 {
					rf.mu.Lock()
					rf.state = LEADER
					rf.mu.Unlock()
					break LOOP
				}
			}
		// (b)
		// If the leader’s term (included in its RPC)
		// is at least as large as the candidate’s current term,
		// then the candidate recognizes the leader as legitimate and
		// returns to follower state. If the term in the RPC is smaller than the candidate’s current term,
		// then the candidate rejects the RPC and con- tinues in candidate state.
		case entries := <-rf.gotEntriesChan:
			{
				rf.mu.Lock()
				if entries.Term >= rf.currentTerm {
					rf.state = FOLLOWER
					rf.currentTerm = entries.Term
					rf.checkEntriesChan <- true // todo
				}
				rf.mu.Unlock()
				break LOOP
			}
		case <-rf.gotRequestVoteChan:
			{
				rf.grantVoteChan <- false
			}
		// (c)
		// start a new election by incrementing its term and initiating another round of Request- Vote RPCs.
		case <-electionTimeout:
			{
				break LOOP
			}
		}
	}
}
