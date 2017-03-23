package raft

// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isLeader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.

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
	FOLLOWER  = "FOLLOWER"
	CANDIDATE = "CANDIDATE"
	LEADER    = "LEADER"
)

const heartBeetsInterval = 120 * time.Millisecond
const minElectionTimeOut = 750
const maxElectionTimeOut = 900

// ApplyMsg as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// Log contains command for state machine, and term when entry was received by leader
type Log struct {
	Term    int         // when it was created
	Command interface{} // command
}

// Raft is a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// customer state
	state              string        // server state, can be LEADER, FOLLOWER or CANDIDATE
	gotEntriesChan     chan bool     // got entries, send whether accept these entries
	gotRequestVoteChan chan bool     // got request vote, send whether accept the vote
	applyMsgChan       chan ApplyMsg // send an ApplyMsg to the service (or tester)

	// persistent state on all servers
	currentTerm int   // latest term server has seen( initialized to 0 on first boot, increases monotonically)
	votedFor    int   // candidateId that received vote in current term(or null if none)
	log         []Log // log entries; each entry contains command for state machine, and term when entry was received by leader

	// volatile state on all servers
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// volatile state on leaders (reinitialized after election)
	nextIndex  []int // for each server, index of the next log entry to send to that server(initialized to leader last log index + 1)
	matchIndex []int //	for each server, index of highest log entry know to be replicated on server (initialized to 0, increases monotonically)
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LEADER
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	writer := new(bytes.Buffer)
	encoder := gob.NewEncoder(writer)
	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.votedFor)
	encoder.Encode(rf.log)
	data := writer.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	reader := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(reader)

	decoder.Decode(&rf.currentTerm)
	decoder.Decode(&rf.votedFor)
	decoder.Decode(&rf.log)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		// todo
		return
	}
}

// RequestVoteArgs RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateID  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's lost log entry
}

// RequestVoteReply example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// 1. Reply false if term < currentTerm (§5.1)
	// 2.
	//  a. If votedFor is null or candidateId
	//  b. candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.4)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 1
	if args.Term < rf.currentTerm {
		DPrintf("%v reject RequestVote because stale in term %v\n", rf.me, rf.currentTerm)
		rf.gotRequestVoteChan <- false
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		DPrintf("%v %v reset term %v to %v\n", rf.state, rf.me, rf.currentTerm, args.Term)
		rf.resetState(args.Term)
	}

	// 2 a
	if rf.votedFor != -1 && rf.votedFor != args.CandidateID {
		DPrintf("%v reject because voted for %v\n", rf.me, rf.votedFor)
		rf.gotRequestVoteChan <- false
		reply.VoteGranted = false
		return
	}

	// 2 b
	if rf.commitIndex > 0 {
		if rf.log[rf.commitIndex-1].Term > args.LastLogTerm {
			DPrintf("%v reject because stale log term for %v\n", rf.me, rf.votedFor)
			rf.gotRequestVoteChan <- false
			reply.VoteGranted = false
			return
		}

		if rf.log[rf.commitIndex-1].Term == args.LastLogTerm && rf.commitIndex > args.LastLogIndex {
			DPrintf("%v reject because stale log index for %v\n", rf.me, rf.votedFor)
			rf.gotRequestVoteChan <- false
			reply.VoteGranted = false
			return
		}
	}
	rf.votedFor = args.CandidateID
	DPrintf("%v accept votes for %v in term %v\n", rf.me, rf.votedFor, rf.currentTerm)
	rf.gotRequestVoteChan <- true
	reply.VoteGranted = true
	reply.Term = rf.currentTerm
	return
}

// sendRequestVote RPC callee
// just send reply to main goroutine and handle it
func (rf *Raft) sendRequestVote(server *labrpc.ClientEnd, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return server.Call("Raft.RequestVote", args, reply)
}

func (rf *Raft) sendRequestVoteToServers() <-chan RequestVoteReply {
	rf.mu.Lock()
	lastLogTerm := -1
	if rf.commitIndex > 0 {
		lastLogTerm = rf.log[rf.commitIndex-1].Term
	}
	requestVoteArgs := RequestVoteArgs{
		rf.currentTerm,
		rf.me,
		rf.commitIndex,
		lastLogTerm}
	DPrintf("%v sendRequestVote in Term %v", rf.me, rf.currentTerm)
	rf.mu.Unlock()
	replayChan := make(chan RequestVoteReply)
	for index, server := range rf.peers {
		if index != rf.me {
			server := server
			go func() {
				requestVoteReply := new(RequestVoteReply)
				if rf.sendRequestVote(server, &requestVoteArgs, requestVoteReply) {
					replayChan <- *requestVoteReply
				}
			}()
		}
	}
	return replayChan
}

// AppendEntriesArgs field names must start with capital letters!
type AppendEntriesArgs struct {
	Term         int   // leader's term
	LeaderID     int   // so follower can redirect clients
	PrevLogIndex int   // index of log entry immediately preceding new ones
	PreLogTerm   int   // term of prevLogIndex entry
	Entries      []Log // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int   // leader's commitIndex
}

// AppendEntriesReply field names must start with capital letters!
type AppendEntriesReply struct {
	Term    int  // currentTerm, for candidate to update itself
	Success bool // if follower contained entry matching prevLogIndex and prevLogTerm
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	DPrintf("%v %v receive entries %v in term %v\n", rf.state, rf.me, args, rf.currentTerm)

	// if discovers that its term is out of date, it immediately reverts to follower state.
	if args.Term > rf.currentTerm {
		rf.resetState(args.Term)
	}

	if rf.state == CANDIDATE {
		if args.Term == rf.currentTerm {
			rf.resetState(args.Term)
		}
	}

	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		DPrintf("%v reject entries because of stale term\n", rf.me)
		rf.gotEntriesChan <- false
		reply.Success = false
		return
	}

	// 2 Reply false if log does not contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if rf.currentTerm == args.Term && args.PrevLogIndex > len(rf.log) {
		DPrintf("%v reject entries because of stale index, %v %v\n", rf.me, args.PrevLogIndex, len(rf.log))
		rf.gotEntriesChan <- false
		reply.Success = false
		return
	}
	// ???
	// if args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex-1].Term != args.PreLogTerm {
	// 	DPrintf("%v reject entries because of term does not match \n", rf.me)
	// 	rf.gotEntriesChan <- false
	// 	reply.Success = false
	// 	return
	// }

	// 3 If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
	// todo just overrive the term does not match, see the reason at Q&A

	// 4. Append any new entries not already in the log
	if args.Entries != nil {
		rf.log = append(rf.log, args.Entries...)
		DPrintf("%v append entries %v in term %v, log %v", rf.me, args.Entries, rf.currentTerm, rf.log)
	}
	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	lastEntryIndex := len(rf.log)
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > lastEntryIndex {
			rf.commitIndex = lastEntryIndex
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	}

	// all server: if commitIndex > lastAppliedId: increment lastAppliedId, apply log[lastApplied] to state machine
	if rf.commitIndex > rf.lastApplied {
		for i := rf.lastApplied; i < rf.commitIndex; i++ {
			command := rf.log[i].Command
			applyMsg := ApplyMsg{i + 1, command, false, nil} // todo
			DPrintf("follower %v applied %v in term %v", rf.me, applyMsg, rf.currentTerm)
			rf.applyMsgChan <- applyMsg
		}
		rf.lastApplied = rf.commitIndex
	}
	reply.Success = true
	rf.gotEntriesChan <- true
	DPrintf("%v accept entries %v in term %v\n", rf.me, args, rf.currentTerm)
	return
}

// sendAppendEntries callee
func (rf *Raft) sendAppendEntries(server *labrpc.ClientEnd, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return server.Call("Raft.AppendEntries", args, reply)
}

// callee add lock
func (rf *Raft) setAppendEntriesArgs(server int) AppendEntriesArgs {
	prevLogIndex := rf.nextIndex[server] - 1
	preLogTerm := -1
	logEntries := rf.log[prevLogIndex:] // left-open-right-close, index = real index in logs + 1, get log after prevLogIndex
	if prevLogIndex > 0 {
		preLogTerm = rf.log[prevLogIndex-1].Term
	}

	return AppendEntriesArgs{
		rf.currentTerm, // leader's term
		rf.me,          // leader's id
		prevLogIndex,   // index of log entry immediately preceding new ones
		preLogTerm,     // term of prevLogIndex entry
		logEntries,     // log entries to store (empty for heartbeat; may send more than one for efficiency)
		rf.commitIndex} // leader’s commitIndex
}

func (rf *Raft) makeAgreement(command interface{}) {
	DPrintf("%v send entries to all servers in term %v", rf.me, rf.currentTerm)
	replyChan := make(chan int)
	for index, server := range rf.peers {
		if index != rf.me {
			index := index
			server := server
			go func() {
				rf.mu.Lock()
				state := rf.state
				rf.mu.Unlock()
				for state == LEADER {
					reply := new(AppendEntriesReply)
					reply.Success = false
					rf.mu.Lock()
					appendEntriesArgs := rf.setAppendEntriesArgs(index)
					rf.mu.Unlock()
					ok := rf.sendAppendEntries(server, &appendEntriesArgs, reply)
					if !ok {
						return
					}
					if reply.Success {
						rf.mu.Lock()
						rf.nextIndex[index] = len(rf.log) + 1
						rf.mu.Unlock()
						replyChan <- 1
						return
					}
					rf.mu.Lock()
					rf.nextIndex[index]--
					rf.mu.Unlock()
					return
				}
			}()
		}
	}
	go func() {
		committedAmount := 0
		for {
			if command == nil {
				return // nothing need apply to state machine
			}
			<-replyChan
			committedAmount++
			if committedAmount > len(rf.peers)/2 {
				rf.mu.Lock()
				rf.commitIndex++
				applyMsg := ApplyMsg{rf.commitIndex, command, false, nil} // todo
				rf.lastApplied = rf.commitIndex
				rf.mu.Unlock()
				DPrintf("leader %v applied %v in term %v", rf.me, applyMsg, rf.currentTerm)
				rf.applyMsgChan <- applyMsg
				return
			}
		}
	}()
}

// Start at the leader starts the process of adding a new operation to the log;
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
	rf.mu.Lock()
	isLeader := rf.state == LEADER
	defer rf.mu.Unlock()
	if isLeader {
		DPrintf("leader %v new log %v", rf.me, command)
		// step 1, leader appends the command to its logs as a new entry
		rf.log = append(rf.log, Log{rf.currentTerm, command})
		rf.persist()
		// step 2, issues AppendEntries RPCs in parallel to each of the other servers to replicate the entry.
		go rf.makeAgreement(command)
		return len(rf.log), rf.currentTerm, isLeader
	}
	return -1, -1, isLeader
}

// Kill when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
	// todo
}

// Make will create a Raft server. the ports
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
	rf.gotEntriesChan = make(chan bool)
	rf.gotRequestVoteChan = make(chan bool)
	rf.applyMsgChan = applyCh

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]Log, 0)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers)) // initialize in leader stuff
	rf.matchIndex = make([]int, 0)         // todo

	rf.state = FOLLOWER

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func(rf *Raft) {
		for {
			rf.mu.Lock()
			DPrintf("%v is %v", rf.me, rf.state)
			state := rf.state
			rf.mu.Unlock()
			switch state {
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
	randTime := rand.Intn(maxElectionTimeOut-minElectionTimeOut) + minElectionTimeOut
	return time.After(time.Duration(randTime) * time.Millisecond)
}

// incoming RequestVote RPC has a higher term that you,
// you should first step down and adopt their term (thereby resetting votedFor), and then handle the RPC
// note: use lock in callee
func (rf *Raft) resetState(term int) {
	rf.state = FOLLOWER
	rf.currentTerm = term
	rf.votedFor = -1
}

// follower state
func (rf *Raft) followerStuff() {
	timeOut := getElectionTimeout() // note: if the term in the AppendEntries arguments is outdated, do not reset timer
FOLLOWER_LOOP:
	for {
		select {
		// get entries from leader
		// only update election timeout when getting an illegal request
		case result := <-rf.gotEntriesChan:
			{
				if result {
					timeOut = getElectionTimeout()
				}
			}
		// got an vote request from candidate
		case result := <-rf.gotRequestVoteChan:
			{
				if result {
					DPrintf("%v refresh timeout", rf.me)
					timeOut = getElectionTimeout()
				}
			}
		// If election timeout elapses without receiving AppendEntries
		// RPC from current leader or granting vote to candidate: convert to candidate
		case <-timeOut: // received requests will refresh time
			{
				DPrintf("%v getElectionTimeout \n", rf.me)
				rf.mu.Lock()
				rf.state = CANDIDATE
				rf.mu.Unlock()
				break FOLLOWER_LOOP
			}
		}
	}
}

func (rf *Raft) leaderStuff() {
	heartbeat := time.Tick(heartBeetsInterval)
	rf.mu.Lock()
	state := rf.state
	for index := range rf.peers {
		rf.nextIndex[index] = rf.commitIndex + 1
	}
	rf.mu.Unlock()
LEADER_LOOP:
	for state == LEADER {
		select {
		case result := <-rf.gotEntriesChan:
			{
				if result {
					break LEADER_LOOP
				}
			}
		case result := <-rf.gotRequestVoteChan:
			{
				if result {
					break LEADER_LOOP
				}
			}
		case <-heartbeat:
			{
				rf.mu.Lock()
				rf.makeAgreement(nil)
				rf.mu.Unlock()
			}
		}
		rf.mu.Lock()
		state = rf.state
		rf.mu.Unlock()
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
	state := rf.state
	rf.mu.Unlock()
	electionTimeout := getElectionTimeout()
	replayChan := rf.sendRequestVoteToServers()
	gotVotes := 1 // initial to it self
CANDIDATE_LOOP:
	for state == CANDIDATE {
		select {
		// (a) wins an election
		// receives votes from a majority of the servers in the full cluster for the same term.
		case replay := <-replayChan:
			{
				isAchieved := func() bool {
					DPrintf("%v receive votes %v", rf.me, replay)
					rf.mu.Lock() // notice defer is function scope
					defer rf.mu.Unlock()

					if replay.Term > rf.currentTerm {
						rf.currentTerm = replay.Term
						return false
					}

					if !replay.VoteGranted {
						return false
					}
					gotVotes++
					DPrintf("follower %v got %v votes require %v in term %v", rf.me, gotVotes, len(rf.peers)/2+1, rf.currentTerm)
					if gotVotes > len(rf.peers)/2 {
						rf.state = LEADER
						return true
					}
					return false
				}()
				if isAchieved {
					break CANDIDATE_LOOP
				}
			}
		// (b) another server established itself as leader
		// handled in AppendEntries
		case result := <-rf.gotEntriesChan:
			{
				if result {
					break CANDIDATE_LOOP
				}
			}
		case result := <-rf.gotRequestVoteChan:
			{
				if result {
					break CANDIDATE_LOOP
				}
			}
		// (c) a period of time goes by with no winner
		// start a new election by incrementing its term and initiating another round of Request-Vote RPCs.
		case <-electionTimeout:
			{
				//incrementing term in next candidate loop
				break CANDIDATE_LOOP
			}
		}
		rf.mu.Lock()
		state = rf.state
		rf.mu.Unlock()
	}
}
