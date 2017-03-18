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

const heartBeetsInterval = 130 * time.Millisecond
const minElectionTimeOut = 600
const macElectionTimeOut = 950

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
	state              string                 // server state, can be LEADER, FOLLOWER or CANDIDATE
	gotEntriesChan     chan AppendEntriesArgs // got entries
	checkEntriesChan   chan bool              // take or reject this Entries
	gotRequestVoteChan chan RequestVoteArgs   // got request vote
	grantVoteChan      chan bool              // grant vote or not
	applyMsgChan       chan ApplyMsg          // send an ApplyMsg to the service (or tester)

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

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	reader := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(reader)

	rf.mu.Lock()
	defer rf.mu.Unlock()
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
// just send request to main goroutine, and wait for reply
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	rf.gotRequestVoteChan <- *args
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
	reply.VoteGranted = <-rf.grantVoteChan
	DPrintf("%v got request vote from %v, result %v\n", rf.me, args.CandidateID, reply)
	return
}

// sendRequestVote RPC callee
// just send reply to main goroutine and handle it
func (rf *Raft) sendRequestVote(server *labrpc.ClientEnd, args *RequestVoteArgs, reply *RequestVoteReply, ch chan RequestVoteReply) bool {
	ok := server.Call("Raft.RequestVote", args, reply)
	DPrintf("%v sendRequestVote in term %v", rf.me, rf.currentTerm)
	if ok {
		ch <- *reply
	}
	return ok
}

// AppendEntriesArgs field names must start with capital letters!
type AppendEntriesArgs struct {
	Term         int           // leader's term
	LeaderID     int           // so follower can redirect clients
	PrevLogIndex int           // index of log entry immediately preceding new ones
	PreLogTerm   int           // term of prevLogIndex entry
	Entries      []interface{} // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int           // leader's commitIndex
}

// AppendEntriesReply field names must start with capital letters!
type AppendEntriesReply struct {
	Term    int  // currentTerm, for candidate to update itself
	Success bool // if follower contained entry matching prevLogIndex and prevLogTerm
}

// AppendEntries got entries, send request to main goroutine and wait for reply
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	rf.gotEntriesChan <- *args
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
	reply.Success = <-rf.checkEntriesChan
	DPrintf("%v got entries from %v, result %v\n", rf.me, args.LeaderID, reply)
	return
}

func (rf *Raft) sendAppendEntries(server *labrpc.ClientEnd, args *AppendEntriesArgs, reply *AppendEntriesReply, ch chan AppendEntriesReply) bool {
	ok := server.Call("Raft.AppendEntries", args, reply)
	DPrintf("%v send entries in term %v\n", rf.me, rf.currentTerm)
	if ok {
		ch <- *reply
	}
	return ok
}

func (rf *Raft) sendEntriesToServers(replayChan chan AppendEntriesReply, entries []interface{}) {
	for index, server := range rf.peers {
		prevLogIndex := rf.nextIndex[index]
		preLogTerm := 0
		if prevLogIndex < rf.commitIndex {
			preLogTerm = rf.log[prevLogIndex-1].Term
		}
		appendEntriesArgs := AppendEntriesArgs{
			rf.currentTerm, // leader's term
			rf.me,          // leader's id
			prevLogIndex,   // index of log entry immediately preceding new ones
			preLogTerm,     // term of prevLogIndex entry
			entries,        // log entries to store (empty for heartbeat; may send more than one for efficiency) todo  内容从prevLogIndex 到现在?
			rf.commitIndex} // leader’s commitIndex
		DPrintf("%v sendEntries in Term %v", rf.me, rf.currentTerm)
		appendEntriesReply := new(AppendEntriesReply)
		go rf.sendAppendEntries(server, &appendEntriesArgs, appendEntriesReply, replayChan)
	}
}

func (rf *Raft) makeAgreement(command interface{}) {
	replyChan := make(chan AppendEntriesReply)
	rf.sendEntriesToServers(replyChan, []interface{}{command})
	rf.applyMsgChan <- ApplyMsg{rf.commitIndex, command, false, nil} // todo Snapshot
	// todo
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
		// step 1, leader appends the command to its logs as a new entry
		rf.log = append(rf.log, Log{rf.currentTerm, command})
		rf.commitIndex = len(rf.log)
		rf.persist()

		// step 2, issues AppendEntries RPCs in parallel to each of the other servers to replicate the entry.
		go rf.makeAgreement(command)

		// todo comment in 6.824, we should return result immediately. but in paper, we should wait for the result
		return rf.commitIndex, rf.currentTerm, isLeader
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
	rf.gotEntriesChan = make(chan AppendEntriesArgs)
	rf.checkEntriesChan = make(chan bool)
	rf.applyMsgChan = applyCh

	rf.gotRequestVoteChan = make(chan RequestVoteArgs)
	rf.grantVoteChan = make(chan bool)

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]Log, 0)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	for index := range peers {
		rf.nextIndex[index] = rf.commitIndex + 1
	}

	rf.matchIndex = make([]int, 0) // todo

	rf.state = FOLLOWER

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func(rf *Raft) {
		for {
			DPrintf("%v is %v", rf.me, rf.state)
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
	randTime := rand.Intn(macElectionTimeOut-minElectionTimeOut) + minElectionTimeOut
	return time.After(time.Duration(randTime) * time.Millisecond)
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
	for _, server := range rf.peers {
		requestVoteReply := new(RequestVoteReply)
		go rf.sendRequestVote(server, &requestVoteArgs, requestVoteReply, replayChan)
	}
	return replayChan
}

// incoming RequestVote RPC has a higher term that you,
// you should first step down and adopt their term (thereby resetting votedFor),
// and then handle the RPC
func (rf *Raft) resetState(term int) {
	rf.mu.Lock()
	rf.state = FOLLOWER
	rf.currentTerm = term
	rf.votedFor = -1
	rf.mu.Unlock()
}

// follower state
func (rf *Raft) followerStuff() {
	timeOut := getElectionTimeout() // note: if the term in the AppendEntries arguments is outdated, do not reset timer
FOLLOWER_LOOP:
	for {
		select {
		// get entries from leader
		case entriesArgs := <-rf.gotEntriesChan:
			{
				// 1. Reply false if term < currentTerm (§5.1)
				// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
				// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
				// 4. Append any new entries not already in the log
				// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)

				// 1
				if entriesArgs.Term < rf.currentTerm {
					rf.checkEntriesChan <- false
					break
				}
				// 2
				if entriesArgs.PrevLogIndex > rf.commitIndex {
					rf.checkEntriesChan <- false
					break
				}

				// 3 just append in logs
				rf.mu.Lock()
				rf.currentTerm = entriesArgs.Term
				rf.checkEntriesChan <- true
				// todo append entries into log
				if entriesArgs.LeaderCommit > rf.commitIndex {
					if entriesArgs.LeaderCommit > len(rf.log) {
						rf.commitIndex = len(rf.log)
					} else {
						rf.commitIndex = entriesArgs.LeaderCommit
					}
				}
				DPrintf("follower %v received AppendEntries %v in term %v\n", rf.me, entriesArgs, rf.currentTerm)
				rf.mu.Unlock()
				timeOut = getElectionTimeout()
			}
		// got an vote request from candidate
		case requestVoteArgs := <-rf.gotRequestVoteChan:
			{
				// 1. Reply false if term < currentTerm (§5.1)
				// 2.
				//  a.If votedFor is null or candidateId
				//  b. candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.4)
				rf.mu.Lock()

				// 1
				var result bool
				if requestVoteArgs.Term < rf.currentTerm {
					rf.grantVoteChan <- false
					DPrintf("follower %v received RequestVote from %v in term %v, result false \n", rf.me, requestVoteArgs.CandidateID, requestVoteArgs.Term)
					DPrintf("reject because stale term\n")
					rf.mu.Unlock()
					break
				}

				if requestVoteArgs.Term > rf.currentTerm {
					rf.currentTerm = requestVoteArgs.Term
					rf.votedFor = requestVoteArgs.CandidateID
				}

				// 2 a
				if rf.votedFor == -1 || rf.votedFor == requestVoteArgs.CandidateID {
					result = true
				} else {
					DPrintf("reject because voted for %v\n", rf.votedFor)
					result = false
				}

				// 2 b
				if rf.commitIndex > requestVoteArgs.LastLogIndex {
					DPrintf("reject because stale log \n")
					result = false
				}
				if result {
					timeOut = getElectionTimeout()
				}
				rf.grantVoteChan <- result
				DPrintf("follower %v received RequestVote %v in term %v, result %v \n", rf.me, requestVoteArgs, requestVoteArgs.Term, result)
				rf.mu.Unlock()
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

// used in leader or candidate state
// return true if found another leader
func (rf *Raft) gotRequestVote(request RequestVoteArgs) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%v %v got request vote in term %v, the other leader's term is %v", rf.state, rf.me, rf.currentTerm, request.Term)
	if request.Term > rf.currentTerm {
		rf.resetState(request.Term)
		rf.grantVoteChan <- true
		return true
	}
	rf.grantVoteChan <- false
	return false
}

// used in leader or candidate state
// if discovers that its term is out of date, it immediately reverts to follower state.
// if receives a request with a stale term number. it rejects the request
func (rf *Raft) gotEntries(entries AppendEntriesArgs) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%v %v got entries in term %v, the other leader's term is %v", rf.state, rf.me, rf.currentTerm, entries.Term)
	if rf.state == LEADER && entries.Term > rf.currentTerm ||
		rf.state == CANDIDATE && entries.Term >= rf.currentTerm {
		rf.resetState(entries.Term)
		rf.checkEntriesChan <- true // todo apply entries
		return true
	}
	return false
}

func (rf *Raft) leaderStuff() {
	replyChan := make(chan AppendEntriesReply)
	heartbeat := time.Tick(heartBeetsInterval)
LEADER_LOOP:
	for {
		select {
		case <-replyChan:
			{
				// todo
			}
		case entries := <-rf.gotEntriesChan:
			{
				isFoundOtherLeader := rf.gotEntries(entries)
				if isFoundOtherLeader {
					break LEADER_LOOP
				}
			}
		case request := <-rf.gotRequestVoteChan:
			{
				isFoundOtherLeader := rf.gotRequestVote(request)
				if isFoundOtherLeader {
					break LEADER_LOOP
				}
			}
		case <-heartbeat:
			{
				rf.mu.Lock()
				rf.sendEntriesToServers(replyChan, nil)
				rf.mu.Unlock()
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
	t0 := time.Now()
	rf.mu.Unlock()
	replayChan := rf.sendRequestVoteToServers()
	gotVotes := 1 // initial to it self
CANDIDATE_LOOP:
	for {
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
		// If the leader’s term (included in its RPC)
		// is at least as large as the candidate’s current term,
		// then the candidate recognizes the leader as legitimate and
		// returns to follower state. If the term in the RPC is smaller than the candidate’s current term,
		// then the candidate rejects the RPC and continues in candidate state.
		case entries := <-rf.gotEntriesChan:
			{
				isFoundOtherLeader := rf.gotEntries(entries)
				if isFoundOtherLeader {
					break CANDIDATE_LOOP
				}
			}
		case request := <-rf.gotRequestVoteChan:
			{
				isFoundOtherLeader := rf.gotRequestVote(request)
				if isFoundOtherLeader {
					break CANDIDATE_LOOP
				}
			}
		// (c) a period of time goes by with no winner
		// start a new election by incrementing its term and initiating another round of Request-Vote RPCs.
		case <-electionTimeout:
			{
				//incrementing term in next candidate loop
				DPrintf("%v time out!\n", time.Since(t0))
				t0 = time.Now()
				break CANDIDATE_LOOP
			}
		}
	}
}
