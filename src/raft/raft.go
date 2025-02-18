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
	//	"bytes"

	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Index   int
	Command string
	Term    int
}

type RaftStatus int

const (
	Leader RaftStatus = iota
	Follower
	Candidate
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	raftStatus  RaftStatus
	currentTerm int // 当前任期
	votedFor    int // 投票给哪个节点
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	lastHeartbeatTime int64
	electionTimeout   int64
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool = false
	// Your code here (3A).
	term = rf.currentTerm
	DPrintf(dLog, "S%d 我身份被查\n", rf.me)
	if rf.raftStatus == Leader {
		isleader = true
	}
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int // 候选人日志中最后一个条目索引
	LastLogTerm  int // 候选人日志中最后一个任期
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	DPrintf(dLog, "S%d 收%d拉，我term%d,vf%d,st%d\n", rf.me, args.CandidateId, rf.currentTerm, rf.votedFor, rf.raftStatus)
	reply.Term = rf.currentTerm
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	} else if args.Term > rf.currentTerm {
		rf.raftStatus = Follower
		DPrintf(dLog, "S%d 我身份改为follower\n", rf.me)
		rf.votedFor = -1
	}
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.VoteGranted = false
		return
	}
	isUpToDate := false
	if args.LastLogTerm > rf.log[rf.lastApplied].Term {
		isUpToDate = true
	} else if args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= rf.log[len(rf.log)-1].Index {
		isUpToDate = true
	}
	if isUpToDate {
		reply.VoteGranted = true
		reply.Term = args.Term
		rf.lastHeartbeatTime = time.Now().UnixNano() / int64(time.Millisecond)
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		DPrintf(dLog, "S%d 我投票给%d，term为%d,我身份为%d\n", rf.me, args.CandidateId, rf.currentTerm, rf.raftStatus)
		rf.raftStatus = Follower
		DPrintf(dLog, "S%d 我身份改为follower\n", rf.me)
		return
	}
	reply.VoteGranted = false
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PreLogIndex  int
	PreLogTerm   int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// if rf.raftStatus == Candidate {
	// }
	DPrintf(dLog, "S%d 我收到%d的心跳,我之前term为%d\n", rf.me, args.LeaderId, rf.currentTerm)
	// rf.votedFor = -1
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	} else {
		rf.lastHeartbeatTime = time.Now().UnixNano() / int64(time.Millisecond)
		rf.raftStatus = Follower
		rf.currentTerm = args.Term
		DPrintf(dLog, "S%d 我为fw,term为%d\n", rf.me, rf.currentTerm)
	}
	if rf.log[args.PreLogIndex].Term != args.PreLogTerm {
		reply.Success = false
		return
	}
	// TODO 3.如果传来的日志索引有和本地一样但term不同的需要删掉该条目以及之后所有的日志

	// TODO 4.加入新的日志条目

	// TODO 5.如果arg.leaderCommit>commitIndex,再做处理

	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.raftStatus = Candidate
	DPrintf(dLog, "S%d 我身份改为can\n", rf.me)
	rf.currentTerm++
	DPrintf(dLog, "S%d 我开始选举，最新term为%d\n", rf.me, rf.currentTerm)
	rf.votedFor = rf.me
	rf.mu.Unlock()
	cnt := 1
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.log[len(rf.log)-1].Index,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	// stopCh := make(chan bool, 1)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		var mu sync.Mutex
		go func(i int) {
			DPrintf(dVote, "S%d 我正在选举，我给%d发消息\n", rf.me, i)
			reply := &RequestVoteReply{}
			ok := false
			term := rf.currentTerm
			for !ok {
				if rf.raftStatus != Candidate || term != rf.currentTerm {
					return
				}
				ok = rf.sendRequestVote(i, args, reply)
			}
			mu.Lock()
			defer mu.Unlock()
			if reply.VoteGranted && reply.Term == rf.currentTerm {
				cnt++
			}
			if cnt == len(rf.peers)/2+1 && reply.VoteGranted && rf.raftStatus == Candidate {
				rf.mu.Lock()
				rf.raftStatus = Leader
				rf.mu.Unlock()
				DPrintf(dTerm, "S%d 我成功当选，term为%d\n", rf.me, rf.currentTerm)
				rf.sendHeartBeats()
				return
			}
		}(i)
	}
	// select {
	// case <-time.After(time.Duration(rf.electionTimeout)): // 如果超过n秒还没有选举成功
	// 	rf.mu.Lock()
	// 	stopCh <- true
	// 	rf.raftStatus = Follower
	// 	fmt.Printf("我是%d,我超时了，term为%d\n", rf.me, rf.currentTerm)
	// 	rf.mu.Unlock()
	// 	rf.votedFor = -1
	// 	rf.lastHeartbeatTime = time.Now().UnixNano() / int64(time.Millisecond)
	// }
}

func (rf *Raft) sendHeartBeats() {
	args := &AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		reply := &AppendEntriesReply{}
		rf.sendAppendEntries(i, args, reply)
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (3A)
		// Check if a leader election should be started.
		var ms int64
		if rf.raftStatus == Leader {
			go rf.sendHeartBeats()
			ms = 100
			time.Sleep(time.Duration(ms) * time.Millisecond)
		} else {
			if time.Now().UnixNano()/int64(time.Millisecond)-rf.lastHeartbeatTime > rf.electionTimeout {
				DPrintf(dLog, "S%d 我超时了\n", rf.me)
				rf.lastHeartbeatTime = time.Now().UnixNano() / int64(time.Millisecond)
				rf.votedFor = -1
				go rf.startElection()
			} else {
				ms = 50 + (rand.Int63() % 10)
				time.Sleep(time.Duration(ms) * time.Millisecond)
			}
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	os.Setenv("VERBOSE", "1")
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.raftStatus = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{}
	rf.log = append(rf.log, LogEntry{Index: 0, Command: "", Term: 0})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.lastHeartbeatTime = time.Now().UnixNano() / int64(time.Millisecond)
	rf.electionTimeout = 650 + (rand.Int63() % 150)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
