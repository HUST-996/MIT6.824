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

	"bytes"
	"math"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
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
	Command interface{}
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
	raftStatus        RaftStatus
	currentTerm       int // 当前任期
	votedFor          int // 投票给哪个节点
	log               []LogEntry
	lastIncludedIndex int
	lastIncludedTerm  int
	snapshot          []byte

	commitIndex int
	lastApplied int
	applyCh     chan ApplyMsg
	cond        *sync.Cond

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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	// rf.mu.Lock()
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	// DPrintf(dLog, "S%d persist 1LastIncludedTerm%d", rf.me, rf.lastIncludedTerm)
	// rf.mu.Unlock()
	raftState := w.Bytes()

	rf.persister.Save(raftState, rf.snapshot)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil || d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		return
	}
	rf.mu.Lock()
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = append([]LogEntry{}, log...)
	rf.lastIncludedIndex = lastIncludedIndex
	// DPrintf(dLog, "S%d 1LastIncludedIndex%d", rf.me, rf.lastIncludedIndex)
	rf.lastIncludedTerm = lastIncludedTerm
	// DPrintf(dLog, "S%d 1LastIncludedTerm%d", rf.me, rf.lastIncludedTerm)
	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex
	rf.log[0].Index = rf.lastIncludedIndex
	rf.log[0].Term = rf.lastIncludedTerm
	rf.mu.Unlock()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// DPrintf(dLog, "S%d 快照, idx为%d\n", rf.me, index)
	// Your code here (3D).
	rf.mu.Lock()
	if index <= rf.lastIncludedIndex || rf.commitIndex < index {
		rf.mu.Unlock()
		return
	}
	rf.snapshot = append([]byte{}, snapshot...)
	for i, v := range rf.log {
		if v.Index == index {
			rf.lastIncludedIndex = index
			if rf.lastApplied < index {
				rf.lastApplied = index
			}
			// DPrintf(dLog, "S%d 2LastIncludedIndex%d", rf.me, rf.lastIncludedIndex)
			rf.lastIncludedTerm = v.Term
			rf.log = rf.log[i+1:]
			var tmpLog []LogEntry = make([]LogEntry, 1)
			rf.log = append(tmpLog, rf.log...)
			rf.log[0].Index = index
			rf.log[0].Term = v.Term
			break
		}
	}
	// DPrintf(dLog, "S%d log为%v\n", rf.me, rf.log)
	rf.persist()
	rf.mu.Unlock()
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	// DPrintf(dLog, "S%d 收%d拉，我term%d,vf%d,st%d\n", rf.me, args.CandidateId, rf.currentTerm, rf.votedFor, rf.raftStatus)
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	} else if args.Term > rf.currentTerm {
		rf.raftStatus = Follower
		rf.currentTerm = args.Term
		// DPrintf(dLog, "S%d 我身份改为follower\n", rf.me)
		rf.votedFor = -1
		rf.persist()
	}
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.VoteGranted = false
		return
	}
	isUpToDate := false
	if args.LastLogTerm > rf.log[len(rf.log)-1].Term {
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
		rf.persist()
		// DPrintf(dLog, "S%d 我投票给%d，term为%d,我身份为%d\n", rf.me, args.CandidateId, rf.currentTerm, rf.raftStatus)
		rf.raftStatus = Follower
		// DPrintf(dLog, "S%d 传来的lastlogidx%d\n", rf.me, args.LastLogIndex)
		// DPrintf(dLog, "S%d 我的日志%v\n", rf.me, rf.log)
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
	XTerm   int
	XIndex  int
	XLen    int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		// DPrintf(dLog, "S%d 我收到心跳,term为%d\n", rf.me, rf.currentTerm)
		reply.Success = false
		reply.XIndex = -1
		reply.XLen = -1
		reply.XTerm = -1
		reply.Term = rf.currentTerm
		return
	} else {
		rf.lastHeartbeatTime = time.Now().UnixNano() / int64(time.Millisecond)
		rf.raftStatus = Follower
		rf.currentTerm = args.Term
		// DPrintf(dLog, "S%d 我为fw,term为%d\n", rf.me, rf.currentTerm)
	}
	if args.PreLogIndex < rf.lastIncludedIndex {
		reply.XLen = -2
		return
	}
	// 跟随者的日志太短
	// 17 18 19 20 21
	// 17 18 19 20
	if args.PreLogIndex > rf.log[len(rf.log)-1].Index {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.XIndex = -1
		reply.XTerm = -1
		reply.XLen = rf.log[len(rf.log)-1].Index + 1
		// DPrintf(dLog, "S%d 我收到%d的日志，我日志太短,lastincludedIndex%d\n", rf.me, args.LeaderId, rf.lastIncludedIndex)
		return
	}
	// DPrintf(dLog, "S%d prelogidx为%d,lastincludeidx为%d\n", rf.me, args.PreLogIndex, rf.lastIncludedIndex)
	if args.PreLogIndex >= rf.lastIncludedIndex && rf.log[args.PreLogIndex-rf.lastIncludedIndex].Term != args.PreLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.XTerm = rf.log[args.PreLogIndex-rf.lastIncludedIndex].Term
		xIndex := args.PreLogIndex
		for xIndex-rf.lastIncludedIndex >= 0 && rf.log[xIndex-rf.lastIncludedIndex].Term == rf.log[args.PreLogIndex-rf.lastIncludedIndex].Term {
			xIndex--
		}
		reply.XIndex = xIndex + 1
		reply.XLen = -1
		// DPrintf(dLog, "S%d 我的xindex%d,xTerm为%d\n", rf.me, reply.XIndex, reply.XTerm)
		return
	}
	// TODO 3.如果本来的日志比leader还长
	if args.PreLogIndex < rf.log[len(rf.log)-1].Index {
		// DPrintf(dLog, "S%d 截断，原日志长度%d,prelogidx为%d\n", rf.me, len(rf.log), args.PreLogIndex)
		rf.log = append([]LogEntry{}, rf.log[:args.PreLogIndex+1-rf.lastIncludedIndex]...)
	}
	// TODO 4.加入新的日志条目
	rf.log = append(rf.log, args.Entries...)
	rf.persist()
	// DPrintf(dLog, "S%d 加入日志，为%v,总%v\n", rf.me, args.Entries, rf.log)
	// TODO 5.如果arg.leaderCommit>commitIndex,再做处理
	if args.LeaderCommit > rf.commitIndex {
		// DPrintf(dLog, "S%d leadercommit为%v,commitidx为%d\n", rf.me, args.LeaderCommit, rf.commitIndex)
		rf.commitIndex = min(args.LeaderCommit, rf.log[len(rf.log)-1].Index)
		rf.cond.Broadcast()
	}
	reply.Success = true
	reply.Term = rf.currentTerm
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) majorityMatch(N int) bool {
	count := 1
	for i := range rf.matchIndex {
		if i == rf.me {
			continue
		}
		if rf.matchIndex[i] >= N {
			count++
			if count > len(rf.matchIndex)/2 {
				return true
			}
		}
	}
	return false
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	// DPrintf(dLog, "S%d 收到%v的快照\n", rf.me, args.LeaderId)
	if rf.currentTerm > args.Term && args.LastIncludedIndex <= rf.lastIncludedIndex {
		rf.mu.Unlock()
		return
	}
	if args.LastIncludedIndex > rf.lastIncludedIndex {
		rf.raftStatus = Follower
		rf.lastIncludedIndex = args.LastIncludedIndex
		// DPrintf(dLog, "S%d 3LastIncludedIndex%d", rf.me, rf.lastIncludedIndex)
		rf.lastIncludedTerm = args.LastIncludedTerm
		if rf.log[len(rf.log)-1].Index < args.LastIncludedIndex {
			rf.log = rf.log[0:1]
		} else {
			for i, v := range rf.log {
				if v.Index == args.LastIncludedIndex && v.Term == args.LastIncludedTerm {
					rf.log = rf.log[i+1:]
					tmpLog := make([]LogEntry, 1)
					rf.log = append(tmpLog, rf.log...)
					rf.log[0].Index = args.LastIncludedIndex
					rf.log[0].Term = args.LastIncludedTerm
					// rf.mu.Unlock()
				}
			}
			rf.log = make([]LogEntry, 1)
			rf.log[0].Index = args.LastIncludedIndex
			rf.log[0].Term = args.LastIncludedTerm
		}
		if rf.lastApplied < rf.lastIncludedIndex {
			rf.lastApplied = rf.lastIncludedIndex
		}
		if rf.commitIndex < rf.lastIncludedIndex {
			rf.commitIndex = rf.lastIncludedIndex
			// DPrintf(dLog, "S%d rf.commitIndex%d", rf.me, rf.commitIndex)
		}
		rf.log[0].Index = rf.lastIncludedIndex
		rf.log[0].Term = rf.lastIncludedTerm
		rf.snapshot = args.Data
		rf.persist()
		snapApplyMsg := ApplyMsg{
			CommandValid:  false,
			SnapshotValid: true,
			SnapshotIndex: args.LastIncludedIndex,
			SnapshotTerm:  args.LastIncludedTerm,
			Snapshot:      args.Data,
		}
		rf.applyCh <- snapApplyMsg
		rf.mu.Unlock()
	} else {
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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
	term, isLeader = rf.GetState()
	rf.mu.Lock()
	if !isLeader {
		index = rf.log[len(rf.log)-1].Index + 1
		rf.mu.Unlock()
		return index, term, isLeader
	}
	// rf.mu.Lock()
	// DPrintf(dLeader, "S%d 我是领导者，收到了客户端命令%v\n", rf.me, command)
	index = rf.log[len(rf.log)-1].Index + 1
	logEntry := LogEntry{Index: index, Command: command, Term: term}
	rf.log = append(rf.log, logEntry)
	rf.persist()
	rf.mu.Unlock()
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
	rf.currentTerm++
	// DPrintf(dLog, "S%d 我开始选举，最新term为%d\n", rf.me, rf.currentTerm)
	rf.votedFor = rf.me
	rf.persist()
	cnt := 1
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.log[len(rf.log)-1].Index,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	rf.mu.Unlock()
	var mu sync.Mutex
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			// DPrintf(dVote, "S%d 我正在选举，我给%d发消息\n", rf.me, i)
			reply := &RequestVoteReply{}
			ok := false
			rf.mu.Lock()
			term := rf.currentTerm
			rf.mu.Unlock()
			for !ok {
				rf.mu.Lock()
				if rf.raftStatus != Candidate || term != rf.currentTerm {
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
				ok = rf.sendRequestVote(i, args, reply)
			}
			mu.Lock()
			defer mu.Unlock()
			rf.mu.Lock()
			if reply.VoteGranted && reply.Term == rf.currentTerm {
				// DPrintf(dTerm, "S%d %d给我投票\n", rf.me, i)
				cnt++
			}
			// rf.mu.Lock()
			if cnt == len(rf.peers)/2+1 && reply.VoteGranted && rf.raftStatus == Candidate && reply.Term == rf.currentTerm {
				rf.raftStatus = Leader
				for i := range rf.nextIndex {
					rf.nextIndex[i] = rf.log[len(rf.log)-1].Index + 1
					rf.matchIndex[i] = 0
				}
				// DPrintf(dTerm, "S%d 我成功当选，term为%d\n", rf.me, rf.currentTerm)
				rf.sendHeartBeats()
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
		}(i)
	}
}

func (rf *Raft) sendInstallSnapshotToOne(server int, currentTerm int, leaderId int, lastIncludedIndex int, lastIncludedTerm int, data []byte) {
	args := &InstallSnapshotArgs{
		Term:              currentTerm,
		LeaderId:          leaderId,
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Data:              data,
	}
	reply := &InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(server, args, reply)
	// DPrintf(dLog, "S%d 发送快照给%v\n", rf.me, server)
	if !ok {
		// DPrintf(dLog, "S%d 发送快照给%v失败\n", rf.me, server)
		// rf.mu.Unlock()
		return
	}
	// DPrintf(dLog, "S%d 发送快照给%v成功\n", rf.me, server)
	// rf.mu.Unlock()
	rf.mu.Lock()
	rf.nextIndex[server] = lastIncludedIndex + 1
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendHeartBeatToOne(server int) {
	rf.mu.Lock()
	if rf.raftStatus != Leader || rf.killed() {
		rf.mu.Unlock()
		return
	}
	// DPrintf(dLog, "S%d 发送心跳给%v\n", rf.me, server)
	nextIdx := rf.nextIndex[server]
	term := rf.currentTerm
	leaderId := rf.me
	preLogIndex := int(math.Max(float64(nextIdx)-1, 0))
	// DPrintf(dTerm, "S%d preLogIndex为%d，lastlogidx%d\n", rf.me, preLogIndex, rf.lastIncludedIndex)
	if preLogIndex-rf.lastIncludedIndex < 0 {
		go rf.sendInstallSnapshotToOne(server, rf.currentTerm, rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, rf.snapshot)
		rf.mu.Unlock()
		return
	}
	preLogTerm := rf.log[preLogIndex-rf.lastIncludedIndex].Term
	leaderCommit := rf.commitIndex
	// 这里就能保证nextIdx <= last log idx的时候发entry
	entries := append([]LogEntry{}, rf.log[nextIdx-rf.lastIncludedIndex:]...)
	rf.mu.Unlock()
	args := &AppendEntriesArgs{
		Term:         term,
		LeaderId:     leaderId,
		PreLogIndex:  preLogIndex,
		PreLogTerm:   preLogTerm,
		LeaderCommit: leaderCommit,
		Entries:      entries,
	}
	reply := &AppendEntriesReply{}
	// ok := false
	// for !ok {
	ok := rf.sendAppendEntries(server, args, reply)
	if !ok {
		return
	}
	// 	time.Sleep(time.Duration(120) * time.Millisecond)
	// }
	rf.mu.Lock()
	if reply.Success {
		// DPrintf(dLog, "S%d 成功\n", rf.me)
		rf.matchIndex[server] = preLogIndex + len(entries)
		rf.nextIndex[server] = nextIdx + len(entries)
		for N := rf.log[len(rf.log)-1].Index; N >= rf.commitIndex+1; N-- {
			if rf.majorityMatch(N) && rf.log[N-rf.lastIncludedIndex].Term == rf.currentTerm {
				rf.commitIndex = N
				// DPrintf(dLog, "S%d 大多数人赞同%v\n", rf.me, N)
				rf.cond.Broadcast()
				break
			}
		}
	} else {
		// rf.nextIndex[server] = int(math.Max(1.0, float64(rf.nextIndex[server]-1)))
		if reply.XLen == -1 && reply.XIndex == -1 && reply.XTerm == -1 {
			rf.mu.Unlock()
			return
		}
		if reply.XLen == -2 {
			rf.mu.Unlock()
			go rf.sendInstallSnapshotToOne(server, rf.currentTerm, rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, rf.snapshot)
			return
		}
		if reply.XLen >= 0 {
			rf.nextIndex[server] = int(math.Max(1.0, float64(reply.XLen)))
		} else {
			flag := false
			for i, v := range rf.log {
				if v.Term == reply.XTerm {
					flag = true
					rf.nextIndex[server] = int(math.Max(1.0, float64(rf.log[i].Index)))
					break
				}
			}
			if !flag {
				rf.nextIndex[server] = int(math.Max(1.0, float64(reply.XIndex)))
				// DPrintf(dLog, "S%d XIndex%d\n", rf.me, rf.nextIndex[server])
			}
		}
		// DPrintf(dLog, "S%d Xlen%d,XIndex%d,XTerm%d\n", rf.me, reply.XLen, reply.XIndex, reply.XTerm)
		go rf.sendHeartBeatToOne(server)
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendHeartBeats() {
	// DPrintf(dLog, "S%d 发送心跳\n", rf.me)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendHeartBeatToOne(i)
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (3A)
		// Check if a leader election should be started.
		var ms int64
		rf.mu.Lock()
		// status := rf.raftStatus
		// rf.mu.Unlock()
		if rf.raftStatus == Leader {
			rf.mu.Unlock()
			go rf.sendHeartBeats()
			ms = 120
			time.Sleep(time.Duration(ms) * time.Millisecond)
		} else {
			// rf.mu.Lock()
			// lastTime := rf.lastHeartbeatTime
			// rf.mu.Unlock()
			if time.Now().UnixNano()/int64(time.Millisecond)-rf.lastHeartbeatTime > rf.electionTimeout {
				// DPrintf(dLog, "S%d 我超时了\n", rf.me)
				// rf.mu.Lock()
				rf.lastHeartbeatTime = time.Now().UnixNano() / int64(time.Millisecond)
				rf.votedFor = -1
				rf.persist()
				rf.mu.Unlock()
				go rf.startElection()
			} else {
				rf.mu.Unlock()
				ms = 50 + (rand.Int63() % 10)
				time.Sleep(time.Duration(ms) * time.Millisecond)
			}
		}
	}
}

func (rf *Raft) ApplyCmdToMachine() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.cond.Wait()
		}
		rf.lastApplied++
		lastApplied := rf.lastApplied
		if lastApplied-rf.lastIncludedIndex < 0 {
			rf.lastApplied = rf.lastIncludedIndex
			rf.mu.Unlock()
			continue
		}
		if lastApplied-rf.lastIncludedIndex >= len(rf.log) {
			rf.mu.Unlock()
			continue
		}
		command := rf.log[lastApplied-rf.lastIncludedIndex].Command

		msg := ApplyMsg{
			CommandValid:  true,
			Command:       command,
			CommandIndex:  lastApplied,
			SnapshotValid: false,
		}
		rf.mu.Unlock()
		rf.applyCh <- msg
		// DPrintf(dLog, "S%d 给虚拟机发消息，%d\n", rf.me, lastApplied)
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
	rf.log = append(rf.log, LogEntry{Index: 0, Command: "111", Term: 0})

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.cond = sync.NewCond(&rf.mu)

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.lastHeartbeatTime = time.Now().UnixNano() / int64(time.Millisecond)
	rf.electionTimeout = 550 + (rand.Int63() % 200)
	// initialize from state persisted before a crash

	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = rf.persister.Copy().ReadSnapshot()
	// DPrintf(dPersist, "S%d 恢复，%v\n", rf.me, rf.log)
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.ApplyCmdToMachine()

	return rf
}
