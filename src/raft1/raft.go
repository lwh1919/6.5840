package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//持久化
	currentTerm int //服务器最后一次知道的任期号，0
	votedFor    int //该服务器投给谁，-1，持久化，同义任期重复的投票
	logs        []logenty

	//
	commitIndex int // 已知最大提交索引
	lastApplied int // 最后被应用于状态机的索引
	//
	nextIndex  []int
	matchIndex []int

	state     int
	heartbeat time.Time

	electionTimeout time.Duration
}

type logenty struct {
	Term    int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.state == 1 {
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

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []logenty
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//init
	reply.Term = rf.currentTerm
	reply.Success = false

	// 1. 任期检查
	if args.Term < rf.currentTerm {
		return
	}
	if (args.Term > rf.currentTerm || args.Term == rf.currentTerm) && rf.state != -1 {
		rf.ToFollower(args.Term)
	}

	rf.heartbeat = time.Now()

}

func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) GetLogsLastTerm() int {
	LogsLastIndex := rf.GetLogsLastIndex()
	if LogsLastIndex == -1 {
		return 0
	}
	return rf.logs[LogsLastIndex].Term
}

func (rf *Raft) GetLogsLastIndex() int {
	return len(rf.logs) - 1
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 初始化回复
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	//任期更大
	if !(args.Term > rf.currentTerm || args.Term == rf.currentTerm && (args.LastLogTerm > rf.GetLogsLastTerm() || args.LastLogTerm == rf.GetLogsLastTerm() && args.LastLogIndex >= rf.GetLogsLastIndex())) {
		return
	}

	if args.Term > rf.currentTerm { //此判断也就是允许了“抢票的存在”
		rf.ToFollower(args.Term) //注意这里面的votefor只是设置为-1
		//注意这里不要return，因为还要判断能不能给当前的人投票
	}

	//处理同任期下 已经票投了的情况
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		// 本任期已经投给了其他人，拒绝投票
		return
	}

	//所有的选举通过
	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	DPrintf("%d votedFor = %d\n", rf.me, rf.votedFor)
}

func (rf *Raft) ToFollower(Term int) {
	rf.currentTerm = Term
	rf.votedFor = -1
	rf.state = -1
	rf.electionTimeout = time.Duration(150+rand.Intn(150)) * time.Millisecond
	DPrintf("%d ToFollower Term = %d\n", rf.me, rf.currentTerm)
}

func (rf *Raft) ToLeader() {
	rf.state = 1
	go rf.Heartbeat()
	DPrintf("%d ToLeader Term = %d\n", rf.me, rf.currentTerm)
}

func (rf *Raft) Heartbeat() {
	//应该不用加锁
	for rf.state == 1 {
		DPrintf("%d heartbeat Term %d \n", rf.me, rf.currentTerm)

		rf.mu.Lock()
		req := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.GetLogsLastIndex(),
			PrevLogTerm:  rf.GetLogsLastTerm(),
			Entries:      []logenty{},
			LeaderCommit: rf.commitIndex,
		}
		rf.mu.Unlock()

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go func(i int) {
				var reply AppendEntriesReply
				ok := rf.SendAppendEntries(i, &req, &reply)
				if ok {
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.mu.Unlock()
						rf.ToFollower(reply.Term)
						rf.mu.Lock()
					}
					rf.mu.Unlock()
				}
			}(i)
		}
		time.Sleep(75 * time.Millisecond)
	}
}

func (rf *Raft) ToCandidate() {
	rf.state = 0
	rf.votedFor = rf.me
	rf.currentTerm++
	DPrintf("%d ToCandidate Term = %d\n", rf.me, rf.currentTerm)

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

const electionTimeout = 500 * time.Millisecond

func (rf *Raft) ticker() {
	for rf.killed() == false {
		DPrintf("%d ticker  who %d  \n", rf.me, rf.state)
		rf.mu.Lock()
		// 只有Follower和Candidate需要检查选举超时
		if rf.state != 1 {
			// 检查是否超过选举超时时间
			if time.Since(rf.heartbeat) > rf.electionTimeout {
				// 触发新的选举
				rf.mu.Unlock()
				rf.startElection()
				rf.mu.Lock()
			}
		}

		rf.mu.Unlock()
		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	DPrintf("%d startElection term = %d\n", rf.me, rf.currentTerm)
	if rf.state == 1 {
		rf.mu.Unlock()
		return
	}
	rf.ToCandidate()
	currentElectionTerm := rf.currentTerm
	req := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.GetLogsLastIndex(),
		LastLogTerm:  rf.GetLogsLastTerm(),
	}
	rf.mu.Unlock()
	var cnt atomic.Int32
	cnt.Store(1)
	WinNeed := int32(len(rf.peers) / 2)
	timeoutCh := time.After(time.Duration(500) * time.Millisecond) // 选举过程超时
	resultCh := make(chan bool, 1)
	for i := 0; i < len(rf.peers); i++ {
		if rf.state != 0 {
			return
		}
		if i != rf.me {
			go func(i int) {
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(i, &req, &reply)
				if ok {
					if reply.Term > currentElectionTerm {
						rf.mu.Lock()
						rf.ToFollower(reply.Term)
						rf.mu.Unlock()
						// 通知选举终止
						select {
						case resultCh <- false:
						default:
						}
						return
					}
					// 关键检查2：确保我们仍在同一任期的选举中
					if rf.state != 0 || rf.currentTerm != currentElectionTerm {
						DPrintf("%d  Skip = %d\n", rf.me, rf.currentTerm)
						return // 状态已改变，忽略此回复
					}

					if reply.VoteGranted {
						cnt.Add(1)
						newCount := cnt.Load()
						if newCount > WinNeed {
							// 通知选举成功
							select {
							case resultCh <- true:
							default:
							}
						}
					}
				} else {

				}
			}(i)

		}
	}
	select {
	case won := <-resultCh:
		if won {
			rf.mu.Lock()
			rf.ToLeader()
			rf.mu.Unlock()
		}
		// 如果选举失败（由于更高任期），什么也不做，状态已更新
	case <-timeoutCh:
		DPrintf("%d timeout Term %d \n", rf.me, rf.currentTerm)
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.mu = sync.Mutex{}
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = -1
	rf.dead = 0
	rf.lastApplied = 0
	rf.logs = make([]logenty, 0)
	rf.commitIndex = 0
	rf.matchIndex = make([]int, 0)
	rf.nextIndex = make([]int, 0)
	rf.heartbeat = time.Now()
	rf.electionTimeout = time.Duration(150+rand.Intn(150)) * time.Millisecond

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	DPrintf("create raft %d\n", me)
	return rf
}
