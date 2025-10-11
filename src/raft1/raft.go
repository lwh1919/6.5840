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
	tester "6.5840/tester1"
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
	Logs        []LogEntry

	//
	CommitIndex int // 已知最大提交索引
	LastApplied int // 最后被应用于状态机的索引
	//
	NextIndex  []int
	MatchIndex []int

	State     int
	Heartbeat time.Time

	ElectionTimeout time.Duration

	// 防止并发的LeaderAppendEntries调用
	appendInProgress bool

	ApplyCh chan raftapi.ApplyMsg
}

type LogEntry struct {
	Term    int
	Command interface{}
	Index   int //日志压缩才有用
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
	if rf.State == 1 {
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
	if len(data) < 1 { // bootstrap without any state?
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
	Entries      []LogEntry
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
	//记录心跳时间
	rf.Heartbeat = time.Now()
	//比自己低但不是跟随者
	//if (args.Term > rf.currentTerm || args.Term == rf.currentTerm) && rf.State != -1 {
	//	rf.ToFollower(args.Term)
	//}
	//bug，这样子就不会更新跟随者多term了

	rf.ToFollower(args.Term)

	// 检查日志一致性
	logConsistent := false
	if args.PrevLogIndex == -1 {
		// PrevLogIndex为-1表示从头开始，总是一致的
		logConsistent = true
	} else if args.PrevLogIndex >= 0 && args.PrevLogIndex < len(rf.Logs) {
		// 检查指定位置的日志条目是否匹配
		if rf.Logs[args.PrevLogIndex].Term == args.PrevLogTerm {
			logConsistent = true
		}
	}

	//开始日志复制
	if len(args.Entries) > 0 {
		if logConsistent {
			// 删除从PrevLogIndex+1开始的所有现有日志条目
			rf.Logs = rf.Logs[:args.PrevLogIndex+1]
			// 追加所有新的日志条目
			rf.Logs = append(rf.Logs, args.Entries...)
			reply.Success = true
			DPrintf("[%d] Appended entries from leader %d, log length: %d\n", rf.me, args.LeaderId, len(rf.Logs))
		}
	} else {
		//心跳包 - 只有在日志一致时才成功
		if logConsistent {
			reply.Success = true
		}
	}

	//更新commitIndex - 只在日志一致且成功时推进
	if args.LeaderCommit > rf.CommitIndex && reply.Success {
		rf.CommitIndex = min(args.LeaderCommit, rf.GetLogsLastIndex())
		DPrintf("[%d] Updated commitIndex to %d\n", rf.me, rf.CommitIndex)
	}
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
	if LogsLastIndex < 0 {
		return 0
	}
	return rf.Logs[LogsLastIndex].Term
}

func (rf *Raft) GetLogsLastIndex() int {
	return len(rf.Logs) - 1
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 初始化回复
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// 1. 任期检查
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.ToFollower(args.Term)
	}

	// 2. 日志新旧程度检查 - 候选人的日志必须至少和自己的一样新
	myLastLogTerm := rf.GetLogsLastTerm()
	myLastLogIndex := rf.GetLogsLastIndex()

	logUpToDate := false
	if args.LastLogTerm > myLastLogTerm {
		logUpToDate = true
	} else if args.LastLogTerm == myLastLogTerm && args.LastLogIndex >= myLastLogIndex {
		logUpToDate = true
	}

	if !logUpToDate {
		return
	}

	//处理同任期下 已经票投了的情况
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		// 本任期已经投给了其他人，拒绝投票
		return
	}

	//所有的选举通过
	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	DPrintf("[%d] Voted for %d in term %d\n", rf.me, rf.votedFor, rf.currentTerm)
}

func (rf *Raft) ToFollower(Term int) {
	rf.currentTerm = Term
	rf.votedFor = -1
	rf.State = -1
	rf.ElectionTimeout = time.Duration(150+rand.Intn(150)) * time.Millisecond
	DPrintf("[%d] -> Follower, term=%d\n", rf.me, rf.currentTerm)
}

func (rf *Raft) ToLeader() {
	rf.State = 1
	for i := 0; i < len(rf.peers); i++ {
		rf.NextIndex[i] = len(rf.Logs)
		if i == rf.me {
			// Leader 自己的 MatchIndex 应该是最后一个日志索引
			rf.MatchIndex[i] = len(rf.Logs) - 1
		} else {
			rf.MatchIndex[i] = 0
		}
	}
	go rf.HeartbeatLoop()
	DPrintf("[%d] -> Leader, term=%d\n", rf.me, rf.currentTerm)
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()

		var applyMsg raftapi.ApplyMsg
		needApply := false

		if rf.LastApplied < rf.CommitIndex {
			rf.LastApplied++
			applyMsg.CommandValid = true
			applyMsg.Command = rf.Logs[rf.LastApplied].Command
			applyMsg.CommandIndex = rf.LastApplied
			needApply = true
			DPrintf("[%d] Applying log entry %d\n", rf.me, rf.LastApplied)
		}
		rf.mu.Unlock()

		if needApply {
			rf.ApplyCh <- applyMsg
		} else {
			time.Sleep(5 * time.Millisecond)
		}
	}
}

func (rf *Raft) LeaderAppendEntries() {
	rf.mu.Lock()
	if rf.State != 1 || rf.appendInProgress {
		rf.mu.Unlock()
		return
	}
	rf.appendInProgress = true
	rf.mu.Unlock()

	defer func() {
		rf.mu.Lock()
		rf.appendInProgress = false
		rf.mu.Unlock()
	}()

	var cnt atomic.Int32
	cnt.Store(1) // Leader自己算一票
	timeoutCh := time.After(time.Millisecond * 150)

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(i int) {
			needCycle := true
			for needCycle {
				needCycle = false
				rf.mu.Lock()
				if rf.State != 1 {
					rf.mu.Unlock()
					return
				}
				prevLogIndex := rf.NextIndex[i] - 1
				req := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  0,
					Entries:      nil,
					LeaderCommit: rf.CommitIndex,
				}

				if prevLogIndex >= 0 && prevLogIndex < len(rf.Logs) {
					req.PrevLogTerm = rf.Logs[prevLogIndex].Term
				}
				req.Entries = make([]LogEntry, len(rf.Logs[rf.NextIndex[i]:]))
				copy(req.Entries, rf.Logs[rf.NextIndex[i]:])
				rf.mu.Unlock()

				reply := AppendEntriesReply{}
				ok := rf.SendAppendEntries(i, &req, &reply)
				if ok {
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.ToFollower(reply.Term)
						rf.mu.Unlock()
						return
					}
					if rf.State != 1 {
						rf.mu.Unlock()
						return
					}
					if reply.Success {
						cnt.Add(1)
						rf.NextIndex[i] = prevLogIndex + 1 + len(req.Entries)
						rf.MatchIndex[i] = rf.NextIndex[i] - 1
						DPrintf("[%d] Replicated to %d, nextIndex=%d\n", rf.me, i, rf.NextIndex[i])
					} else {
						if rf.NextIndex[i] > 1 {
							rf.NextIndex[i]--
							DPrintf("[%d] NeedCycle to %d\n", rf.me, rf.NextIndex[i])
							// 重新检查状态，确保仍然是leader
							if rf.State == 1 {
								needCycle = true
							}
						}
					}
					rf.mu.Unlock()
				}
			}

		}(i)
	}

	<-timeoutCh

	if cnt.Load() > int32(len(rf.peers)/2) {
		rf.mu.Lock()
		if rf.State != 1 {
			rf.mu.Unlock()
			return
		}
		if rf.State == 1 && rf.CommitIndex < len(rf.Logs)-1 {
			//Debug：不能直接 rf.CommitIndex++
			//得靠match数组
			l := rf.CommitIndex
			r := len(rf.Logs) - 1
			for l <= r {
				mid := (l + r) / 2
				cntt := 0
				for i := 0; i < len(rf.peers); i++ {
					if rf.MatchIndex[i] >= mid {
						cntt++
					}
				}
				if cntt < len(rf.peers)/2 {
					r = mid - 1
				} else {
					l = mid + 1
				}
			}
			//r := rf.CommitIndex
			//for i := rf.CommitIndex ; i <= len(rf.Logs); i++ {
			//	cntt := 0
			//	for j := 0; j < len(rf.peers); j++ {
			//		if rf.MatchIndex[j] >= i {
			//			cntt++
			//		}
			//	}
			//	if cntt > len(rf.peers)/2 {
			//		r = i
			//	}
			//}
			rf.CommitIndex = r
			DPrintf("[%d] Advanced commitIndex to %d\n", rf.me, rf.CommitIndex)
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) HeartbeatLoop() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.State != 1 {
			rf.mu.Unlock()
			return
		}

		req := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.GetLogsLastIndex(),
			PrevLogTerm:  rf.GetLogsLastTerm(),
			Entries:      []LogEntry{},
			LeaderCommit: rf.CommitIndex,
		}
		rf.mu.Unlock()

		DPrintf("[%d] Sending heartbeat, term=%d\n", rf.me, req.Term)

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
						rf.ToFollower(reply.Term)
					}
					rf.mu.Unlock()
				}
			}(i)
		}
		rf.mu.Lock()
		// 只有当有新的日志条目需要复制时才触发LeaderAppendEntries
		hasNewEntries := false
		for j := 0; j < len(rf.peers); j++ {
			if j != rf.me && rf.NextIndex[j] < len(rf.Logs) {
				hasNewEntries = true
				break
			}
		}
		if hasNewEntries {
			go rf.LeaderAppendEntries()
		}
		rf.mu.Unlock()
		time.Sleep(75 * time.Millisecond)
	}
}

func (rf *Raft) ToCandidate() {
	rf.State = 0
	rf.votedFor = rf.me
	rf.currentTerm++
	DPrintf("[%d] -> Candidate, term=%d\n", rf.me, rf.currentTerm)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isLeader := rf.State == 1

	if !isLeader {
		return -1, term, false
	}

	index := len(rf.Logs)
	newLog := LogEntry{
		Command: command,
		Term:    term,
		Index:   index,
	}
	rf.Logs = append(rf.Logs, newLog)
	rf.persist()
	DPrintf("[%d] Start: new command, index=%d, term=%d\n", rf.me, index, term)

	go rf.LeaderAppendEntries() // 由新日志触发AppendEntries
	return index, term, true
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
	for !rf.killed() {
		rf.mu.Lock()
		// 只有Follower和Candidate需要检查选举超时
		if rf.State != 1 {
			// 检查是否超过选举超时时间
			if time.Since(rf.Heartbeat) > rf.ElectionTimeout {
				// 触发新的选举
				rf.mu.Unlock()
				rf.startElection()
				rf.mu.Lock()
			}
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	if rf.State == 1 {
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
	DPrintf("[%d] Starting election, term=%d\n", rf.me, rf.currentTerm)
	rf.mu.Unlock()

	var cnt atomic.Int32
	cnt.Store(1) // 给自己投票
	WinNeed := int32(len(rf.peers) / 2)
	timeoutCh := time.After(time.Duration(500) * time.Millisecond) // 选举过程超时
	resultCh := make(chan bool, 1)

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(i int) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(i, &req, &reply)
			if ok {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.mu.Unlock()
					rf.ToFollower(reply.Term)
					// 通知选举终止
					select {
					case resultCh <- false:
					default:
					}
					return
				}
				// 确保我们仍在同一任期的选举中
				if rf.State != 0 || rf.currentTerm != currentElectionTerm {
					rf.mu.Unlock()
					return // 状态已改变，忽略此回复
				}
				rf.mu.Unlock()

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
			}
		}(i)
	}

	select {
	case won := <-resultCh:
		if won {
			rf.mu.Lock()
			if rf.State == 0 && rf.currentTerm == currentElectionTerm {
				rf.ToLeader()
			}
			rf.mu.Unlock()
		}
	case <-timeoutCh:
		rf.mu.Lock()
		currentTerm := rf.currentTerm
		rf.mu.Unlock()
		DPrintf("[%d] Election timeout, term=%d\n", rf.me, currentTerm)
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
	rf.State = -1
	rf.dead = 0
	rf.LastApplied = 0
	rf.Logs = make([]LogEntry, 1)
	rf.Logs[0] = LogEntry{-1, nil, 0} // dummy entry at index 0
	rf.CommitIndex = 0
	rf.MatchIndex = make([]int, len(rf.peers))
	rf.NextIndex = make([]int, len(rf.peers))
	rf.ApplyCh = applyCh

	rf.Heartbeat = time.Now()
	rf.ElectionTimeout = time.Duration(150+rand.Intn(150)) * time.Millisecond

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	DPrintf("create raft %d\n", me)
	return rf
}
