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
	"fmt"
	"math/rand"
	"sync"
	"time"

	"6.824/src/labrpc"
	"code.byted.org/gopkg/logs"
)

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
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

const (
	Leader = iota + 1
	Follower
	Candidate
)

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
	currentTerm    int
	votedFor       int
	commitIndex    int
	lastApplied    int
	nextIndex      map[int]int
	matchIndex     map[int]int
	logs           []LogEntry
	leader         int
	hearbeatNotify chan bool
	status         int
}

type LogEntry struct {
	Command interface{}
	Term    int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	if rf.status == Leader {
		isleader = true
	}
	term = rf.currentTerm
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int //发起选举的人自身的id
	LastLogIndex int //假定Index从1开始
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool //true,代表同意投票
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int //entries前面的一条日志的Index, entries中的Index为PreLogIndex+i
	PrevLogTerm  int
	Entries      []LogEntry
}

type AppendEntriesReply struct {
	Term    int //currentTerm
	Success bool
}

//
// example RequestVote RPC handler.
//
//通过函数名直接指向函数
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		fmt.Printf("reason %v %v\n", args.Term, rf.currentTerm)
		return
	}
	reply.Term = rf.currentTerm
	rf.currentTerm = args.Term //TODO，还要变成follower
	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		if args.LastLogIndex >= len(rf.logs) {
			fmt.Printf(" term %v: %v vote for %v \n", args.Term, rf.me, args.CandidateID)
			rf.votedFor = args.CandidateID
			reply.VoteGranted = true
		} else {
			fmt.Printf("reason:%v\n", rf.votedFor)
			reply.VoteGranted = false
		}
	}
}

// AppendEntries RPC handler
// TODO这里可能有问题
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	defer func() {
		rf.hearbeatNotify <- true
	}()
	reply.Term = rf.currentTerm
	if rf.status == Leader {
		if args.Term > rf.currentTerm {
			fmt.Printf("old leader become follower")
			logs.Debug("%v from %v, currentTerm %v", args.Term, args.LeaderID, rf.currentTerm)
			rf.currentTerm = args.Term
			rf.status = Follower
			reply.Success = false
			return
		}
	}
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	rf.currentTerm = reply.Term
	if len(rf.logs) < args.PrevLogIndex || (len(rf.logs) > 0 && rf.logs[args.PrevLogIndex-1].Term != args.PrevLogTerm) {
		reply.Success = false
		return
	}
	index := args.PrevLogIndex //因为index从1开始的，所以在数组里就代表下一个元素的位置
	appendIndex := 0

	for ; index < len(rf.logs) && appendIndex < len(args.Entries); index++ {
		if rf.logs[index].Term != args.Entries[appendIndex].Term {
			break
		}
		appendIndex++
	}
	rf.logs = rf.logs[:index] //截断
	rf.logs = append(rf.logs, args.Entries[appendIndex:]...)
	rf.commitIndex = min(rf.commitIndex, len(rf.logs))
	reply.Success = true
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//idle和正常的都应该在这里，先写了空闲的
func (rf *Raft) sendAppendEntries() bool {
	args := &AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderID: rf.me,
	}

	isLeader := true
	for i := range rf.peers {
		reply := &AppendEntriesReply{}
		if i == rf.me {
			continue
		}
		go func(i int) {
			ok := rf.peers[i].Call("Raft.AppendEntries", args, reply)
			if ok {
				//收到来自更大的term
				if reply.Term > rf.currentTerm {
					logs.Debug("old leader becom follower")
					logs.Debug("%v from %v, currentTerm %v", reply.Term, i, rf.currentTerm)
					rf.status = Follower //变成follower
					rf.currentTerm = reply.Term
					isLeader = false
				}
			}
		}(i)
	}
	time.Sleep(10 * time.Millisecond) //先用这个替代处理消息的逻辑问题
	return isLeader
}

func (rf *Raft) handleTimeout(me int) {
	rf.currentTerm++ //当前term+1
	args := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateID: me,
	}
	if len(rf.logs) >= 1 {
		args.LastLogTerm = rf.logs[len(rf.logs)-1].Term
		args.LastLogIndex = len(rf.logs) + 1
	}
	//给自己投票
	rf.votedFor = me
	count := 1
	for i := range rf.peers {
		if i == me {
			continue
		}
		reply := &RequestVoteReply{}
		//超时处理正确返回
		go func(i int) {
			//发送给一个disconnect的会存在超时的现象
			//disconnect 发的消息会延迟一段时间才能返回
			ok := rf.sendRequestVote(i, args, reply)
			if ok {
				if reply.VoteGranted == true {
					count++
				} else if reply.VoteGranted == false {
					if rf.currentTerm < reply.Term {
						// rf.currentTerm = reply.Term
					}
				}
			}
		}(i)
	}
	time.Sleep(50 * time.Millisecond) //先用睡眠50ms的方法来做
	if count > len(rf.peers)/2 {
		rf.becomeLeader()
	} else {
		fmt.Printf("%v not requestVote success\n", me)
		rf.votedFor = -1
		rf.status = Follower //变成follower
	}
}

func (rf *Raft) becomeLeader() {
	// pretty.Print(rf)
	rf.status = Leader
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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

//raft逻辑入口从这里开始
func (rf *Raft) server() {
	for {
		status := rf.status
		switch status {
		case Follower:
			rf.serverFollow()
		case Leader:
			rf.serverLeader()
		}
	}
}

func (rf *Raft) serverLeader() {
	//发送一个空的idle
	rf.sendAppendEntries()
	for {
		ticker := time.NewTimer(time.Millisecond * time.Duration(100+rand.Intn(50)))
		select {
		case <-ticker.C:
			logs.Info("%v send heartbeat", rf.me)
			isLeader := rf.sendAppendEntries()
			if !isLeader {
				fmt.Printf("is not have been a leader")
				return
			}
		}
	}
}

func (rf *Raft) serverFollow() {
	for {
		s := GenTimeoutDuration(200, 200)
		logs.Info("%v time Durationn:%v\n", rf.me, s)
		ticker := time.NewTimer(s)
		select {
		case <-ticker.C: //超时了
			logs.Info("%v timeout begin leader election", rf.me)
			rf.handleTimeout(rf.me)  //Fixme:这个函数应该在candidate中处理
			if rf.status == Leader { //自身成为Leader了
				fmt.Printf("%v becomLeader term:%v \n", rf.me, rf.currentTerm)
				return
			}
		case <-rf.hearbeatNotify: //收到来自leader的消息
			fmt.Printf("%v recv heartbeat from leader\n", rf.me)
			ticker.Reset(GenTimeoutDuration(200, 200))
		}
	}
}

func (rf *Raft) serverCandidate() {
	for {
		s := GenTimeoutDuration(200, 200)
		ticker := time.NewTimer(s)
		select {
		case <-ticker.C:
			//超时重新发起投票
			rf.handleTimeout(rf.me)
		}

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
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.votedFor = -1

	// Your initialization code here (2A, 2B, 2C).
	rf.nextIndex = make(map[int]int, len(peers))
	rf.matchIndex = make(map[int]int, len(peers))
	rf.hearbeatNotify = make(chan bool)
	rf.status = Follower

	//非leader的处理逻辑
	//如何接收消息
	go rf.server()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
