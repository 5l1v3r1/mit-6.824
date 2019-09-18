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
	"log"
	"math/rand"
	"sync"
	"time"

	"6.824/src/labrpc"
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
	nextIndex      map[int]int //index从1开始，初始值为len(logs）+1
	matchIndex     map[int]int //初始值为0
	logs           []LogEntry
	leader         int
	hearbeatNotify chan bool
	applyCh        chan ApplyMsg    //业务
	msgCh          chan interface{} //peer的消息通道
	voteMap        map[int]bool     //当前term，peer节点的投票情况
	status         int
	statusCh       chan bool //身份的转变
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
	ReplyID     int //回复者自身的ID
	Term        int
	VoteGranted bool //true,代表同意投票
}

type AppendEntriesArgs struct {
	Term              int
	LeaderID          int
	PrevLogIndex      int //entries前面的一条日志的Index, index从1开始
	PrevLogTerm       int
	Entries           []LogEntry
	LeaderCommitIndex int //LeaderCommitIndex
}

type AppendEntriesReply struct {
	Term       int //currentTerm
	Success    bool
	MatchIndex int
	Id         int //自身的ID

}

type RequestVoteTuple struct {
	args   *RequestVoteArgs
	reply  *RequestVoteReply
	syncCh chan bool
}

//通过函数名直接指向函数
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	tuple := &RequestVoteTuple{
		args:   args,
		reply:  reply,
		syncCh: make(chan bool),
	}
	rf.msgCh <- tuple
	//同步处理逻辑
	<-tuple.syncCh
}

//
// example RequestVote RPC handler.
//
//通过函数名直接指向函数
func (rf *Raft) requestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	reply.ReplyID = rf.me
	//请求者的term小于我的term，直接返回fasle
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		fmt.Printf("reason %v %v\n", args.Term, rf.currentTerm)
		return
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	rf.currentTerm = args.Term
	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		if rf.logUpTodate(args) { //请求者的日志符合条件的时候
			fmt.Printf(" term %v: %v vote for %v \n", args.Term, rf.me, args.CandidateID)
			rf.votedFor = args.CandidateID
			rf.voteMap = make(map[int]bool)
			reply.VoteGranted = true
			//需不需要变成follower?
		}
	}
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	defer func() {
		rf.hearbeatNotify <- true
	}()
	if rf.status != Follower { //身份转变成follower
		if args.Term > rf.currentTerm {
			// logs.Info(" %v term %v converto to follow", rf.me, args.Term)
			rf.meetBigTerm(args.Term)
			rf.statusCh <- true
		}
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	if rf.status == Candidate {
		rf.status = Follower
		rf.statusCh <- true
	}
	if len(rf.logs) < args.PrevLogIndex || (args.PrevLogIndex != 0 && len(rf.logs) > 0 && rf.logs[args.PrevLogIndex-1].Term != args.PrevLogTerm) {
		log.Println("deny request reason %v len %v", args, len(rf.logs))
		reply.Success = false
		return
	}
	if len(args.Entries) != 0 {
		index := args.PrevLogIndex //因为是从1快开始的，下一个元素的位置
		appendIndex := 0

		for ; index < len(rf.logs) && appendIndex < len(args.Entries); index++ {
			if rf.logs[index].Term != args.Entries[appendIndex].Term {
				break
			}
			appendIndex++
		}
		rf.logs = rf.logs[:index] //截断
		rf.logs = append(rf.logs, args.Entries[appendIndex:]...)
	}
	curCommitIndex := min(args.LeaderCommitIndex, len(rf.logs))
	//这个判断条件防止 follower由于收到心跳 提前提交不在当前term内的日志
	if len(rf.logs) > 0 && rf.logs[len(rf.logs)-1].Term == args.Term {
		go func() {
			for i := rf.commitIndex + 1; i <= curCommitIndex; i++ {
				msg := ApplyMsg{
					CommandValid: true,
					Command:      rf.logs[i-1].Command,
					CommandIndex: i,
				}
				// logs.Info("follower apply %v", msg)
				rf.applyCh <- msg
			}
			rf.commitIndex = curCommitIndex
		}()
	}
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

//发送数据，心跳也在这里
func (rf *Raft) sendAppendEntries() bool {

	isLeader := true
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			reply := &AppendEntriesReply{}
			args := &AppendEntriesArgs{
				Term:              rf.currentTerm,
				LeaderID:          rf.me,
				PrevLogIndex:      0,
				PrevLogTerm:       0,
				LeaderCommitIndex: rf.commitIndex,
			}

			//if last logindex >= nextIndex
			//需要发送数据
			//后面一个条件是用来保证leader只提交当前term的日志
			if len(rf.logs) >= rf.nextIndex[i] && rf.logs[len(rf.logs)-1].Term == rf.currentTerm {
				args.PrevLogIndex = rf.nextIndex[i] - 1
				//后面那个条件是为了应对身份转变时，并发修改对象的logs和PrevLogIndex
				if args.PrevLogIndex > 0 && args.PrevLogIndex-1 < len(rf.logs) {
					args.PrevLogTerm = rf.logs[args.PrevLogIndex-1].Term
				}
				args.Entries = append(args.Entries, rf.logs[args.PrevLogIndex:]...)
			}

			// //感觉这里写的不够优雅
			// args.PrevLogIndex = rf.nextIndex[i] - 1
			// if len(rf.logs) > 0 {
			// 	//后面那个条件是为了应对身份转变时，并发修改对象的logs和PrevLogIndex
			// 	//猜测是go func运行时才取值的
			// 	if args.PrevLogIndex > 0 && args.PrevLogIndex-1 < len(rf.logs) {
			// 		args.PrevLogTerm = rf.logs[args.PrevLogIndex-1].Term
			// 	}
			// 	if args.PrevLogIndex < len(rf.logs) {
			// 		args.Entries = append(args.Entries, rf.logs[args.PrevLogIndex])
			// 	}
			// 	args.LeaderCommitIndex = rf.commitIndex
			// }
			ok := rf.peers[i].Call("Raft.AppendEntries", args, reply)
			//在这里加上
			//这里是否有必要在reply中加上matchIndex?
			if ok {
				if len(args.Entries) > 0 {
					reply.MatchIndex = args.PrevLogIndex + len(args.Entries)
				}
				reply.Id = i
				rf.msgCh <- reply
			}
		}(i)
	}
	return isLeader
}

func (rf *Raft) handleTimeout() {
	rf.voteMap = make(map[int]bool)
	rf.currentTerm++ //当前term+1
	args := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateID: rf.me,
	}
	if len(rf.logs) >= 1 {
		args.LastLogTerm = rf.logs[len(rf.logs)-1].Term
		args.LastLogIndex = len(rf.logs) //这里以前为什么要+1，先改成不加1
	}
	//给自己投票
	rf.votedFor = rf.me
	rf.voteMap[rf.me] = true
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		reply := &RequestVoteReply{}
		go func(i int) {
			ok := rf.sendRequestVote(i, args, reply)
			if ok {
				rf.msgCh <- reply
			}
		}(i)
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
	if rf.status != Leader {
		return 0, 0, false
	}

	index = len(rf.logs) + 1
	term = rf.currentTerm
	//插入到日志中去，向follower发送消息
	rf.logs = append(rf.logs, LogEntry{
		Command: command,
		Term:    rf.currentTerm,
	})
	rf.matchIndex[rf.me] = len(rf.logs)

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
		case Candidate:
			rf.serverCandidate()
		}
	}
}

func (rf *Raft) serverLeader() {
	rf.initLeader()
	//发送一个空的idle
	rf.sendAppendEntries()
	ticker := time.NewTimer(time.Millisecond * time.Duration(50+rand.Intn(50)))
	for {
		select {
		case <-rf.statusCh:
			if rf.status != Leader {
				// logs.Info("leader %v become follower", rf.me)
				return
			}
		case <-ticker.C:
			// logs.Info("%v send heartbeat term %v", rf.me, rf.currentTerm)
			rf.sendAppendEntries()
			ticker.Reset(GenTimeoutDuration(50, 50))
		case val := <-rf.msgCh:
			//收到来自peer的消息
			switch peerMsg := val.(type) {
			case *RequestVoteTuple:
				if peerMsg.args.Term > rf.currentTerm {
					rf.meetBigTerm(peerMsg.args.Term)
					rf.statusCh <- true
				}
				rf.requestVote(peerMsg.args, peerMsg.reply)
				close(peerMsg.syncCh)
			case *AppendEntriesReply: //发送消息的回报，心跳也在这里处理
				if peerMsg.Term > rf.currentTerm {
					// logs.Info("%v leader become follower bigTerm:%v", rf.me, peerMsg.Term)
					rf.meetBigTerm(peerMsg.Term)
					return
				}
				if peerMsg.Success == false {
					log.Println("peerMsg deny %v", peerMsg)
					rf.nextIndex[peerMsg.Id]--
				} else {
					//返回的消息index大于上次发送的index
					if peerMsg.MatchIndex > rf.matchIndex[peerMsg.Id] {
						rf.nextIndex[peerMsg.Id] = peerMsg.MatchIndex + 1
						rf.matchIndex[peerMsg.Id] = peerMsg.MatchIndex
						//查询当前match
					}
					curCommitIndex := pollCommitIndex(rf.matchIndex)
					go func(index int) {
						for i := index + 1; i <= curCommitIndex; i++ {
							msg := ApplyMsg{
								CommandValid: true,
								Command:      rf.logs[i-1].Command,
								CommandIndex: i,
							}
							// logs.Warn("Leader send apply %v", msg)
							rf.applyCh <- msg
						}
					}(rf.commitIndex)
					rf.commitIndex = curCommitIndex
					// logs.Info("leader %v  term %vcommit Index%v %v  %v len=%v ", rf.me, rf.currentTerm, rf.commitIndex, rf.matchIndex, rf.nextIndex, len(rf.logs))
				}
			default:
				// logs.Info("unkownType %v", val)
			}
		}
	}
}

func (rf *Raft) serverFollow() {
	ticker := time.NewTimer(GenTimeoutDuration(200, 200))
	for {
		select {
		case <-ticker.C: //超时了
			// logs.Info("%v timeout", rf.me)
			rf.status = Candidate
			return
		case <-rf.hearbeatNotify: //收到来自leader的消息
			// logs.Info("%v recv heartbeat from leader\n", rf.me)
			ticker.Reset(GenTimeoutDuration(200, 200))
		case val := <-rf.msgCh:
			//收到来自peer的消息
			switch peerMsg := val.(type) {
			case *RequestVoteTuple:
				if peerMsg.args.Term > rf.currentTerm {
					rf.meetBigTerm((peerMsg.args.Term))
				}
				rf.requestVote(peerMsg.args, peerMsg.reply)
				close(peerMsg.syncCh)
			default:
				logs.Println("unkownType %v", val)
			}
		}
	}
}

func (rf *Raft) serverCandidate() {
	ticker := time.NewTimer(GenTimeoutDuration(200, 200))
	rf.handleTimeout()
	for {
		select {
		case <-rf.statusCh:
			if rf.status != Candidate {
				// logs.Info(" %v status convert to %v ", rf.me, rf.status)
				return
			}
		case <-ticker.C:
			//超时重新发起投票
			// logs.Info("%v timeout term %v", rf.me, rf.currentTerm)
			rf.handleTimeout()
			ticker.Reset(GenTimeoutDuration(200, 200))
		case val := <-rf.msgCh:
			//收到来自peer的消息
			switch peerMsg := val.(type) {
			case *RequestVoteReply:
				// logs.Info("%v %v recv peer message %v ", rf.me, rf.currentTerm, *peerMsg)
				if peerMsg.Term > rf.currentTerm {
					rf.currentTerm = peerMsg.Term
					rf.votedFor = -1
					rf.voteMap = make(map[int]bool)
					rf.status = Follower
					return
				}
				rf.voteMap[peerMsg.ReplyID] = peerMsg.VoteGranted
				if poll(rf.voteMap, len(rf.peers)) {
					rf.status = Leader
					return
				}
			case *RequestVoteTuple:
				if peerMsg.args.Term > rf.currentTerm {
					rf.meetBigTerm((peerMsg.args.Term))
					rf.statusCh <- true
				}
				rf.requestVote(peerMsg.args, peerMsg.reply)
				close(peerMsg.syncCh)
			default:
				log.Println("unkownType %v", val)
			}
		}

	}
}

//判断请求参数中的日志是否更新
func (rf *Raft) logUpTodate(args *RequestVoteArgs) bool {
	if len(rf.logs) == 0 {
		return true
	}
	if args.LastLogTerm > rf.logs[len(rf.logs)-1].Term {
		return true
	}

	//最后一条日志相同term相同的
	if args.LastLogTerm == rf.logs[len(rf.logs)-1].Term {
		if args.LastLogIndex >= len(rf.logs) {
			return true
		}
	}
	return false
}

//成为leader时调用的函数
//不应该同步不是该term内的数据？
func (rf *Raft) initLeader() {
	log.Println("%v become Leader", rf.me)
	for k, _ := range rf.peers {
		rf.nextIndex[k] = len(rf.logs) + 1
		rf.matchIndex[k] = 0
	}
	rf.matchIndex[rf.me] = len(rf.logs)
}

//收到更大的term请求/回应的时候, 身份转变成follower
func (rf *Raft) meetBigTerm(bigTerm int) {
	rf.currentTerm = bigTerm
	rf.votedFor = -1
	rf.voteMap = make(map[int]bool)
	rf.status = Follower
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
	rf.hearbeatNotify = make(chan bool, 1)
	rf.statusCh = make(chan bool, 1)
	rf.status = Follower
	rf.applyCh = applyCh
	rf.msgCh = make(chan interface{}, 5)
	rf.voteMap = make(map[int]bool)

	//非leader的处理逻辑
	//如何接收消息
	go rf.server()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
