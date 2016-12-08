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

import "sync"
import "labrpc"

import "bytes"
import "encoding/gob"
import "fmt"

import "time"
import "math/rand"

const (
	//Raft uses randomized election timeouts to ensure that
	//split votes are rare and that they are resolved quickly.
	//To prevent split votes in the first place,
	//election timeouts are chosen randomly from a fixed interval
	//(e.g., 150–300ms)
	MAX_TIMEOUT = 300 * time.Millisecond
	MIN_TIMEOUT = 150 * time.Millisecond
	HEARTBEATS  = 50 * time.Millisecond
)

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

type State int

//Within a constant declaration, the predeclared identifier iota represents successive untyped integer constants. It is reset to 0 whenever the reserved word const appears in the source and increments after each ConstSpec. It can be used to construct a set of related constants:
//just like enum-type in C++
const (
	Follower  State = iota
	Candidate State = iota
	Leader    State = iota
)

type LogEntry struct {
	log_command interface{}
	//command to be executed by the replicated state machine
	log_term int
}

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	//current state
	state State

	//persistent state on all servers
	currentTerm int
	votedFor    int
	voteCount   int
	logEntry    []LogEntry

	//volatile state on all servers
	commitIndex int
	lastApplied int

	//volatile state on leaders
	nextIndex  []int
	matchIndex []int

	applyMsgch chan ApplyMsg

	timer *time.Timer

	//if rf is a follower, it will get heartbeat in every heartbeat_inteval
	heartbeat   chan bool
	isleader    chan bool
	iscandidate chan bool
	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	fmt.Printf("this is %v in peers, getting state...\n current term is %v... state is  %v\n", rf.me, rf.currentTerm, rf.state)
	rf.mu.Lock()

	term = rf.currentTerm
	isleader = (rf.state == Leader)
	// Your code here.
	//unlock mu until term and isleader are returned
	defer rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logEntry)
	if rf.currentTerm > 0 {
		//currentTerm is initialized to 0 on first boot，单调增
		e.Encode(rf.logEntry)
	}

	data := w.Bytes()
	rf.persister.SaveRaftState(data)

}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.logEntry)

}

func (rf *Raft) RandomizeTimeout() {
	if rf.timer == nil {
		rf.timer = time.NewTimer(time.Hour)
	}
	if !rf.timer.Stop() {
		<-rf.timer.C
	}

	var timeout int64
	if rf.state != Leader {
		timeout = random(int64(MIN_TIMEOUT), int64(MAX_TIMEOUT))
	}
	fmt.Printf("the time out is %v\n", timeout)
	rf.timer.Reset(time.Duration(timeout))
}

func random(min int64, max int64) int64 {
	rand.Seed(time.Now().Unix())
	return (rand.Int63n(max - min)) + min
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	term         int
	candidateId  int
	lastLogIndex int
	lastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	term        int
	voteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	//当follower变成candidate时，给集群内其他人同时发requestvote RPCs
	rf.mu.Lock()
	fmt.Printf("this is %v... got request from %v\n", rf.me, args.candidateId)
	defer rf.persist()
	defer rf.mu.Unlock()


	//Reply false if term < currentTerm
	//If votedFor is null or candidateId,
	//and candidate’s log is at least as up-to-date as receiver’s log,
	//grant vote
	var grantVote bool
	if len(rf.logEntry) > 0 {
		if rf.logEntry[len(rf.logEntry)-1].log_term > args.term ||
			(rf.logEntry[len(rf.logEntry)-1].log_term == args.lastLogTerm && len(rf.logEntry) > args.lastLogIndex) {
			grantVote = false
		}
	}
	fmt.Printf("%v reject %v...grantVote value is %v\n", rf.me, args.candidateId, grantVote)

	if args.term < rf.currentTerm {
		reply.voteGranted = false
		reply.term = rf.currentTerm
		fmt.Printf("candidate term is %v, current term is %v, grantVote is false because rf term is ahead\n", args.term, rf.currentTerm)
		return
	}
	if args.term > rf.currentTerm {
		rf.currentTerm = args.term
		rf.state = Follower
		rf.isleader <- false
		rf.votedFor = args.candidateId
		if len(rf.logEntry) > 0 {
			if rf.logEntry[len(rf.logEntry)-1].log_term < args.lastLogTerm {
				rf.votedFor = args.candidateId
				rf.persist()
			}
		}
		reply.voteGranted = grantVote
		reply.term = rf.currentTerm
		return

	}
	if args.term == rf.currentTerm {
		rf.votedFor = args.candidateId
		rf.persist()
		reply.voteGranted = rf.votedFor == args.candidateId
		reply.term = rf.currentTerm
		return
	}

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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	rf.mu.Lock()

	var request RequestVoteArgs
	if len(rf.logEntry) > 0 {
		request = RequestVoteArgs{
			term:         rf.currentTerm,
			candidateId:  rf.me,
			lastLogIndex: len(rf.logEntry) - 1,
			lastLogTerm:  rf.logEntry[len(rf.logEntry)-1].log_term,
		}
	} else {
		request = RequestVoteArgs{
			term:         rf.currentTerm,
			candidateId:  rf.me,
			lastLogTerm:  1,
			lastLogIndex: 0,
		}
	}
	fmt.Printf("this is %v Sending requestVote RPCs ...term is %v\n", rf.me, rf.currentTerm)

	rf.persist()
	rf.mu.Unlock()
	/*
		candidate 的状态在以下终止
		1, 赢得选举：在一个term内赢得多数票
		2, 另一个candidate宣布自己当选（收到来自新leader的AppendEntriesRPC 和 hearbeat）
			(1)如果新leader的term 比自己大， 承认新leader的地位
			(2)反之，拒绝并继续当candidate
		3, 一段时间过后没有赢家
	*/
	var ok bool

	for peer := 0; peer < len(rf.peers); peer = peer + 1 {
		if peer == rf.me {
			continue
		}
		go func(p int) {
			var reply RequestVoteReply
			fmt.Printf("candidate %v term is %v...Now sending requestVote \n", rf.me, request.term)
			ok = rf.peers[p].Call("Raft.RequestVote", request, &reply)

			fmt.Printf("Call peers : %v\n", ok)

			if ok {
				if reply.term < rf.currentTerm {
					fmt.Printf("Not accept this reply\n")
				} else {
					fmt.Printf("other candidate's term is larger, become Follower\n")
					rf.currentTerm = reply.term
					rf.state = Follower
					rf.votedFor = -1
					rf.persist()
				}
				if reply.voteGranted && rf.state == Candidate {
					rf.voteCount++
					fmt.Printf("this is %v...having %v votes", rf.me, rf.voteCount)
					if rf.voteCount > (len(rf.peers)/2 + 1) {
						fmt.Printf("Got majority of votes, become Leader\n")
						rf.isleader <- true
						rf.state = Leader
						for i := 0; i < len(rf.peers); i += 1 {
							//for each server,
							//index of the next log entry to send to that server
							//(initialized to leader last log index + 1)
							rf.nextIndex[i] = len(rf.logEntry) - 1
							//for each server, index of highest log entry known to be replicated on server
							//(initialized to 0, increases monotonically)
							rf.matchIndex[i] = 0
						}
					}
				}
			}
		}(peer)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.currentTerm
	isLeader := rf.state == Leader
	fmt.Printf("this is %v ... Start \n", rf.me)
	if isLeader {
		index = len(rf.logEntry)
		term = rf.currentTerm
		tmp := LogEntry{
			log_term:    term,
			log_command: command,
		}
		rf.logEntry = append(rf.logEntry, tmp)
		rf.persist()
	}

	//first index is 1
	return index + 1, term, isLeader
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
	rf.me = me

	fmt.Printf("Make %v\n", me)
	// Your initialization code here.
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.voteCount = 0
	rf.logEntry = append(rf.logEntry, LogEntry{log_term: 0})
	//rf.hearbeat <- false

	rf.nextIndex = make([]int, len(peers)+1)
	rf.matchIndex = make([]int, len(peers)+1)
	rf.applyMsgch = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(rf.persister.ReadRaftState())
	//rf.RandomizeTimeout()

	go func() {
		for {
			switch rf.state {
			case Follower:
				select {
				case <-rf.heartbeat:
				//if follower gets heartbeat, indicates that the leader works fine
				case <-time.After(HEARTBEATS):
					{
						rf.state = Candidate
						fmt.Printf("follower become candidate\n")
					}

				}
			case Leader:
				{
					//time.Sleep(HEARTBEATS)
				}
			case Candidate:
				{

					/*
						follower成为candidate之后
						1, increaments its current term
						2, change to candidate state
						3, 投票给自己
						4, 给集群内其他人同时issue RequestVote RPCs
					*/
					rf.mu.Lock()
					rf.currentTerm++
					rf.votedFor = rf.me
					rf.voteCount = 1
					rf.persist()
					rf.mu.Unlock()
					fmt.Printf("this is %v...current term is %v...having %v votes\n", rf.me, rf.currentTerm, rf.voteCount)
					request := RequestVoteArgs{
						term:        rf.currentTerm,
						candidateId: rf.me,
					}
					if len(rf.logEntry) > 0 {
						request.lastLogIndex = len(rf.logEntry) - 1
						request.lastLogTerm = rf.logEntry[len(rf.logEntry)-1].log_term
					} else {
						request.lastLogIndex = 0
						request.lastLogTerm = 1
					}

					var reply RequestVoteReply
					rf.sendRequestVote(rf.me, request, &reply)
					select {
					case <-rf.heartbeat:
						rf.state = Follower
					case <-rf.isleader:
					}
				}
			}
		}
	}()

	return rf
}
