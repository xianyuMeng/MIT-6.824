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
import "log"
import "fmt"
import "io/ioutil"
import "io"
import "os"

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
	Follower State = iota
	Candidate State = iota
	Leader State = iota
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
	votedFor int
	voteCount int
	logEntry []LogEntry
	
	//volatile state on all servers
	commitIndex int
	lastApplied int

	//volatile state on leaders
	nextIndex []int
	matchIndex []int

	applyMsgch chan ApplyMsg

	logger *log.Logger
	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

func GetLoggerWriter() io.Writer {
	if enable_log := os.Getenv("GOLAB_ENABLE_LOG"); enable_log != "" {
		return os.Stderr
	} else {
		return ioutil.Discard
	}
}
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	
	fmt.Printf("this is %v in peers, getting state...\n current term is %v... is Leader ? %v", rf.me, rf.currentTerm, rf.state)
	rf.mu.Lock()

	term = rf.currentTerm
	isleader = (rf.state == Leader)
	// Your code here.
	//unlock mu until term and isleader are returned
	defer  rf.mu.Unlock()
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
	if(rf.currentTerm > 0) {
		//currentTerm is initialized to 0 on first boot，单调增
		e.Encode(rf.logEntry[rf.currentTerm - 1].log_term)
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
	buf := rf.persister.ReadRaftState()
	if(buf == nil){
		return 
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.logEntry)

}




//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	term int
	candidateId int
	lastLogIndex int
	lastLogTerm int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	term int
	voteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	//当follower变成candidate时，给集群内其他人同时发requestvote RPCs
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	rf.logger.Printf("Got vote request: %v, may grant vote: %v\n", args)
	//Reply false if term < currentTerm
	//If votedFor is null or candidateId, 
	//and candidate’s log is at least as up-to-date as receiver’s log, 
	//grant vote 
	var grantVote bool
	if((rf.votedFor == -1 || rf.votedFor == args.candidateId)){
		if(len(rf.logEntry) > 0){
			if(rf.logEntry[len(rf.logEntry) - 1].log_term == args.lastLogTerm){
				grantVote = true
			}
		}
	}
	rf.logger.Printf("grantVote value is %v\n", grantVote)

	if(args.term < rf.currentTerm){
		reply.voteGranted = false
		reply.term = rf.currentTerm
		return
	}
	if(args.term > rf.currentTerm){
		rf.currentTerm = args.term
		rf.state = Follower
		rf.votedFor = -1
		if(len(rf.logEntry) > 0){
			if(rf.logEntry[len(rf.logEntry)-1].log_term < args.lastLogTerm ){
				rf.votedFor = args.candidateId
				rf.persist()
			}			
		}
		reply.voteGranted = grantVote
		reply.term = rf.currentTerm
		return

	}
	if(args.term == rf.currentTerm){
		rf.votedFor = args.candidateId
		rf.persist()
		reply.voteGranted = grantVote
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
	defer rf.mu.Unlock()
	defer rf.persist()

	var request RequestVoteArgs
	if(len(rf.logEntry) > 0){
		request = RequestVoteArgs{
			term:             rf.currentTerm,
			candidateId:      rf.me,
			lastLogIndex:     len(rf.logEntry) - 1,
			lastLogTerm:      rf.logEntry[len(rf.logEntry) - 1].log_term,
		}
	} else {
		request = RequestVoteArgs{
			term: rf.currentTerm,
			candidateId: rf.me,
			lastLogTerm: 1,
			lastLogIndex: 0,
		}
	}


	var ok bool

	for peer := 0; peer < len(rf.peers); peer += 1 {
		if peer == rf.me {
			continue
		}
		go func(p int) {
			var reply RequestVoteReply
			ok = rf.peers[p].Call("Raft.RequestVote", request, &reply)

			rf.logger.Printf("Call peers : %v\n", ok)

			if ok {
				if(reply.term < rf.currentTerm){
					rf.logger.Printf("Not accept this reply\n")
				}else{
					rf.logger.Printf("other candidate's term is larger, become Follower\n")
					rf.currentTerm = reply.term
					rf.state = Follower
					rf.votedFor = -1
					rf.persist()
				}
				if(reply.voteGranted && rf.state == Candidate){
					rf.voteCount ++
					if(rf.voteCount > (len(rf.peers) / 2 + 1)){
						rf.logger.Printf("Got majority of votes, become Leader\n")
						rf.state = Leader
						for i := 0; i < len(rf.peers); i += 1{
							//for each server, 
							//index of the next log entry to send to that server 
							//(initialized to leader last log index + 1)
							rf.nextIndex[i] = len(rf.logEntry)
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
	term := -1
	isLeader := rf.state == Leader

	if(rf.state != Leader){
		isLeader = false
		return index, term, isLeader
	}else{
		index = len(rf.logEntry)
		term = rf.currentTerm
		tmp := LogEntry{
			log_term: term,
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
	rf.logger = log.New(GetLoggerWriter(), fmt.Sprintf("[Node %v] ", me), log.LstdFlags)
	
	fmt.Printf("Make %v\n", me)
	// Your initialization code here.
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logEntry = make([]LogEntry, 0)

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.applyMsgch = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(rf.persister.ReadRaftState())
	
	return rf
}
