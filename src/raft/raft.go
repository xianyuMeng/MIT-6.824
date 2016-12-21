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
import "time"
import "math/rand"
import "fmt"
// import "bytes"
// import "encoding/gob"



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
type LogEntry struct {
    Command interface{}
    //command to be executed by the replicated state machine
    Term int
    Index int
}
type State int

//Within a constant declaration, the predeclared identifier iota represents successive untyped integer constants. It is reset to 0 whenever the reserved word const appears in the source and increments after each ConstSpec. It can be used to construct a set of related constants:
//just like enum-type in C++
const (
    Follower  State = iota
    Candidate State = iota
    Leader    State = iota
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
    mu        sync.Mutex
    peers     []*labrpc.ClientEnd
    persister *Persister
    me        int // index into peers[]
    
    // Your data here.
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
    state State
    
    // Persistent state.
    currentTerm int
    votedFor int
    logs []LogEntry
    
    // Volatile states.
    commitIndex int
    lastApplied int
    
    // Volatile states for leader.
    nextIndex []int
    matchIndex []int
    
    applyCh chan ApplyMsg
    
    lastTime time.Time // Last contact from leader.
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

return rf.currentTerm, rf.state == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//

// Helper function to print debug info for this server.
func (rf *Raft) debug(format string, a ...interface{}) (n int, err error) {

    a = append(a, 0, 0, 0)
    copy(a[3:], a[0:])
    a[0] = rf.me
    a[1] = rf.currentTerm
    a[2] = &rf.state
    n, err = DPrintf("Raft %v Term %v as %v " + format, a...)
    return
}
func (rf *Raft) persist() {
    // Your code here.
    // Example:
    // w := new(bytes.Buffer)
    // e := gob.NewEncoder(w)
    // e.Encode(rf.xxx)
    // e.Encode(rf.yyy)
    // data := w.Bytes()
    // rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
    // Your code here.
    // Example:
    // r := bytes.NewBuffer(data)
    // d := gob.NewDecoder(r)
    // d.Decode(&rf.xxx)
    // d.Decode(&rf.yyy)
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
    // Your code here.
    rf.mu.Lock()
    fmt.Printf("this is %v... got request from %v\n", rf.me, args.CandidateId)
    defer rf.persist()
    defer rf.mu.Unlock()


    //Reply false if term < currentTerm
    //If votedFor is null or candidateId,
    //and candidate’s log is at least as up-to-date as receiver’s log,
    //grant vote
    grantVote := true

    fmt.Printf("%v got request from %v...grantVote value is %v\n", rf.me, args.CandidateId, grantVote)

    if args.Term < rf.currentTerm {
        reply.VoteGranted = false
        reply.Term = rf.currentTerm
        fmt.Printf("candidate term is %v, current term is %v, grantVote is false because rf term is ahead\n", args.Term, rf.currentTerm)
        return
    }
    
    // if args.Term > rf.currentTerm
    // change back to follower and update rf.currentTerm
    if args.Term > rf.currentTerm {
        rf.currentTerm = args.Term
        rf.state = Follower
        rf.votedFor = -1
        rf.lastTime = time.Now()
    }

    // Check if the candidate's log is update.
    // If so, check votedFor == -1 || votedFor == candidateId
    // grantVote or not.
    // Definition of "Update": 
    // if myLastLogTerm == candidateLastTerm : 
    //   myLastLogIndex <= candidateLastIndex 
    // else : 
    //   myLastLogTerm < candidateLastTerm
    if rf.logs[len(rf.logs) - 1].Term < args.LastLogTerm {
        if rf.votedFor == -1 || rf.votedFor ==args.CandidateId {
            reply.VoteGranted = true
            reply.Term = rf.currentTerm
            return
        }
    } else if rf.logs[len(rf.logs) - 1].Term == args.LastLogTerm {
        if rf.logs[len(rf.logs) - 1].Index <= args.LastLogIndex {
            // Check votedFor?
            if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
                reply.VoteGranted = true
                reply.Term = rf.currentTerm
                return                
            }
        }
    }
    // reject the vote.
    reply.VoteGranted = false
    reply.Term = rf.currentTerm
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
    ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
    return ok
}

func (rf *Raft)sendAppendEntry(server int, args AppendEntryArgs, reply * AppendEntryReply) bool {
    ok := rf.peers[server].Call("Raft.RequestAppendEntry", args, reply)
    return ok
}

func (rf *Raft)RequestAppendEntry(args AppendEntryArgs, reply *AppendEntryReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    // Check if args.Term < rf.currentTerm
    // Reject append.
    if args.Term < rf.currentTerm{
        reply.Term = rf.currentTerm
        reply.Success = false
    }else{
        rf.lastTime = time.Now()
        reply.Success = true
    }
}
func (rf *Raft) electionTimer() {
    for {
        sleepTime := time.Duration(rand.Intn(150) + 150) * time.Millisecond
        time.Sleep(sleepTime)
        now := time.Now()
        // We start election when:
        // 1. now is after lastTime + sleepTime AND
        // 2. I am not leader.
        // To prevent others from modifying lastTime
        rf.mu.Lock()
        if now.After(rf.lastTime.Add(sleepTime)) && rf.state != Leader {
            go rf.elect()
        }
        rf.mu.Unlock()
    }
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
    // Your data here.
    Term         int
    CandidateId  int
    LastLogIndex int
    LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
    // Your data here.
    Term        int
    VoteGranted bool
}

type AppendEntryArgs struct{
    Term int
    LeaderId int
    PrevLogIndex int
    PrevLogTerm int
    Entries []LogEntry
    LeaderCommit int
}

type AppendEntryReply struct{
    Term int
    Success bool
}

// Send vote request to every one.
// Wait for 100ms for reply, if no reply, time out.
// Collect votes and see if we can be leader.
func (rf *Raft) elect() {
    
    rf.mu.Lock()
    rf.currentTerm++
    rf.votedFor = rf.me
    rf.state = Candidate
    
    rf.lastTime = time.Now()
    
    request := RequestVoteArgs{
        Term            : rf.currentTerm,
        CandidateId     : rf.me,
        LastLogIndex    : len(rf.logs) - 1,
        LastLogTerm     : rf.logs[len(rf.logs)-1].Term,
    }
    
    rf.mu.Unlock()
    
    voteCh := make(chan bool)
    
    for peer := 0; peer < len(rf.peers); peer++ {
        if peer == rf.me {
            continue
        }
        // We want a 100ms timeout for request vote.
        // We use channel to do it.
        // go func() {
        //     ch := make(chan bool)
        //     reply := Reply
        //     go func() {
        //         ch <- whatever(&reply)
        //     }
        //     select {
        //         case ok := <- ch:
        //         case time.After(100 * time.Millisecond):
        //             // Timeout.
        //     }
        // }
        go func(p int) {
            var reply RequestVoteReply
            okchan := make(chan bool)
            go func() {
                okchan <- rf.sendRequestVote(p, request, &reply)
            }()
            select {
                case ok := <- okchan :
                    if !ok {
                        voteCh <- false
                    } else {
                        // We now have the reply.
                        // We have to check reply.Term > rf.currentTerm
                        if rf.currentTerm < reply.Term {
                            rf.mu.Lock()
                            rf.state = Follower
                            rf.currentTerm = reply.Term
                            rf.votedFor = -1
                            rf.lastTime = time.Now()
                            rf.mu.Unlock()
                        }
                        voteCh <- reply.VoteGranted
                        
                    }
                case <- time.After(100 * time.Millisecond) :
                    voteCh <- false
                          
            }
        }(peer)
        
    }
    
    voteCount := 1
    for i:= 0; i < len(rf.peers) - 1; i++ {
        v := <- voteCh
        if v {
            voteCount++
        }
    }
    
    fmt.Printf("this is %v having %v votes\n", rf.me, voteCount)
    if voteCount > (len(rf.peers) / 2 ) && rf.state == Candidate && rf.currentTerm == request.Term {
        rf.mu.Lock()
        rf.state = Leader
        for i := 0; i < len(rf.peers); i++ {
            rf.nextIndex[i] = rf.logs[len(rf.logs) - 1].Index + 1
            rf.matchIndex[i] = 0
        }
        go rf.sendHeartBeat()
        rf.mu.Unlock()
    }
}

func (rf *Raft) sendHeartBeat() {
    for{
        // Fill in the args.
        heartbeat := AppendEntryArgs{
        Term: rf.currentTerm,
        LeaderId: rf.me,
        }
        for peer := 0; peer < len(rf.peers); peer++{
            go func (p int){
                var reply AppendEntryReply
                okchan := make(chan bool)
                go func(){
                    okchan <- rf.sendAppendEntry(p, heartbeat, &reply)
                }()
                select{
                    case  <- okchan :

                    case <- time.After(100 * time.Millisecond):
                }
            } (peer)
        }

        time.Sleep(time.Duration(100 * time.Millisecond))
    }
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
    index := -1
    term := -1
    isLeader := true


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

    // Your initialization code here.
    rf.state = Follower
    rf.votedFor = -1
    rf.currentTerm = 0
    rf.logs = make([]LogEntry, 1)
    rf.logs[0].Index = 0
    rf.logs[0].Term = 0
    rf.commitIndex = 0
    rf.lastApplied = 0
    rf.nextIndex = make([]int, len(rf.peers))
    rf.matchIndex = make([]int, len(rf.peers))
    rf.applyCh = applyCh
    rf.lastTime = time.Now()
    // initialize from state persisted before a crash
    rf.readPersist(persister.ReadRaftState())
    
    go rf.electionTimer()
    return rf
}
