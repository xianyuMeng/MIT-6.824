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
import "bytes"
import "encoding/gob"

type SnapshotArgs struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Data             []byte
}

type SnapshotReply struct {
    ReplyTerm int
}
func (rf *Raft) GetStateSize() int {
	return rf.persister.RaftStateSize()
}
func (rf *Raft) InstallSnapshot(shot SnapshotArgs, reply *SnapshotReply) {
	//rf.debug("been installing...LOCK\n")
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//rf.debug("been installing...\n")
	if shot.Term < rf.currentTerm {
		reply.ReplyTerm = rf.currentTerm
		return
	} 
    if shot.Term > rf.currentTerm {
        rf.votedFor = -1
    }
    rf.currentTerm = shot.Term
    rf.state = Follower
    rf.lastTime = time.Now()

	rf.commitIndex = shot.LastIncludeIndex
	newlogs := make([]LogEntry, 1)
	newlogs[0] = LogEntry{
		Index: shot.LastIncludeIndex,
		Term:  shot.LastIncludeTerm,
	}
	pos := shot.LastIncludeIndex - FirstIndex(rf.logs)
	if pos >= 0 && pos < len(rf.logs) && rf.logs[pos].Term == shot.LastIncludeTerm {
		newlogs = append(newlogs, rf.logs[pos+1:]...)
	}
	rf.logs = newlogs
	rf.persister.SaveSnapshot(shot.Data)
    rf.persist()

	applych := ApplyMsg{
		UseSnapshot: true,
		Snapshot:    shot.Data,
	}
	rf.applyCh <- applych
	reply.ReplyTerm = rf.currentTerm
	rf.debug("done\n")
	return
}

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
	Term  int
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
	votedFor    int
	logs        []LogEntry

	// Volatile states.
	commitIndex int
	lastApplied int

	// Volatile states for leader.
	nextIndex  []int
	matchIndex []int

	working bool
	applyCh chan ApplyMsg

	lastTime time.Time // Last contact from leader.
}

func (rf *Raft) Snapshot(maps []byte, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	baseindex := FirstIndex(rf.logs)
	if index <= baseindex || index > LastIndex(rf.logs) {
		//index is not in the current logs
        rf.debug("WTF...snapshot\n")
		return
	}

	newlogs := make([]LogEntry, 0)
	newlogs = append(newlogs, rf.logs[index - FirstIndex(rf.logs):]...)
	rf.logs = newlogs
	rf.persist()

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(FirstIndex(rf.logs))
	e.Encode(FirstTerm(rf.logs))
	data := w.Bytes()
	data = append(data, maps...)
	rf.persister.SaveSnapshot(data)
}

func (rf *Raft) Readsnapshot(data []byte) {
	if len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	// markClient = make(map[int64]int, 0)
	// markRequest = make(map[string]string, 0)
	var lastIncludeIndex int
	var lastIncludeTerm int

	d.Decode(&lastIncludeIndex)
	d.Decode(&lastIncludeTerm)
	// d.Decode(&markClient)
	// d.Decode(&markRequest)
	rf.commitIndex = lastIncludeIndex
	newlogs := make([]LogEntry, 1)
	newlogs[0] = LogEntry{
		Index: lastIncludeIndex,
		Term:  lastIncludeTerm,
	}
	pos := lastIncludeIndex - FirstIndex(rf.logs)
	if pos >= 0 && pos < len(rf.logs) && rf.logs[pos].Term == lastIncludeTerm {
		newlogs = append(newlogs, rf.logs[pos+1:]...)
	}
	rf.logs = newlogs
	applych := ApplyMsg{
		UseSnapshot: true,
		Snapshot:    data,
	}
	go func() {
		rf.applyCh <- applych
	}()

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.state == Leader
}

func LastIndex(logs []LogEntry) int {
	return logs[len(logs)-1].Index
}

func LastTerm(logs []LogEntry) int {
	return logs[len(logs)-1].Term
}

func FirstIndex(logs []LogEntry) int {
	return logs[0].Index
}

func FirstTerm(logs []LogEntry) int {
	return logs[0].Term
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
	a[2] = rf.state
	n, err = DPrintf("Raft %v Term %v as %v "+format, a...)
	return
}

func (rf *Raft) printLogs() {
	logs := fmt.Sprintf("Base %v ", FirstIndex(rf.logs))
	for i := 0; i < len(rf.logs); i++ {
		if rf.logs[i].Index == rf.commitIndex {
			logs = fmt.Sprintf("%v%v|", logs, rf.logs[i].Term)
		} else {
			logs = fmt.Sprintf("%v%v ", logs, rf.logs[i].Term)
		}
	}
	rf.debug(logs + "\n")
}

func (rf *Raft) persist() {
	// Your code here.
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	if len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)

	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.logs)
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	//rf.debug("got request from %v\n", args.CandidateId)
	rf.mu.Lock()

	defer func() {
		rf.persist()
		rf.mu.Unlock()
	}()

	//Reply false if term < currentTerm
	//If votedFor is null or candidateId,
	//and candidate’s log is at least as up-to-date as receiver’s log,
	//grant vote

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		//rf.debug("candidate term is %v, current term is %v, grantVote is false because rf term is ahead\n", args.Term, rf.currentTerm)
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

	reply.Term = rf.currentTerm

	// Check if the candidate's log is update.
	// If so, check votedFor == -1 || votedFor == candidateId
	// grantVote or not.
	// Definition of "Update":
	// if myLastLogTerm == candidateLastTerm :
	//   myLastLogIndex <= candidateLastIndex
	// else :
	//   myLastLogTerm < candidateLastTerm
	// isUpToDate := func(term int, index int) bool {
	//     myTerm := LastTerm(rf.logs)
	//     myIndex := LastIndex(rf.logs)
	//     if myTerm == term {
	//         return myIndex <= index
	//     }
	//     return myTerm < term
	// }

	if LastTerm(rf.logs) < args.LastLogTerm {
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			return
		}
	} else if LastTerm(rf.logs) == args.LastLogTerm {
		if LastIndex(rf.logs) <= args.LastLogIndex {
			// Check votedFor?
			if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
				rf.votedFor = args.CandidateId
				reply.VoteGranted = true
				return
			}
		}
	}
	// reject the vote.
	reply.VoteGranted = false
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
func (rf *Raft) installSanpshotRPC(server int, args SnapshotArgs, reply *SnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
    rf.debug("ok is %v\n", ok)
	return ok
}
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntry(server int, args AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntry", args, reply)
	return ok
}

func (rf *Raft) RequestAppendEntry(args AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer func() {
		rf.persist()
		rf.mu.Unlock()
	}()
	//rf.debug("got request Append Entry from %v\n",args.LeaderId)
	// 1. if args.Term < curcurrentTerm, reject.
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		//rf.debug("reject request append entry from %v args term is %v\n ", args.LeaderId, args.Term)
		return
	}

	// 2. Update my term and lastTime and set to follower.
	// what should we do to votedFor?
	if args.Term > rf.currentTerm {
		rf.votedFor = -1
	}
	rf.currentTerm = args.Term
	rf.state = Follower
	rf.lastTime = time.Now()

	// 3. Check my logs, is rf.logs[args.PrevLogIndex] == args.PrevLogTerm?
	//    if not, reject.
	reply.Term = rf.currentTerm
	if args.PrevLogIndex > LastIndex(rf.logs) {
		reply.ReplyIndex = LastIndex(rf.logs) + 1
		reply.Success = false
		//rf.debug("reject request append entry from %v for logs\n", args.LeaderId)
		return
	}

	if args.PrevLogIndex < FirstIndex(rf.logs) {
		reply.ReplyIndex = LastIndex(rf.logs) + 1
		reply.Success = false
		return
	}
	if rf.logs[args.PrevLogIndex-FirstIndex(rf.logs)].Term != args.PrevLogTerm {
		for i := 0; i < len(rf.logs); i++ {
			if rf.logs[i].Term == rf.logs[args.PrevLogIndex - FirstIndex(rf.logs)].Term {
				reply.ReplyIndex = rf.logs[i].Index
				//rf.debug("reply Index is %v\n", reply.ReplyIndex)
				break
			}
		}
		//rf.debug("reject request append entry from %v for update\n", args.LeaderId)
		reply.Success = false
		return
	}
	// 4. Append args.Entries to my logs, starting from prevlogindex.
	//rf.debug("len logs is %v, previndex is %v\n", len(rf.logs), args.PrevLogIndex)

	rf.logs = rf.logs[0 : args.PrevLogIndex + 1 - FirstIndex(rf.logs)]
	rf.logs = append(rf.logs, args.Entries...)

	// 5. Check args.LeaderCommit and commit my logs.
	//    For every new commit entry, send an ApplyMsg to applyCh
	//
	if args.LeaderCommit > rf.commitIndex {
		oldCommitIndex := rf.commitIndex
		rf.commitIndex = args.LeaderCommit
		if args.LeaderCommit > LastIndex(rf.logs) {
			rf.commitIndex = LastIndex(rf.logs)
		}
		for i := oldCommitIndex + 1; i <= rf.commitIndex; i++ {
			msg := ApplyMsg{
				Index:       i,
				Command:     rf.logs[i-FirstIndex(rf.logs)].Command,
				UseSnapshot: false,
			}
			rf.applyCh <- msg
		}
	}
	reply.Success = true
	reply.ReplyIndex = LastIndex(rf.logs) + 1
	return
}
func (rf *Raft) electionTimer() {
	for {
		if !rf.working {
			return
		}
		sleepTime := time.Duration(rand.Intn(150)+150) * time.Millisecond
		time.Sleep(sleepTime)
		now := time.Now()
		// We start election when:
		// 1. now is after lastTime + sleepTime AND
		// 2. I am not leader.
		// To prevent others from modifying lastTime
		rf.mu.Lock()
		elect := now.After(rf.lastTime.Add(sleepTime)) && rf.state != Leader
		rf.mu.Unlock()

		if elect {
			go rf.elect()
		}
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

type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntryReply struct {
	Term       int
	Success    bool
	ReplyIndex int
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
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: LastIndex(rf.logs),
		LastLogTerm:  LastTerm(rf.logs),
	}

	rf.persist()
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
			case ok := <-okchan:
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
						//rf.debug("reset lastTime to %v\n", rf.lastTime)
						rf.persist()
						rf.mu.Unlock()
					}
					voteCh <- reply.VoteGranted

				}
			case <-time.After(100 * time.Millisecond):
				voteCh <- false

			}
		}(peer)

	}

	voteCount := 1
	for i := 0; i < len(rf.peers)-1; i++ {
		v := <-voteCh
		if v {
			voteCount++
		}
	}

	//rf.debug("having %v votes\n", voteCount)
	if voteCount > (len(rf.peers)/2) && rf.state == Candidate && rf.currentTerm == request.Term {
		rf.mu.Lock()
		rf.state = Leader
		//rf.debug("I got majority votes!, sending nextindex %v to peers\n", LastIndex(rf.logs) + 1)
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = LastIndex(rf.logs) + 1
			rf.matchIndex[i] = 0
		}

		go rf.sendHeartBeat(request.Term)
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendHeartBeat(leaderTerm int) {
	for {

		// Lock.
		rf.mu.Lock()
		if !rf.working || rf.state != Leader || rf.currentTerm != leaderTerm {
			rf.mu.Unlock()
			return
		}
		// Fill in the args.
		for N := rf.commitIndex + 1; N <= LastIndex(rf.logs); N++ {
			cnt := 1
			for p := 0; p < len(rf.matchIndex); p++ {
				if p != rf.me && rf.matchIndex[p] >= N {
					cnt++
				}
			}

			if cnt > len(rf.matchIndex)/2 && rf.logs[N-FirstIndex(rf.logs)].Term == rf.currentTerm {
				for commit := rf.commitIndex + 1; commit <= N; commit++ {
					msg := ApplyMsg{
						Index:       commit,
						Command:     rf.logs[commit-FirstIndex(rf.logs)].Command,
						UseSnapshot: false,
					}
					rf.applyCh <- msg
				}
				rf.commitIndex = N
                //rf.debug("modify commitIndex as %v\n", N)
				//rf.printLogs()
			}
		}

        shot := SnapshotArgs{
            Term:             rf.currentTerm,
            LeaderId:         rf.me,
            LastIncludeIndex: FirstIndex(rf.logs),
            LastIncludeTerm:  FirstTerm(rf.logs),
            Data:             rf.persister.ReadSnapshot(),
        }
		for peer := 0; peer < len(rf.peers); peer++ {

			if peer == rf.me {
				continue
			}
			// Prepare the args for this follower
			// For each followewr, send entries [nextIndex[peer], end]
			// Here make a deep copy of the sending entries to avoid race condition.

			nextIndex := rf.nextIndex[peer]

			if nextIndex <= (FirstIndex(rf.logs)) {
				//rf.debug("prepare to install snapshot for %v\n", peer)
				//prepare snapshot
				//call installsnapshotRPC

				
				go func(p int, shot SnapshotArgs) {
                    var reply SnapshotReply
					okchan := make(chan bool)
					go func() {
						okchan <- rf.installSanpshotRPC(p, shot, &reply)
					}()
					select {
					case ok := <-okchan:
						if ok {
							rf.debug("installing snapshot for peer %v， replyterm is %v\n", p, reply.ReplyTerm)
							rf.mu.Lock()
                            defer func(){
                                rf.persist()
                                rf.mu.Unlock()
                            }()
                            if reply.ReplyTerm > rf.currentTerm {
                                rf.debug("replyterm is bigger\n")

								rf.state = Follower
								rf.votedFor = -1
								rf.lastTime = time.Now()
								rf.currentTerm = reply.ReplyTerm
								return
							}
							rf.nextIndex[p] = FirstIndex(rf.logs) + 1
							rf.matchIndex[p] = FirstIndex(rf.logs) 
						} else {
							//rf.debug("failed to install snapshot\n")
							return
						}
					case <-time.After(1000 * time.Millisecond):
						//rf.debug("WTF\n")
					}
				}(peer, shot)
			} else {
				if FirstIndex(rf.logs) != 0 {
					//rf.debug("nextIndex for peer %v is %v, len logs is %v, firstIndex is %v\n", peer, nextIndex, len(rf.logs), FirstIndex(rf.logs))
				}

				heartbeat := AppendEntryArgs{
					Term:         leaderTerm,
					LeaderId:     rf.me,
					Entries:      make([]LogEntry, len(rf.logs[nextIndex-FirstIndex(rf.logs):])),
					LeaderCommit: rf.commitIndex,
					PrevLogTerm:  rf.logs[nextIndex-1-FirstIndex(rf.logs)].Term,
					PrevLogIndex: nextIndex - 1,
				}
				//rf.debug("sending heartbeat... leaderTerm is %v\n", leaderTerm)
				copy(heartbeat.Entries, rf.logs[nextIndex-FirstIndex(rf.logs):])

				go func(p int, heartbeat AppendEntryArgs) {
					var reply AppendEntryReply
					okchan := make(chan bool)
					go func() {
						okchan <- rf.sendAppendEntry(p, heartbeat, &reply)
					}()
					select {
					// We get the reply.
					case ok := <-okchan:
						// if ok
						// 1. if reply.Term > rf.currentTerm, update term and change back to follower
						// 2. if succeed, update matchIndex and nextIndex
						// 3. if failed, decrease nextIndex by 1
						if ok {
							rf.mu.Lock()
							defer rf.mu.Unlock()

							if !rf.working {
								return
							}

							if rf.currentTerm != leaderTerm {
								return
							}

							if rf.state != Leader {
								return
							}

							if reply.Term > rf.currentTerm {
								// Whatelse do we have to do here?
								// Check elect.
								rf.state = Follower
								rf.currentTerm = reply.Term
								rf.lastTime = time.Now()
								rf.votedFor = -1
								rf.persist()
								return
							}
							if reply.Success {
								// You do not have to update nextIndex amd matchIndex
								// if heartbeat is emtpy
								if len(heartbeat.Entries) > 0 {
									rf.nextIndex[p] = LastIndex(heartbeat.Entries) + 1
									rf.matchIndex[p] = LastIndex(heartbeat.Entries)
									//rf.debug("I got reply from %v, nextIndex is %v\n", p, rf.nextIndex[p])
									for k := 0; k < len(heartbeat.Entries); k++ {
										//rf.debug("heartbeat %v is %v\n", k, heartbeat.Entries[k])
									}
								}
								return
							}
							if rf.nextIndex[p] > 1 {
								// rf.nextIndex[p]--
								rf.nextIndex[p] = reply.ReplyIndex
							}

						}

					case <-time.After(100 * time.Millisecond):
						//rf.debug("Now is %v, After 100 Millisecond\n", time.Now())
					}
				}(peer, heartbeat)
			}
		}

		// Unlock
		rf.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
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
	rf.mu.Lock()
	defer func() {
		rf.persist()
		rf.mu.Unlock()
	}()

	if rf.state != Leader {
		return -1, -1, false
	}

	log := LogEntry{
		Index:   LastIndex(rf.logs) + 1,
		Command: command,
		Term:    rf.currentTerm,
	}
	rf.logs = append(rf.logs, log)
	rf.printLogs()
	//rf.debug("Index is %v\n", rf.logs[len(rf.logs) - 1].Index)

	return LastIndex(rf.logs), rf.currentTerm, rf.state == Leader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.working = false
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
	rf.working = true
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.Readsnapshot(persister.ReadSnapshot())
	go rf.electionTimer()
	return rf
}
