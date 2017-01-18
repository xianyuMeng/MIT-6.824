package shardkv

import (
	"shardmaster"
	"labrpc"
	"raft"
	"sync"
	"log"
	"encoding/gob"
	"fmt"
	"time"
	"bytes"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// Op struct for client request.
type RequestOp struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Args 	RequestArgs
	Reply 	RequestReply
}

type OpKind int
const (
	REQUEST OpKind  = 0
	START_RECONFIG  = 1
	FINISH_RECONFIG = 2
	ADD_SHARD 		= 3
)

type Op struct {
	Kind 	OpKind
	Command interface{}
}

// Status for a shard kv.
type State string
const (
	USER State 	= "USER"
	CONF 	   	= "CONF"
)

// Status for shard
type ShardState string
const (
	SHARD_WORKING = "SHARD_WORKING" // This shard is owned by myself and is working.
	SHARD_SENDING = "SHARD_SENDING" // This shard is being sent to its new owner.
	SHARD_WAITING = "SHARD_WAITING" // This shard is being sent to me, I haven't received it.
	SHARD_BYOTHER = "SHARD_BYOTHER" // This shard is held by other group or 0.
)

type AddShardArgs struct {
	Receiver 	int 		// Receiver group.
	Sender 		int 		// Send group.
	Shard 		int 		// Shard we are sending.
	Num 		int  		// Num of the new config.
	KV 			map[string]string
	Records 	map[int64]int // For receiver to update its records to avoid duplicate requests.
}

type AddShardReply struct {
	Succeed     bool
}

type AddShardOp struct {
	Args 		AddShardArgs
	Reply 		AddShardReply
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	mck 		*shardmaster.Clerk

	config 		shardmaster.Config
	newConfig 	shardmaster.Config
	state 		State

	// States for each shard, all initialized to SHARD_BYOTHER.
	shardStates [shardmaster.NShards] ShardState

	working bool
	kvs 	map[int]map[string]string 	// Key-value for each shard
	records map[int64]int

	results map[int]chan Op
}

func (kv *ShardKV) debug(format string, a ...interface{}) (n int, err error) {
	if !kv.working {
		return
	}
	n, err = DPrintf("%v", kv.sdebug(format, a...))
	return
}

// Return a debug string.
func (kv *ShardKV) sdebug(format string, a ...interface{}) string {
	if !kv.working {
		return "not working"
	}
	a = append(a, 0, 0, 0, 0)
	copy(a[4:], a[0:])
	a[0] = kv.gid
	a[1] = kv.me
	a[2] = kv.state
	a[3] = kv.config.Num
	return fmt.Sprintf("ShardKV %v.%v %v %v " + format, a...)
}

// Check if we need a snapshot.
// Caller should hold the lock.
// Args: lastIncluded index of the snapshot.
func (kv *ShardKV) snapshot(index int) {
	if kv.maxraftstate != -1 && kv.rf.GetStateSize() > kv.maxraftstate {
		w := new(bytes.Buffer)
		e := gob.NewEncoder(w)
		e.Encode(kv.kvs)
		e.Encode(kv.records)
		e.Encode(kv.state)
		e.Encode(kv.config)
		e.Encode(kv.newConfig)
		e.Encode(kv.shardStates)
		go kv.rf.Snapshot(w.Bytes(), index)
	}
}

// Caller should hold the lock.
func (kv *ShardKV) DuplicateRequesst(id int64, sid int) bool {
	record, ok := kv.records[id]
	if !ok {
		return false
	}
	return record >= sid
}

// Caller should hold the lock.
// Try to apply the user request.
// First it check if it's in USER state.
// Then check if the shard is handled here.
// Finally check if this is duplicate.
func (kv *ShardKV) ApplyRequest(args *RequestArgs, reply *RequestReply) {
	shard := key2shard(args.Key)
	if kv.shardStates[shard] != SHARD_WORKING {
		reply.Err = ErrWrongGroup
		return
	}

	if !kv.DuplicateRequesst(args.Id, args.Sid) {
		// Not duplicate. Apply
		kv.records[args.Id] = args.Sid
		switch (args.Op) {
			case "PUT":		kv.kvs[shard][args.Key] = args.Value
			case "APPEND":	kv.kvs[shard][args.Key] = kv.kvs[shard][args.Key] + args.Value
			case "GET": 	// Do nothing.
			default: 		// Unknown.
		}
	}

	// Fill in the reply.
	value, ok := kv.kvs[shard][args.Key]
	if !ok {
		reply.Err = ErrNoKey
		reply.Value = ""
		return
	}
	reply.Err = OK
	reply.Value = value
	return
}

//
// A daemon listening to apply channel.
//
func (kv *ShardKV) Run() {
	for {
		if !kv.working {
			return
		}

		msg := <- kv.applyCh

		if msg.UseSnapshot {
			// Install a snapshot.
			r := bytes.NewBuffer(msg.Snapshot)
			d := gob.NewDecoder(r)

			var lastIndex int
			var lastTerm int

			kv.mu.Lock()

			d.Decode(&lastIndex)
			d.Decode(&lastTerm)

			kv.kvs = make(map[int]map[string]string)
			kv.records = make(map[int64]int)
			d.Decode(&kv.kvs)
			d.Decode(&kv.records)
			d.Decode(&kv.state)
			d.Decode(&kv.config)
			d.Decode(&kv.newConfig)
			d.Decode(&kv.shardStates)
			kv.mu.Unlock()
		} else {
			op := msg.Command.(Op)
			switch (op.Kind) {
			case REQUEST:
				// Normal user request.
				rop := op.Command.(RequestOp)
				// Fill the reply
				kv.mu.Lock()
				kv.ApplyRequest(&rop.Args, &rop.Reply)

				ch, ok := kv.results[msg.Index]
				if !ok {
					kv.results[msg.Index] = make(chan Op, 1)
					ch = kv.results[msg.Index]
				} else {
					select {
					case <- ch:
					default:
					}
				}

				op.Command = rop
				ch <- op
				kv.snapshot(msg.Index)
				kv.mu.Unlock()

			case ADD_SHARD:
				aop := op.Command.(AddShardOp)
				checked := kv.checkAddShard(&aop.Args)
				kv.mu.Lock()
				switch (checked) {
				case CHECK_ARGS_REJECT: 
					aop.Reply.Succeed = false
				case CHECK_ARGS_DUPLICATE: 
					aop.Reply.Succeed = true
				case CHECK_ARGS_EXECUTE:
					aop.Reply.Succeed = true
					// Make a copy of the shard to avoid race condition
					// with raft service.
					kv.kvs[aop.Args.Shard] = make(map[string]string)
					for k, v := range aop.Args.KV {
						kv.kvs[aop.Args.Shard][k] = v
					}
					// Update records.
					for id, newSid := range aop.Args.Records {
						if mySid, ok := kv.records[id]; ok {
							if mySid < newSid {
								kv.records[id] = newSid
							}
						} else {
							kv.records[id] = newSid
						}
					}
					// Change shard state to working.
					kv.shardStates[aop.Args.Shard] = SHARD_WORKING
				default:
					panic(kv.sdebug("Unknown checked %v\n", checked))
				}

				ch, ok := kv.results[msg.Index]
				if !ok {
					kv.results[msg.Index] = make(chan Op, 1)
					ch = kv.results[msg.Index]
				} else {
					select {
					case <- ch:
					default:
					}
				}

				op.Command = aop
				ch <- op
				kv.snapshot(msg.Index)
				kv.mu.Unlock()

			case START_RECONFIG:
				// Start reconfiguration.
				newConfig := op.Command.(shardmaster.Config)
				kv.mu.Lock()
				if kv.state == USER && newConfig.Num == kv.config.Num + 1 {
					// Start reconfiguration.
					// kv.debug("Start reconfig for %v\n", newConfig.Num)
					kv.state = CONF
					kv.newConfig = newConfig
					// Update shards' states.
					for shard := 0; shard < len(newConfig.Shards); shard++ {
						oldGID := kv.config.Shards[shard]
						newGID := newConfig.Shards[shard]
						if oldGID != kv.gid && newGID == kv.gid {
							// Incoming shard.
							kv.shardStates[shard] = SHARD_WAITING
						}
						if oldGID == kv.gid && newGID != kv.gid {
							// Outgoing shard.
							kv.shardStates[shard] = SHARD_SENDING
						}
					}
				}
				kv.snapshot(msg.Index)
				kv.mu.Unlock()

			case FINISH_RECONFIG:
				// Finish reconfiguration.
				kv.mu.Lock()
				if kv.config.Num == kv.newConfig.Num - 1 {
					// kv.debug("Finish reconfig for %v\n", kv.newConfig.Num)
					kv.state = USER
					kv.config = kv.newConfig
				}
				kv.snapshot(msg.Index)
				kv.mu.Unlock()

			default:
				panic(kv.sdebug("Unknown Op.Kind %v\n", op.Kind))
			}
		}
	}
}

// Send the shard to new client.
// When finish, it will delete the shard and change the state back to SHARD_BYOTHER.
func (kv *ShardKV) sendShard(args AddShardArgs) {
	for {
		if !kv.working {
			return
		}
		gid := args.Receiver
		if servers, ok := kv.newConfig.Groups[gid]; ok {
			for i := 0; i < len(servers); i++ {
				server := kv.make_end(servers[i])
				reply := AddShardReply{}
				ok := server.Call("ShardKV.AddShard", &args, &reply)
				if ok && reply.Succeed {
					kv.mu.Lock()
					defer kv.mu.Unlock()
					// Be careful because we may send shard to ourselves.
					if args.Receiver != kv.gid {
						kv.shardStates[args.Shard] = SHARD_BYOTHER
						delete(kv.kvs, args.Shard)
					}
					return
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}



func (kv *ShardKV) Update() {

	for {

		if !kv.working {
			return
		}

		var current int
		var state State
		kv.mu.Lock()
		state = kv.state
		current = kv.config.Num
		kv.mu.Unlock()

		if state == USER {
			// We are in user mode.
			newConfig := kv.mck.Query(current + 1)
			if newConfig.Num == current + 1 {
				// There is a new term.
				// Prepare Op
				op := Op {
					Kind 	: START_RECONFIG,
					Command : newConfig,
				}
				// Try to start it. Note that we do not care about the result.
				// Nor do we care about if we are the leader. We just try.
				kv.rf.Start(op)
			}
		} else {
			// We are in reconfiguration mode.
			// Try to send those shards.
			ready := true
			sentCnt := 0
			doneCh := make(chan bool)
			kv.mu.Lock()
			for shard := 0; shard < len(kv.shardStates); shard++ {
				if kv.shardStates[shard] == SHARD_SENDING {
					// Send the shard.
					ready = false
					sentCnt++
					// Prepare the args.
					args := AddShardArgs {
						Receiver 	: kv.newConfig.Shards[shard],
						Sender 		: kv.gid,
						Shard 		: shard,
						Num 		: kv.newConfig.Num,
						KV 			: make(map[string]string),
						Records 	: make(map[int64]int),
					}
					// Copy the shard and records.
					for k, v := range kv.kvs[shard] {
						args.KV[k] = v
					}
					for k, v := range kv.records {
						args.Records[k] = v
					}
					go func(args AddShardArgs) {
						kv.sendShard(args)
						doneCh <- true
					}(args)
				}
				if kv.shardStates[shard] == SHARD_WAITING {
					ready = false
					if kv.config.Shards[shard] == 0 {
						// Initialize new shard and send them to myself.
						if _, ok := kv.kvs[shard]; ok {
							panic(kv.sdebug("Initializing shard %v, but already there\n", shard))
						}
						sentCnt++
						// Prepare the args.
						args := AddShardArgs {
							Receiver 	: kv.newConfig.Shards[shard],
							Sender 		: kv.gid,
							Shard 		: shard,
							Num 		: kv.newConfig.Num,
							KV 			: make(map[string]string),
							Records 	: make(map[int64]int),
						}
						// Copy records.
						for k, v := range kv.records {
							args.Records[k] = v
						}
						go func(args AddShardArgs) {
							kv.sendShard(args)
							doneCh <- true
						}(args)
					}
				}
			}
			kv.mu.Unlock()

			// We block here to wait for shard being delivered.
			// It makes no sense to move on if we still have undelivered shards.
			for i := 0; i < sentCnt; i++ {
				<- doneCh
			}

			if ready {
				// We are ready to update our config.
				op := Op {
					Kind 	: FINISH_RECONFIG,
					Command : nil,
				}
				kv.rf.Start(op)
			}
		}

		// Check update every 100ms.
		time.Sleep(100 * time.Millisecond)
	}
}

// Internal used to check args of AddShard. Returns
// 0 if we have to reject the RPC.
// 1 if we have already held the shard. RPC should return true without executing it.
// 2 if we are expecting this shard. Execute it as normal RPC.
const (
	CHECK_ARGS_REJECT int = 0
	CHECK_ARGS_DUPLICATE  = 1
	CHECK_ARGS_EXECUTE    = 2
)
func (kv *ShardKV) checkAddShard(args *AddShardArgs) int {
	if !kv.working {
		return CHECK_ARGS_REJECT
	}
	if args.Receiver != kv.gid {
		// What happened?
		panic(kv.sdebug("Wrong delivered AddShardArgs to %v, should to%v\n", kv.gid, args.Receiver))
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// Check if we have newer configuration.
	if kv.config.Num >= args.Num {
		return CHECK_ARGS_DUPLICATE
	}
	// Check if this is from way ahead configuration.
	if kv.newConfig.Num < args.Num {
		return CHECK_ARGS_REJECT
	}
	// This is the shard we are expecting for kv.newConfig.
	// Check if we already have it.
	switch (kv.shardStates[args.Shard]) {
	case SHARD_SENDING: panic(kv.sdebug("Invalid sending shard %v\n", args.Shard))
	case SHARD_BYOTHER: panic(kv.sdebug("Invalid byother shard %v\n", args.Shard))
	case SHARD_WAITING: return CHECK_ARGS_EXECUTE
	case SHARD_WORKING: return CHECK_ARGS_DUPLICATE
	default: panic(kv.sdebug("Unknown shard state %v\n", kv.shardStates[args.Shard]))
	}
}

func (kv *ShardKV) AddShard(args *AddShardArgs, reply *AddShardReply) {

	checked := kv.checkAddShard(args)
	if checked == CHECK_ARGS_REJECT {
		reply.Succeed = false
		return
	}
	if checked == CHECK_ARGS_DUPLICATE {
		reply.Succeed = true
		return
	}
	// We have to execute this RPC.
	// Prepare the op.
	aop := AddShardOp {
		Args 	: *args,
		Reply 	: AddShardReply{},
	}
	op := Op {
		Kind 	: ADD_SHARD,
		Command : aop,
	}

	// Try to start.
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Succeed = false
		return
	}
	kv.mu.Lock()
	ch, ok := kv.results[index]
	if !ok {
		kv.results[index] = make(chan Op, 1)
		ch = kv.results[index]
	}
	kv.mu.Unlock()

	// Wait for the result.
	select {
	case result := <- ch:
		if result.Kind == ADD_SHARD {
			aop := result.Command.(AddShardOp)
			if aop.Args.Shard == args.Shard && aop.Args.Num == args.Num {
				// This is what we are expecting.
				reply.Succeed = aop.Reply.Succeed
				return
			}
		}
	case <- time.After(time.Second):
	}
	reply.Succeed = false
	return
}

func (kv *ShardKV) Request(args *RequestArgs, reply *RequestReply) {

	if !kv.working {
		reply.WrongLeader = true
		return
	}

	shard := key2shard(args.Key)
	kv.mu.Lock()
	if kv.shardStates[shard] != SHARD_WORKING {
		// This is the wrong group.
		reply.WrongLeader = true
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	// We decided to take the command.
	rop := RequestOp {
		Args 	: *args,
		Reply 	: RequestReply {},
	}

	op := Op {
		Kind 	: REQUEST,
		Command : rop,
	}

	// Try to start the request.
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	// Create the channel to listen to.
	kv.mu.Lock()
	ch, ok := kv.results[index]
	if !ok {
		kv.results[index] = make(chan Op, 1)
		ch = kv.results[index]
	}
	kv.mu.Unlock()

	select {
	case result := <- ch:
		if result.Kind != REQUEST {
			reply.WrongLeader = true
			return
		}
		rop := result.Command.(RequestOp)
		if rop.Args.Id != args.Id || rop.Args.Sid != args.Sid {
			reply.WrongLeader = true
			return
		}
		// We got our result.
		reply.WrongLeader = false
		reply.Value = rop.Reply.Value
		reply.Err = rop.Reply.Err
		return
	case <- time.After(time.Second):
		reply.WrongLeader = true
		return
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	kv.working = false
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// log) with persister.SaveRaftState().
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})
	gob.Register(RequestOp{})
	gob.Register(RequestReply{})
	gob.Register(RequestArgs{})
	gob.Register(AddShardOp{})
	gob.Register(AddShardArgs{})
	gob.Register(AddShardReply{})
	gob.Register(shardmaster.Config{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.mck = shardmaster.MakeClerk(kv.masters)

	// First configuration.
	kv.config = kv.mck.Query(0)
	kv.newConfig = kv.config
	kv.state = USER

	// Initialize shards to SHARD_BYOTHER
	for i := 0; i < len(kv.shardStates); i++ {
		kv.shardStates[i] = SHARD_BYOTHER
	}

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.working = true
	kv.kvs = make(map[int]map[string]string)
	kv.records = make(map[int64]int)
	kv.results = make(map[int]chan Op)

	go kv.Run()
	go kv.Update()


	return kv
}
