package shardkv

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"raft"
	"shardmaster"
	"sync"
	"time"
	"fmt"
)

type ServerState int

const (
	Working  ServerState = iota
	ReConfig ServerState = iota
)

type ShardState int

const (
	//this shard is mine temporarily, I'm sending it to the other server
	Sending    ShardState = iota
	Waiting    ShardState = iota
	Holding    ShardState = iota
	NotHolding ShardState = iota
)

// type Op struct {
// 	// Your definitions here.
// 	// Field names must start with capital letters,
// 	// otherwise RPC will break.
// }

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
	state ServerState

	markClient  map[int64]int
	markRequest map[int]map[string]string
	//this is the map which stores key-value
	//shard number -> (key -> value)
	markReply map[int]chan Op

	markShard map[int]ShardState
	//record state of shards

	configs   []shardmaster.Config
	newConfig shardmaster.Config
	mck       *shardmaster.Clerk

	alive bool
}

func (kv *ShardKV) sdebug(format string, a ...interface{}) string {
	if !kv.alive {
		return "not working"
	}
	a = append(a, 0, 0, 0, 0)
	copy(a[4:], a[0:])
	a[0] = kv.gid
	a[1] = kv.me
	a[2] = kv.state
	a[3] = kv.configs[len(kv.configs) - 1].Num
	return fmt.Sprintf("ShardKV %v.%v %v %v " + format, a...)
}
func (kv *ShardKV) LastConfigNum() int {
	if len(kv.configs) == 0 {
		return 0
	} else {
		return kv.configs[len(kv.configs)-1].Num
	}
}

func (kv *ShardKV) LastConfig() shardmaster.Config {
	if len(kv.configs) == 0 {
		return shardmaster.Config{}
	} else {
		return kv.configs[len(kv.configs) - 1]
	}
}
func (kv *ShardKV) Exec(args *SKVArgs, reply *SKVReply) {
	if !kv.alive {
		reply.WrongLeader = true
		return
	}
	shard := key2shard(args.Key)
	kv.mu.Lock()
	if kv.markShard[shard] != Holding {
		reply.WrongLeader = true
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	op := Op {
		OpType : args.OpType,
		Command : *args,
	}
	index, _, isLeader := kv.rf.Start(op)

	if isLeader == false {
		reply.WrongLeader = true
		return
	} else {
		// kv.debug("start %v %v at %v\n", args.ClientID, args.SerialID, index)

		kv.mu.Lock()
		if _, ok := kv.markReply[index]; !ok {
			kv.markReply[index] = make(chan Op, 1)
		}
		kv.mu.Unlock()
		select {
		case r := <-kv.markReply[index]:
			if r.OpType == RECONFIG || r.OpType == PULLSHARD || r.OpType == READY {
				reply.WrongLeader = true
				return
			}
			skvreply := r.Command.(SKVReply) 
			if skvreply.ClientID == args.ClientID && skvreply.SerialID == args.SerialID {
				reply.WrongLeader = skvreply.WrongLeader
				reply.Err = skvreply.Err
				reply.Value = skvreply.Value
			} else {
				reply.WrongLeader = true
			}
			return
		case <-time.After(1000 * time.Millisecond):
			reply.WrongLeader = true
			return
		}
	}

}

func (kv *ShardKV) Run() {
	reply := SKVReply{
		WrongLeader: true,
	}
	for {
		applych := <-kv.applyCh
		//read applych
		//reconfig
		//change myself as reconfig, if I'm reconfig already, ignore
		//update shard based on config and new config
		//
		//ready
		//check if I'm reconfig, if not, ignore
		//change myself as working

		//sending shard
		//get shard

		if applych.UseSnapshot == true {
			var lastincludeindex int
			var lastincludeterm int
			var state ServerState
			var newconfig shardmaster.Config
			configs := make([]shardmaster.Config, 0)
			newmarkClient := make(map[int64]int, 0)
			newmarkRequest := make(map[int]map[string]string, 0)
			newmarkShard := make(map[int]ShardState, 0)

			if len(applych.Snapshot) == 0 {
				return
			}

			r := bytes.NewBuffer(applych.Snapshot)
			d := gob.NewDecoder(r)
			d.Decode(&lastincludeindex)
			d.Decode(&lastincludeterm)
			d.Decode(&state)
			d.Decode(&newconfig)
			d.Decode(&configs)
			d.Decode(&newmarkClient)
			d.Decode(&newmarkRequest)
			d.Decode(&newmarkShard)
			kv.mu.Lock()
			kv.markClient = newmarkClient
			kv.markRequest = newmarkRequest
			kv.markShard = newmarkShard
			kv.state = state
			kv.configs = configs
			kv.newConfig = newconfig
			kv.mu.Unlock()

		} else {
			op := applych.Command.(Op)
			if op.OpType == PUT || op.OpType == GET || op.OpType == APPEND {
				skvargs := op.Command.(SKVArgs)
				skvreply := SKVReply{}
				kv.mu.Lock()
				shard := key2shard(skvargs.Key)
				if kv.markShard[shard] != Holding {
					skvreply.Err = ErrWrongGroup
					return
				}
				_, ok := kv.markClient[skvargs.ClientID]
				if !ok || (ok && (kv.markClient[skvargs.ClientID]+1) == skvargs.SerialID) {
					//not duplicate
					if skvargs.OpType == PUT {
						kv.markRequest[shard][skvargs.Key] = skvargs.Value
					}
					if skvargs.OpType == APPEND {
						kv.markRequest[shard][skvargs.Key] = kv.markRequest[shard][skvargs.Key] + skvargs.Value
					}
					kv.markClient[skvargs.ClientID] = skvargs.SerialID

				} // else: sliently ignore duplicate command.

				// Prepare the reply.
				reply.WrongLeader = false
				reply.OpType = skvargs.OpType
				if _, ok := kv.markRequest[shard][skvargs.Key]; !ok {
					reply.Err = ErrNoKey
					reply.Value = ""
					return
				} else {
					reply.Err = OK
					reply.Value = kv.markRequest[shard][skvargs.Key]
				}
				reply.ClientID = skvargs.ClientID
				reply.SerialID = skvargs.SerialID

				if _, ch := kv.markReply[applych.Index]; !ch {
					kv.markReply[applych.Index] = make(chan Op, 1)
				}

				kv.markReply[applych.Index] <- op
				op.Command = skvargs
				if kv.maxraftstate != -1 && kv.rf.GetStateSize() > kv.maxraftstate {
					w := new(bytes.Buffer)
					e := gob.NewEncoder(w)
					e.Encode(kv.state)
					e.Encode(kv.newConfig)
					e.Encode(kv.configs)
					e.Encode(kv.markClient)
					e.Encode(kv.markRequest)
					e.Encode(kv.markShard)
					go kv.rf.Snapshot(w.Bytes(), applych.Index)
				}
				kv.mu.Unlock()
			}
			if op.OpType == PULLSHARD {
				Args := op.Command.(AddShard)
				kv.mu.Lock()
				pull := kv.PullShardCheck(&(Args.Args))
				if pull == REJECT {
					Args.Reply = false
				}
				if pull == DUPLICATE {
					Args.Reply = true
				}
				if pull == DONE {
					Args.Reply = true
				}

				kv.markRequest[Args.Args.Shard] = make(map[string]string, 0)
				for k, v := range Args.Args.ShardKV {
					kv.markRequest[Args.Args.Shard][k] = v
				}
				for c, s := range Args.Args.markClient {
					if ss, ok := kv.markClient[c]; ok {
						if ss < s {
							kv.markClient[c] = s
						}
					} else {
						kv.markClient[c] = s
					}
				}
				kv.markShard[Args.Args.Shard] = Holding
				if _, ch := kv.markReply[applych.Index]; !ch {
					kv.markReply[applych.Index] = make(chan Op, 1)
				}

				kv.markReply[applych.Index] <- op
				if kv.maxraftstate != -1 && kv.rf.GetStateSize() > kv.maxraftstate {
					w := new(bytes.Buffer)
					e := gob.NewEncoder(w)
					e.Encode(kv.state)
					e.Encode(kv.newConfig)
					e.Encode(kv.configs)
					e.Encode(kv.markClient)
					e.Encode(kv.markRequest)
					e.Encode(kv.markShard)
					go kv.rf.Snapshot(w.Bytes(), applych.Index)
				}
				kv.mu.Unlock()
			}
			if op.OpType == RECONFIG {
				skvargs := op.Command.(SKVArgs)
				kv.mu.Lock()
				if kv.state == Working && skvargs.Config.Num == (kv.configs[len(kv.configs) - 1].Num + 1) {
					kv.state = ReConfig
					kv.newConfig = skvargs.Config
					lastconfig := kv.LastConfig()
					for s, gid := range kv.newConfig.Shards {
						if kv.gid != lastconfig.Shards[s] && kv.gid == gid {
							kv.markShard[s] = Waiting
						} 
						if kv.gid == lastconfig.Shards[s] && kv.gid != gid {
							kv.markShard[s] = Sending
						}
					}
				}
				if kv.maxraftstate != -1 && kv.rf.GetStateSize() > kv.maxraftstate {
					w := new(bytes.Buffer)
					e := gob.NewEncoder(w)
					e.Encode(kv.state)
					e.Encode(kv.newConfig)
					e.Encode(kv.configs)
					e.Encode(kv.markClient)
					e.Encode(kv.markRequest)
					e.Encode(kv.markShard)
					go kv.rf.Snapshot(w.Bytes(), applych.Index)
				}
				kv.mu.Unlock()
			}
			if op.OpType == READY {
				kv.mu.Lock()
				if (kv.LastConfigNum() + 1) == kv.newConfig.Num {
					kv.state = Working
					kv.configs = append(kv.configs, kv.newConfig)

				}
				if kv.maxraftstate != -1 && kv.rf.GetStateSize() > kv.maxraftstate {
					w := new(bytes.Buffer)
					e := gob.NewEncoder(w)
					e.Encode(kv.state)
					e.Encode(kv.newConfig)
					e.Encode(kv.configs)
					e.Encode(kv.markClient)
					e.Encode(kv.markRequest)
					e.Encode(kv.markShard)
					go kv.rf.Snapshot(w.Bytes(), applych.Index)
				}				
				kv.mu.Unlock()
			}
		}
	}

}
func (kv *ShardKV) PullShardCheck(args *SendShardArgs) PullOperation {
	if !kv.alive {
		return REJECT
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// Check if we have newer configuration.
	if kv.configs[len(kv.configs)-1].Num >= args.ConfigNum {
		return DUPLICATE
	}
	// Check if this is from way ahead configuration.
	if kv.newConfig.Num < args.ConfigNum {
		return REJECT
	}
	// This is the shard we are expecting for kv.newConfig.
	// Check if we already have it.
	switch kv.markShard[args.Shard] {
	case Sending:
		panic(kv.sdebug("Invalid sending shard %v\n", args.Shard))
	case NotHolding:
		panic(kv.sdebug("Invalid byother shard %v\n", args.Shard))
	case Waiting:
		return DONE
	case Holding:
		return DUPLICATE
	default:
		panic(kv.sdebug("Unknown shard state %v\n", kv.markShard[args.Shard]))
	}
}

// func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
// 	// Your code here.
// }

// func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
// 	// Your code here.
// }

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	kv.alive = false
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

func (kv *ShardKV) Update() {
	for {
		if !kv.alive {
			return
		}
		if kv.state == Working {
			newconfig := kv.mck.Query(kv.LastConfigNum() + 1)
			if newconfig.Num == (kv.LastConfigNum() + 1) {
				args := Op{
					OpType: RECONFIG,
					Command: SKVArgs{
						OpType: RECONFIG,
						Config: newconfig,
					},
				}
				kv.rf.Start(args)
			}
		} else {
			ready := true
			sentCnt := 0
			AlldoneCh := make(chan bool)
			kv.mu.Lock()
			for s, state := range kv.markShard {
				if state == Sending {
					ready = false
					sentCnt++
					args := SendShardArgs{
						ReceiverGID: kv.newConfig.Shards[s],
						SenderGID:   kv.gid,
						Shard:       s,
						ConfigNum:   kv.newConfig.Num,
						ShardKV:     make(map[string]string, 0),
						markClient:  make(map[int64]int, 0),
					}

					for k, v := range kv.markRequest[s] {
						args.ShardKV[k] = v
					}
					for k, v := range kv.markClient {
						args.markClient[k] = v
					}
					go func(args SendShardArgs) {
						kv.SendingShard(args)
						AlldoneCh <- true
					}(args)
				}
				if state == Waiting {
					ready = false
					if kv.configs[len(kv.configs)-1].Shards[s] == 0 {
						sentCnt++
						args := SendShardArgs{
							ReceiverGID: kv.newConfig.Shards[s],
							SenderGID:   kv.gid,
							Shard:       s,
							ConfigNum:   kv.newConfig.Num,
							ShardKV:     make(map[string]string, 0),
							markClient:  make(map[int64]int, 0),
						}
						for k, v := range kv.markClient {
							args.markClient[k] = v
						}
						go func(args SendShardArgs) {
							kv.SendingShard(args)
							AlldoneCh <- true
						}(args)
					}
				}
			}
			kv.mu.Unlock()
			for i := 0; i < sentCnt; i++ {
				<-AlldoneCh
			}

			if ready {
				// We are ready to update our config.
				args := Op{
					OpType:  READY,
					Command: nil,
				}
				kv.rf.Start(args)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) SendingShard(args SendShardArgs) {
	for {
		if !kv.alive {
			return
		}
		if servers, ok := kv.newConfig.Groups[args.ReceiverGID]; ok {
			for i := 0; i < len(servers); i++ {
				server := kv.make_end(servers[i])
				reply := SendShardReply{}
				ok := server.Call("ShardKV.AddShard", &args, &reply)
				if ok && reply.Success {
					kv.mu.Lock()
					defer kv.mu.Unlock()
					// Be careful because we may send shard to ourselves.
					if args.ReceiverGID != kv.gid {
						kv.markShard[args.Shard] = NotHolding
						delete(kv.markRequest, args.Shard)
					}
					return
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
func (kv *ShardKV) AddShard(args *SendShardArgs, reply *SendShardReply) {

	if !kv.alive {
		reply.Success = false
		return
	}

	if kv.configs[len(kv.configs)-1].Num >= args.ConfigNum {
		//this is duplicate request
		reply.Success = true
		return
	}

	if kv.newConfig.Num < args.ConfigNum {
		reply.Success = false
		return
	}

	_args := Op{
		OpType: PULLSHARD,
		Command: AddShard{
			Args:  *args,
			Reply: false,
		},
	}

	index, _, isLeader := kv.rf.Start(_args)
	if !isLeader {
		reply.Success = false
		return
	}

	kv.mu.Lock()
	if _, ok := kv.markRequest[index]; !ok {
		kv.markReply[index] = make(chan Op, 1)
	}
	kv.mu.Unlock()

	// Wait for the result.
	select {
	case result := <-kv.markReply[index]:
		if result.OpType == PULLSHARD {
			addshard := result.Command.(AddShard)
			if addshard.Args.Shard == args.Shard && addshard.Args.ConfigNum == args.ConfigNum {
				reply.Success = addshard.Reply
				return
			}
		}
	case <-time.After(time.Second):
	}
	reply.Success = false
	return
}
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	//gob.Register(Op{})
	gob.Register(SKVArgs{})
	gob.Register(SKVReply{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.markClient = make(map[int64]int, 0)
	kv.markRequest = make(map[int]map[string]string, 0)
	kv.markReply = make(map[int]chan Op, 0)
	kv.configs = make([]shardmaster.Config, 0)
	kv.state = Working
	kv.alive = true
	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.Run()
	go kv.Update()

	return kv
}
