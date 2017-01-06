package shardmaster

import (
	//"container/heap"
	"raft"
	"labrpc"
	"sync"
	"encoding/gob"
	"time"
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	
	//ClientID -> SerialID
	markClient map[int64]int
	//index -> reply
	markReply map[int] chan SKVReply

	configs []Config // indexed by config num
}


type Op struct {
	// Your data here.
}

func LastConfig (configs []Config) Config {
	return configs[len(configs) - 1]
}

func (sm *ShardMaster) Exec (args *SKVArgs, reply *SKVReply) {
	index, _, isLeader := sm.rf.Start(*args)

	if isLeader == false {
		reply.WrongLeader = true
		return
	} else {
		// kv.debug("start %v %v at %v\n", args.ClientID, args.SerialID, index)
		sm.mu.Lock()
		if _, ok := sm.markReply[index]; !ok {
			sm.markReply[index] = make(chan SKVReply, 1)
		}
		sm.mu.Unlock()
		select {
		case r := <- sm.markReply[index]:
			// kv.debug("recv %v %v %v\n", r.ClientID, r.SerialID, r.WrongLeader)
			if r.ClientID == args.ClientID && r.SerialID == args.SerialID {
				// kv.debug("return\n")
				reply.WrongLeader = r.WrongLeader
				reply.Err = r.Err
				reply.Config = r.Config
				// *reply = r
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

func (sm *ShardMaster) handle() {
	skvreply := SKVReply {
		WrongLeader : true,
	}
	for {
		applych := <- sm.applyCh
		// kv.debug("Received from applyCh\n")
		index := applych.Index
		// kv.debug("Use snapshot %v\n", applych.UseSnapshot)
		skvargs := applych.Command.(SKVArgs)
		skvreply.OpType = skvargs.OpType
		// kv.debug("apply cid %v sid %v\n", kvargs.ClientID, kvargs.SerialID)

		sm.mu.Lock()
		_, ok := sm.markClient[skvargs.ClientID]

		if !ok || (ok && (sm.markClient[skvargs.ClientID] + 1) == skvargs.SerialID) {
			//not duplicate
			lastconfig := LastConfig(sm.configs)
			config := Config {
				Num : lastconfig.Num + 1,
				Groups : make(map[int][]string, 0),
			}			
			if skvargs.OpType == JOIN {
				config.Groups = skvargs.Servers
				keyNum := len(config.Groups)
				keys := make([]int, 0, keyNum)
				for k := range config.Groups {
					keys = append(keys, k)
				}
				for i := 0; i < NShards; i++ {
					config.Shards[i] = keys[i % keyNum]
				}
			}
			if skvargs.OpType == LEAVE {
				config.Groups = lastconfig.Groups
				for i := range config.Groups {
					delete(config.Groups, i)
				}
				keyNum := len(config.Groups)
				keys := make([]int, 0, keyNum)
				for k := range config.Groups {
					keys = append(keys, k)
				}
				for i := 0; i < NShards; i++ {
					config.Shards[i] = keys[i % keyNum]
				}
			}
			if skvargs.OpType == MOVE {
				config.Groups = lastconfig.Groups
				config.Shards = lastconfig.Shards
				config.Shards[skvargs.Shard] = skvargs.GID

			}
			if skvargs.OpType == QUERY {
				if skvargs.Num == -1 || skvargs.Num > lastconfig.Num{
					skvreply.Config = lastconfig
				} else {
					skvreply.Config = sm.configs[skvargs.Num]
				}
			}
			sm.configs = append(sm.configs, config)
		} // else: sliently ignore duplicate command.

		// Prepare the reply.
        skvreply.WrongLeader = false

		skvreply.ClientID = skvargs.ClientID
		skvreply.SerialID = skvargs.SerialID

		if _, ch := sm.markReply[index]; !ch {
			sm.markReply[index] = make(chan SKVReply, 1)
		}

		sm.markReply[index] <- skvreply
		sm.mu.Unlock()			
	}
}
// func (sm *ShardMaster) Join(args *SKVArgs, reply *SKVReply) {
// 	// Your code here.
// }

// func (sm *ShardMaster) Leave(args *SKVArgs, reply *SKVReply) {
// 	// Your code here.
// }

// func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
// 	// Your code here.
// }

// func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
// 	// Your code here.
// }


//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	gob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.markClient = make(map[int64]int, 0)
	sm.markReply = make(map[int] chan SKVReply, 0)
	go sm.handle()

	return sm
}
