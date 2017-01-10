package shardmaster

import (
	//"container/heap"
	"encoding/gob"
	"labrpc"
	"raft"
	"sync"
	"time"
	//"log"
	"fmt"
	"sort"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		fmt.Printf(format, a...)
	}
	return
}
func (sm *ShardMaster)PrintShard(s [NShards]int) {
	if Debug > 0 {
		for i, g := range s {
			sm.debug("shard %v -> gid %v\n", i, g)
		}		
	}
}
type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	//ClientID -> SerialID
	markClient map[int64]int
	//index -> reply
	markReply map[int]chan SKVReply

	configs []Config // indexed by config num
}

func (sm *ShardMaster) debug(format string, a ...interface{}) (n int, err error) {
	a = append(a, 0)
	copy(a[1:], a[0:])
	a[0] = sm.me
	n, err = DPrintf("SKVRaft %v "+format, a...)
	return
}

type Op struct {
	// Your data here.
}

func LastConfig(configs []Config) Config {
	return configs[len(configs)-1]
}

type Item struct {
	GID    int
	Shards []int
	Num    int
}

type Items []Item

func (g Items) Len() int {
	return len(g)
}

func (g Items) Swap(i, j int) {
	g[i], g[j] = g[j], g[i]
}

func (g Items) Less(i, j int) bool {
	return g[i].Num < g[j].Num
}

func (sm *ShardMaster) MoveShards(config *Config) {

	gid_to_shards := make(map[int]Item, 0)
	leavegid := make([]int, 0)
	newgid := make([]int, 0)
	leave_gid_to_shards := make(map[int]Item, 0)

	for s, g := range config.Shards {
		if _, ok := config.Groups[g]; !ok {
			//this is gid that left the group
				leavegid = append(leavegid, g)
				sm.debug("leave gid %v for shard %v\n", g, s)
				if _, ok := leave_gid_to_shards[g]; !ok {
					shards := []int{s}
					item := Item{
						GID:    g,
						Shards: shards,
						Num:    1,
					}
					leave_gid_to_shards[g] = item
				} else {
					item := Item{
						Num:    gid_to_shards[g].Num + 1,
						GID:    g,
						Shards: make([]int, len(leave_gid_to_shards[g].Shards)),
					}
					copy(item.Shards, leave_gid_to_shards[g].Shards)
					item.Shards = append(item.Shards, s)
					leave_gid_to_shards[g] = item
				}

		} else {
			//this is old gid
			if _, ok := gid_to_shards[g]; !ok {
				//check if the gid is in the map
				shards := []int{s}
				item := Item{
					GID:    g,
					Shards: shards,
					Num:    1,
				}
				gid_to_shards[g] = item
			} else {
				item := Item{
					Num: gid_to_shards[g].Num + 1,
					GID: g,
				}
				item.Shards = make([]int, len(gid_to_shards[g].Shards))
				copy(item.Shards, gid_to_shards[g].Shards)
				item.Shards = append(item.Shards, s)
				gid_to_shards[g] = item
			}
		}
	}
	for k, _ := range config.Groups {
		if _, ok := gid_to_shards[k]; !ok {
			//this is new gid
			item := Item{
				Num:    0,
				GID:    k,
				Shards: make([]int, 0),
			}
			newgid = append(newgid, k)
			gid_to_shards[k] = item
			sm.debug("new gid %v\n", k)
		}
	}

	items := make(Items, 0)
	for _, v := range gid_to_shards {
		items = append(items, v)
	}
	//shard number that should to be assigned to another gid
	shards_to_move := make([]int, 0)
	sort.Sort(items)

	ave_anticipated := len(config.Shards) / (len(items))
	ret := len(config.Shards) % (len(items))
	sm.debug("ave_anticipated is %v\n", ave_anticipated)
	if len(newgid) > 0 {	
		tmp := 0
		for i := len(items) - 1; i >= 0; i = i - 1 {
			if len(items[i].Shards) > ave_anticipated {
				if tmp < ret {
					shards_to_move = append(shards_to_move, items[i].Shards[ave_anticipated+1:]...)
				} else {
					shards_to_move = append(shards_to_move, items[i].Shards[ave_anticipated:]...)
				}
				tmp += 1
			}
		}

		for i := 0; i < len(shards_to_move); i++ {
			config.Shards[shards_to_move[i]] = newgid[i%len(newgid)]
		}
	}
	if len(leavegid) > 0 {
		//sm.debug("should remove gid %v\n", len(leavegid))
		
		for _, v := range leave_gid_to_shards {
			for _, s := range v.Shards {
				config.Shards[s] = items[0].GID
				items[0].Shards = append(items[0].Shards, s)
				items[0].Num += 1
				sort.Sort(items)
			}
		}
	}
}
func (sm *ShardMaster) Exec(args *SKVArgs, reply *SKVReply) {
	index, _, isLeader := sm.rf.Start(*args)
	//sm.debug("exec...\n")

	if isLeader == false {
		reply.WrongLeader = true
		//sm.debug("WrongLeader\n")
		return
	} else {
		sm.mu.Lock()
		if _, ok := sm.markReply[index]; !ok {
			sm.markReply[index] = make(chan SKVReply, 1)
		}
		sm.mu.Unlock()

		select {
		case r := <-sm.markReply[index]:
			sm.debug("recv %v %v %v\n", r.ClientID, r.SerialID, r.WrongLeader)
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
	skvreply := SKVReply{
		WrongLeader: true,
	}
	for {
		applych := <-sm.applyCh
		//sm.debug("Received from applyCh\n")
		index := applych.Index
		// kv.debug("Use snapshot %v\n", applych.UseSnapshot)
		skvargs := applych.Command.(SKVArgs)
		skvreply.OpType = skvargs.OpType
		// kv.debug("apply cid %v sid %v\n", kvargs.ClientID, kvargs.SerialID)

		sm.mu.Lock()
		_, ok := sm.markClient[skvargs.ClientID]

		if !ok || (ok && (sm.markClient[skvargs.ClientID]+1) == skvargs.SerialID) {
			//not duplicate
			lastconfig := LastConfig(sm.configs)
			config := Config{
				Num:    lastconfig.Num + 1,
				Groups: make(map[int][]string, 0),
			}

			for k, v := range lastconfig.Groups {
				config.Groups[k] = v
			}

			for i, v := range lastconfig.Shards {
				config.Shards[i] = v
				//sm.debug("shard %v -> gid %v\n", i, v)
			}
			sm.debug("last config has %v gids\n", len(config.Groups))

			if skvargs.OpType == JOIN {
				for k, v := range skvargs.Servers {
					config.Groups[k] = v
					sm.debug("gid %v join the group\n", k)
				}
				sm.MoveShards(&config)
				sm.PrintShard(config.Shards)
			}


			if skvargs.OpType == LEAVE {
				for _, g := range skvargs.GIDs {
					delete(config.Groups, g)
					sm.debug("delete gid %v\n", g)
				}
				sm.MoveShards(&config)
				sm.PrintShard(config.Shards)
			}
			if skvargs.OpType == MOVE {
				config.Shards[skvargs.Shard] = skvargs.GID
				sm.debug("move shard %v -> gid %v\n", skvargs.Shard, skvargs.GID)
				sm.PrintShard(config.Shards)
				//sm.MoveShards(&config)
			}

			if skvargs.OpType == QUERY {
				if skvargs.Num == -1 || skvargs.Num > lastconfig.Num {
					skvreply.Config = lastconfig
				} else {
					skvreply.Config = sm.configs[skvargs.Num]
				}
			}
			if skvargs.OpType != QUERY{
				sm.configs = append(sm.configs, config)
			}
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
	gob.Register(SKVReply{})
	gob.Register(SKVArgs{})

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	gob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.markClient = make(map[int64]int, 0)
	sm.markReply = make(map[int]chan SKVReply, 0)
	go sm.handle()

	return sm
}
