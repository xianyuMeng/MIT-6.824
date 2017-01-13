package shardkv

import (
	"shardmaster"
	"labrpc"
	"raft"
	"sync"
	"encoding/gob"
	"time"
	"bytes"
)

type ServerType int
const (
	Working ServerType = iota
	ReConfig ServerType = iota
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
	state ServerType

	markClient map[int64]int
	markRequest map[int]map[string]string
	//this is the map which stores key-value
	//shard number -> (key -> value)
	markReply map[int]chan SKVReply

	configs []shardmaster.Config

	mck *shardmaster.Clerk
}

func (kv *ShardKV) LastConfigNum() int {
	if len(kv.configs) == 0 {
		return 0
	} else {
		return kv.configs[len(kv.configs) - 1].Num		
	}
}
func (kv *ShardKV) Exec (args *SKVArgs, reply *SKVReply) {
	index, _, isLeader := kv.rf.Start(*args)

	if isLeader == false {
		reply.WrongLeader = true
		return
	} else {
		// kv.debug("start %v %v at %v\n", args.ClientID, args.SerialID, index)

		kv.mu.Lock()
		if _, ok := kv.markReply[index]; !ok {
			kv.markReply[index] = make(chan SKVReply, 1)
		}
		kv.mu.Unlock()
		select {
		case r := <-kv.markReply[index]:
			// kv.debug("recv %v %v %v\n", r.ClientID, r.SerialID, r.WrongLeader)
			if r.ClientID == args.ClientID && r.SerialID == args.SerialID {
				// kv.debug("return\n")
				reply.WrongLeader = r.WrongLeader
				reply.Err = r.Err
				reply.Value = r.Value
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

func (kv *ShardKV) Run() {
	reply := SKVReply{
		WrongLeader: true,
	}
	for {
		applych := <-kv.applyCh

		if applych.UseSnapshot == true {
			newmarkClient := make(map[int64]int, 0)
			newmarkRequest := make(map[int]map[string]string, 0)
			var lastincludeindex int
			var lastincludeterm int
			if len(applych.Snapshot) == 0 {
				return
			}
			r := bytes.NewBuffer(applych.Snapshot)
			d := gob.NewDecoder(r)
			d.Decode(&lastincludeindex)
			d.Decode(&lastincludeterm)
			d.Decode(&newmarkClient)
			d.Decode(&newmarkRequest)
			kv.markClient = newmarkClient
			kv.markRequest = newmarkRequest
		} else { 
			// kv.debug("Received from applyCh\n")
			index := applych.Index
			// kv.debug("Use snapshot %v\n", applych.UseSnapshot)
			skvargs := applych.Command.(SKVArgs)

			// kv.debug("apply cid %v sid %v\n", kvargs.ClientID, kvargs.SerialID)

			kv.mu.Lock()
			_, ok := kv.markClient[skvargs.ClientID]

			if !ok || (ok && (kv.markClient[skvargs.ClientID] + 1) == skvargs.SerialID) {
				//not duplicate
				if skvargs.OpType == PUT {
					kv.markRequest[skvargs.Shard] = make(map[string]string, 0)
					var tmp map[string]string
					tmp[skvargs.Key] = skvargs.Value
					kv.markRequest[skvargs.Shard] = tmp
				}
				if skvargs.OpType == APPEND {
					kv.markRequest[skvargs.Shard][skvargs.Key] = kv.markRequest[skvargs.Shard][skvargs.Key] + skvargs.Value
				}
				kv.markClient[skvargs.ClientID] = skvargs.SerialID

			} // else: sliently ignore duplicate command.

			// Prepare the reply.
	        reply.WrongLeader = false
			reply.OpType = skvargs.OpType
			if _, ok := kv.markRequest[skvargs.Shard][skvargs.Key]; !ok {
				reply.Err = ErrNoKey
			} else {
				reply.Err = OK
				reply.Value = kv.markRequest[skvargs.Shard][skvargs.Key]
			}
			reply.ClientID = skvargs.ClientID
			reply.SerialID = skvargs.SerialID

			if _, ch := kv.markReply[index]; !ch {
				kv.markReply[index] = make(chan SKVReply, 1)
			}

			kv.markReply[index] <- reply

			//maxraftsize might be -1
			if kv.rf.GetStateSize() > kv.maxraftstate && kv.maxraftstate >= 0 {
				w := new(bytes.Buffer)
	    		e := gob.NewEncoder(w)
	    		e.Encode(kv.markClient)
	    		e.Encode(kv.markRequest)
	    		data := w.Bytes()

	    		go kv.rf.Snapshot(data, index)
	    		//kv.debug("Start snapshot for %v\n", index)
			} 
			kv.mu.Unlock()
		}
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
		go func() {
			okconfig := make(chan shardmaster.Config, 0)
			go func() {
				okconfig <- kv.mck.Query(kv.LastConfigNum() + 1)
			}()
			select {
				case ok := <- okconfig :
					if ok.Num != (kv.LastConfigNum() + 1) ||  kv.state != Working {
						time.Sleep(50 * time.Millisecond)
					} else {
						// need to reconfig
						kv.mu.Lock()
						kv.state = ReConfig
						kv.mu.Unlock()
						recong := SKVArgs {
							OpType : RECONFIG,
							Config : ok,
						}
						_, _, isLeader := kv.rf.Start(recong)
						if isLeader == true {
							kv.configs = append(kv.configs, ok)
						} 
					}
				case <-time.After(50 * time.Millisecond) :
			}
		}()
		time.Sleep(50 * time.Millisecond)
	}	
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
	kv.markReply = make(map[int]chan SKVReply, 0)
	kv.configs = make([]shardmaster.Config, 0)
	kv.state = Working
	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)


	return kv
}
