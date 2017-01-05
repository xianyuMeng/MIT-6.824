package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
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

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	markClient map[int64]int
	// key : ClientID; value : SerialID
	markReply map[int]chan KVReply
	// key : index (replied by func "start"); value : KVReply
	markRequest map[string]string
}

// Helper function to print debug info for this server.
func (kv *RaftKV) debug(format string, a ...interface{}) (n int, err error) {
    a = append(a, 0)
    copy(a[1:], a[0:])
    a[0] = kv.me
    n, err = DPrintf("KVRaft %v " + format, a...)
    return
}

func (kv *RaftKV) Exe(args *KVArgs, reply *KVReply) {

	//markRequest[args.Key] = args.Value

	index, _, isLeader := kv.rf.Start(*args)

	if isLeader == false {
		reply.WrongLeader = true
		return
	} else {
		// kv.debug("start %v %v at %v\n", args.ClientID, args.SerialID, index)
		kv.mu.Lock()
		if _, ok := kv.markReply[index]; !ok {
			kv.markReply[index] = make(chan KVReply, 1)
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

func (kv *RaftKV) Run() {
	reply := KVReply{
		WrongLeader: true,
	}
	for {
		applych := <-kv.applyCh

		if applych.UseSnapshot == true {
			newmarkClient := make(map[int64]int, 0)
			newmarkRequest := make(map[string]string, 0)
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
			kvargs := applych.Command.(KVArgs)

			// kv.debug("apply cid %v sid %v\n", kvargs.ClientID, kvargs.SerialID)

			kv.mu.Lock()
			_, ok := kv.markClient[kvargs.ClientID]

			if !ok || (ok && (kv.markClient[kvargs.ClientID]+1) == kvargs.SerialID) {
				//not duplicate
				if kvargs.OpType == PUT {
					kv.markRequest[kvargs.Key] = kvargs.Value
				}
				if kvargs.OpType == APPEND {
					kv.markRequest[kvargs.Key] = kv.markRequest[kvargs.Key] + kvargs.Value
				}
				kv.markClient[kvargs.ClientID] = kvargs.SerialID

			} // else: sliently ignore duplicate command.

			// Prepare the reply.
	        reply.WrongLeader = false
			reply.OpType = kvargs.OpType
			if _, ok := kv.markRequest[kvargs.Key]; !ok {
				reply.Err = ErrNoKey
			} else {
				reply.Err = OK
				reply.Value = kv.markRequest[kvargs.Key]
			}
			reply.ClientID = kvargs.ClientID
			reply.SerialID = kvargs.SerialID

			if _, ch := kv.markReply[index]; !ch {
				kv.markReply[index] = make(chan KVReply, 1)
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

// func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
// 	// Your code here.
// }

// func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
// 	// Your code here.
// }

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})
	gob.Register(KVReply{})
	gob.Register(KVArgs{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.markClient = make(map[int64]int)
	kv.markReply = make(map[int]chan KVReply)
	kv.markRequest = make(map[string]string)
	go kv.Run()

	return kv
}
