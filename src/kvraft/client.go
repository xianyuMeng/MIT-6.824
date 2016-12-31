package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	ClientID int64
	SerialID int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.ClientID = nrand()
	ck.SerialID = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := KVArgs {
		Key : key,
		OpType : GET,
	}
	reply := ck.handle(&args)
	if reply.Err == ErrNoKey{
		return ""
	} else{
		return reply.Value
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := KVArgs{
		Key : key,
		Value : value,
	}
	if op == "Put" {
		args.OpType = PUT
	} else if op == "Append" {
		args.OpType = APPEND
	}
	ck.handle(&args)
	return
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) handle (Args *KVArgs) KVReply {
	//got new request
	ck.SerialID ++
	Args.ClientID = ck.ClientID
	Args.SerialID = ck.SerialID
	
	for i := 0; ; i++{
		var worker int
		if i >= len(ck.servers) {
			worker = i % len(ck.servers)
		} else {
			worker = i
		}

		Reply := KVReply{}

		ok := ck.servers[worker].Call("RaftKV.Exe", Args, &Reply)
		if ok {
			if Reply.WrongLeader {
				continue
			} else {
				return Reply
			}			
		}

	}
}
