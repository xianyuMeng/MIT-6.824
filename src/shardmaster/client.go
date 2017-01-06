package shardmaster

//
// Shardmaster clerk.
//

import "labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
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
	// Your code here.
	ck.ClientID = nrand()
	ck.SerialID = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &SKVArgs{}
	// Your code here.
	args.OpType = QUERY
	args.Num = num
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply SKVReply
			ok := srv.Call("ShardMaster.Exec", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &SKVArgs{}
	// Your code here.
	args.OpType = JOIN
	args.Servers = servers

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply SKVReply
			ok := srv.Call("ShardMaster.Exec", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &SKVArgs{}
	// Your code here.
	args.OpType = LEAVE
	args.GIDs = gids

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply SKVReply
			ok := srv.Call("ShardMaster.Exec", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &SKVArgs{}
	// Your code here.
	args.OpType = MOVE
	args.Shard = shard
	args.GID = gid

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply SKVReply
			ok := srv.Call("ShardMaster.Exec", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
