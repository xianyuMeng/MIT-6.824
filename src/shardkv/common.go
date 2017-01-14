package shardkv
import (
	"shardmaster"
)
//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
)

type Operation int

const (
	PUT Operation = iota
	GET Operation = iota
	APPEND Operation = iota
	RECONFIG Operation = iota
)

type Err string

type SKVArgs struct {
	OpType Operation
	Key string
	Value string
	Shard int
	ClientID int64
	SerialID int

	ConfigNum int
} 

type SKVReply struct {
	OpType Operation
	WrongLeader bool
	Err Err
	Value string

	ClientID int64
	SerialID int
}

// Put or Append
// type PutAppendArgs struct {
// 	// You'll have to add definitions here.
// 	Key   string
// 	Value string
// 	Op    string // "Put" or "Append"
// 	// You'll have to add definitions here.
// 	// Field names must start with capital letters,
// 	// otherwise RPC will break.
// }

// type PutAppendReply struct {
// 	WrongLeader bool
// 	Err         Err
// }

// type GetArgs struct {
// 	Key string
// 	// You'll have to add definitions here.
// }

// type GetReply struct {
// 	WrongLeader bool
// 	Err         Err
// 	Value       string
// }
