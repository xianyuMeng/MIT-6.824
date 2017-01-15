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
	READY Operation = iota
	PULLSHARD Operation = iota
)

type PullOperation int
const (
	REJECT PullOperation = iota
	DUPLICATE PullOperation = iota
	DONE PullOperation = iota
)
type Op struct {
	OpType 	Operation
	Command interface{}
}
type SendShardArgs struct {
	ConfigNum int
	ShardKV map[string]string
	Shard int
	ReceiverGID int
	SenderGID int
	markClient map[int64]int
}

type SendShardReply struct {
	Success bool
	ConfigNum int
	ShardKV  int
}

type AddShard struct {
	Args SendShardArgs
	Reply bool
}

type Err string

type SKVArgs struct {
	OpType Operation
	Key string
	Value string
	Shard int
	ClientID int64
	SerialID int

	ConfigNum int

	//for reconfig
	Config shardmaster.Config
} 

type SKVReply struct {
	OpType Operation
	WrongLeader bool
	Err Err
	Value string

	ClientID int64
	SerialID int

	ConfigNum int
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
