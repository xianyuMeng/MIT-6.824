package shardkv

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

type Err string

type RequestArgs struct {
	Op 		string 	// "PUT" or "APPEND" or "GET"
	Key 	string
	Value 	string
	Id 		int64
	Sid 	int
}

type RequestReply struct {
	WrongLeader bool
	Err 		Err
	Value 		string
}
