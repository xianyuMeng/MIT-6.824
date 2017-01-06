package shardmaster
//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// A GID is a replica group ID. GIDs must be uniqe and > 0.
// Once a GID joins, and leaves, it should never join again.
//
// You will need to add fields to the RPC arguments.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK = "OK"
)

type Operation int

const (
	JOIN Operation = iota
	LEAVE Operation = iota
	MOVE Operation = iota
	QUERY Operation = iota
)

type Err string

type SKVArgs struct {
	OpType Operation
	Servers map[int][]string
	GIDs []int
	Shard int
	GID int
	Num int

	ClientID int64
	SerialID int
}

type SKVReply struct {
	OpType Operation
	WrongLeader bool
	Err Err
	Config Config

	ClientID int64
	SerialID int
}
// type JoinArgs struct {
// 	Servers map[int][]string // new GID -> servers mappings
// }

// type JoinReply struct {
// 	WrongLeader bool
// 	Err         Err
// }

// type LeaveArgs struct {
// 	GIDs []int
// }

// type LeaveReply struct {
// 	WrongLeader bool
// 	Err         Err
// }

// type MoveArgs struct {
// 	Shard int
// 	GID   int
// }

// type MoveReply struct {
// 	WrongLeader bool
// 	Err         Err
// }

// type QueryArgs struct {
// 	Num int // desired config number
// }

// type QueryReply struct {
// 	WrongLeader bool
// 	Err         Err
// 	Config      Config
// }
