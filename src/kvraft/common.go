package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

type Operation int

const (
	PUT Operation = iota
	GET Operation = iota
	APPEND Operation = iota
)

type KVArgs struct{
	OpType Operation
	Key string 
	Value string
	ClientID int64
	SerialID int
}

type KVReply struct{
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
