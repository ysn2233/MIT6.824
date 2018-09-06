package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	Timeout = "Timeout"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   		string
	Value 		string
	Op    		string // "Put" or "Append"
	Seq	  		int64
	ClientId	int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key 		string
	Seq 		int64
	ClientId	int64
	// You'll have to add definitions here.
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}
