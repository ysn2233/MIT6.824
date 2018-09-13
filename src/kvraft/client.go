package raftkv

import (
	"labrpc"
)
import "crypto/rand"
import (
	"math/big"
	"sync"
)


type Clerk struct {
	servers 	[]*labrpc.ClientEnd
	n 			int
	leader		int
	mu			sync.Mutex
	seq			int64
	clientId	int64
	// You will have to modify this struct.
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
	ck.n = len(ck.servers)
	ck.leader = 0
	ck.seq = 0	// sequence number to identify the sent message to avoid duplicate
	ck.clientId = nrand()

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
	ck.seq++

	args := GetArgs{
		Key: 		key,
		Seq: 		ck.seq,
		ClientId:	ck.clientId,
	}
	i := ck.leader

	for {
		var reply GetReply
		ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
		if !ok {
			continue
		}
		if reply.Err=="OK" && !reply.WrongLeader{
			ck.mu.Lock()
			ck.leader = i
			ck.mu.Unlock()
			return reply.Value
		} else {
			i = (i + 1) % ck.n
		}
	}
	return ""
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
	ck.seq++

	args := PutAppendArgs {
		Key:	key,
		Value:	value,
		Op:		op,
		Seq:	ck.seq,
		ClientId: ck.clientId,
	}

	i := ck.leader
	for  {
		// time.Sleep(100 * time.Millisecond)
		var reply PutAppendReply
		ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
		if reply.Err=="OK" && !reply.WrongLeader{
			ck.mu.Lock()
			ck.leader = i
			ck.mu.Unlock()
			return
		} else {
			i = (i + 1) % ck.n
		}
	}


}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
