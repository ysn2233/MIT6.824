package raftkv

import (
	"labrpc"
	"time"
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
	// You'll have to add code here.
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

	args := GetArgs{
		Key: key,
	}

	i := ck.leader
	for {
		time.Sleep(time.Millisecond*300)
		var reply GetReply
		ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
		if ok && !reply.WrongLeader{
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

	args := PutAppendArgs {
		Key:	key,
		Value:	value,
		Op:		op,
	}

	i := ck.leader
	for  {
		time.Sleep(time.Millisecond*300)
		var reply PutAppendReply
		ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
		if ok && !reply.WrongLeader{
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
