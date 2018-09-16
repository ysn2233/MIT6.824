package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
	"bytes"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	Operation	string
	Key 		string
	Value		string
	Seq			int64
	ClientId	int64
}

type RaftKV struct {
	mu      	sync.Mutex
	me      	int
	rf      	*raft.Raft
	kvData  	map[string]string 	// kv database
	applyCh 	chan raft.ApplyMsg
	opLogs  	map[int]chan Op		// stored command based on log index
	clientSeq	map[int64]int64		// record the sequence number of each client

	maxraftstate int // snapshot if log grows this big

}


func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	op := Op{Operation: "Get", Key: args.Key, Seq: args.Seq, ClientId: args.ClientId}
	result := kv.runOp(op)
	if result != OK {
		reply.WrongLeader = true
		reply.Err = result
	} else {
		kv.mu.Lock()
		reply.WrongLeader = false
		val, ok := kv.kvData[args.Key]
		if !ok {
			reply.Err = ErrNoKey
		} else {
			reply.Err = OK
			reply.Value = val
		}
		kv.mu.Unlock()
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{Operation: args.Op, Key: args.Key, Value: args.Value, Seq: args.Seq, ClientId: args.ClientId}
	result := kv.runOp(op)
	if result != OK {
		reply.WrongLeader = true
		reply.Err = result
	} else {
		reply.WrongLeader = false
		reply.Err = OK
	}

}

func (kv * RaftKV) runOp(op Op) Err {
	// agreement with raft
	idx, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return ErrWrongLeader
	}
	// create a Op channel to store command log based on log index
	// because it needs to be consistent with the common written into raft logs
	kv.mu.Lock()
	opCh, ok := kv.opLogs[idx]
	if !ok {
		opCh = make(chan Op)
		kv.opLogs[idx] = opCh
	}
	kv.mu.Unlock()
	defer func() {
		kv.mu.Lock()
		delete(kv.opLogs, idx)
		kv.mu.Unlock()
	}()
	select {
		case logOp := <- opCh:
			if logOp == op {
				return OK
			} else {
				return ErrWrongLeader
			}
		case <- time.After(2000 * time.Millisecond):
			return Timeout
		}
}

// a go-routine function listen to the applyCh channel from raft
func (kv *RaftKV) ApplyLoop() {
	for {
		msg := <- kv.applyCh
		if msg.UseSnapshot {
			var LastIncludedIndex int
			var LastIncludedTerm int
			c := bytes.NewBuffer(msg.Snapshot)
			decoder := gob.NewDecoder(c)
			kv.kvData = make(map[string]string)
			kv.opLogs = make(map[int]chan Op)
			kv.mu.Lock()
			decoder.Decode(&LastIncludedIndex)
			decoder.Decode(&LastIncludedTerm)
			decoder.Decode(&kv.kvData)
			decoder.Decode(&kv.opLogs)
			kv.mu.Unlock()
		} else {
			kv.mu.Lock()
			index := msg.Index
			op := msg.Command.(Op)
			if op.Seq > kv.clientSeq[op.ClientId] {
				switch op.Operation {
				case "Put":
					kv.kvData[op.Key] = op.Value
				case "Append":
					if _, ok := kv.kvData[op.Key]; !ok {
						kv.kvData[op.Key] = op.Value
					} else {
						kv.kvData[op.Key] += op.Value
					}
				default:
				}
				kv.clientSeq[op.ClientId] = op.Seq
			}
			opCh, ok := kv.opLogs[index]
			if ok {
				// clear the channel
				select {
				case <- opCh:
				default:
				}
				opCh <- op
			}

			if kv.maxraftstate != -1 && kv.rf.PersisterSize() > kv.maxraftstate {
				w := new(bytes.Buffer)
				e := gob.NewEncoder(w)
				e.Encode(kv.kvData)
				e.Encode(kv.opLogs)
				data := w.Bytes()
				go kv.rf.GoSnapshot(msg.Index, data)
			}
			kv.mu.Unlock()

		}

	}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.kvData = make(map[string]string)
	kv.opLogs = make(map[int]chan Op)
	kv.clientSeq = make(map[int64]int64)

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.ApplyLoop()
	// You may need initialization code here.

	return kv
}
