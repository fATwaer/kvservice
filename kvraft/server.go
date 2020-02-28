package raftkv

import (
	"bytes"
	"encoding/json"
	"kvservice/labgob"
	"kvservice/raft"
	"kvservice/realrpc"
	"log"
	"sync"
	"time"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type opeartion int
const (
	r = iota
	w
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Class opeartion // read or write
	Write PutAppendArgs
	Read  GetArgs
}



type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	tch chan Op //all get/write Op write into this channel
	store map[string]string //store the keys and values
	//opset map[int] int // cid -> seriesN
	cidRecord int

	// - A kvserver should not complete a Get() RPC if it is not part of a majority
	rpcMap map[UniqueID] chan string
	commitMap map[Namespace] int	// [sid, cid] -> seriesN

	// used to detect leadership
	leaderChange chan bool
}

func (kv *KVServer)Register(args *RegisterArgs, reply *RegisterReply) error {
	_, isLeader := kv.rf.GetState()
	//DPrintf("[Register] server %d is leader? %v", kv.me, isLeader)
	if !isLeader {
		reply.IsLeader = false
		return nil
	} else {
		reply.IsLeader = true
		kv.mu.Lock()
		kv.cidRecord += 1
		reply.Cid = kv.cidRecord
		kv.mu.Unlock()
		reply.Sid = kv.me
		DPrintf("register client %d", reply.Cid)
	}
	return nil
}

//func (kv *KVServer) passOp (id UniqueID) Err {
//	_, isLeader := kv.rf.GetState()
//	//DPrintf("[Put] server %d is leader? %v", kv.me, isLeader)
//
//	if !isLeader {
//		return ErrNotLeader
//	}
//
//	series, hasKey := kv.opset[id.Cid]
//	DPrintf("haskey %v, server series %d, args series %d", hasKey, series, id.SeriesN)
//	if !hasKey || series < id.SeriesN {
//		kv.opset[id.Cid] = id.SeriesN
//	} else {
//		return ErrDuplicateOp
//	}
//
//	return ""
//}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.

	// err := kv.passOp(args.Id)

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrNotLeader
		return nil
	}
	reply.WrongLeader = false


	DPrintf("%#v", args)

	// Operation valid
	// process Op

	done := make(chan string)
	kv.mu.Lock()
	// TODO
	if ch, ok := kv.rpcMap[args.Id]; ok {
		done = ch
	} else {
		kv.rpcMap[args.Id] = done
	}
	kv.mu.Unlock()


	rop := Op{
		Class: r,
		Write: PutAppendArgs{},
		Read:  *args,
	}

	// start the operation in raft
	kv.tch <- rop

	// wait for apply msg
	msg := <- done

	if msg == ErrNotLeader {
		reply.WrongLeader = true
		reply.Err = Err(msg)
	} else {
		reply.Value = msg
	}

	select {
	// this case for several reader has same UniqueID
	case done <- msg:

	// else clean the rpcMap and close channel
	default:
		close(done)
		kv.mu.Lock()
		delete(kv.rpcMap, args.Id)
		kv.mu.Unlock()
	}
	return nil
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrNotLeader
		return nil
	}
	reply.WrongLeader = false

	// Operation valid
	// process Op
	DPrintf("%#v", args)

	// 1. register channel
	done := make(chan string)
	kv.mu.Lock()
	kv.rpcMap[args.Id] = done
	kv.mu.Unlock()

	// 2. commit operation
	wop := Op{
		Class: w,
		Write: *args,
		Read:  GetArgs{},
	}
	kv.tch <- wop

	// 3. Put operation also need wait to
	// be committed in the raft's log
	msg := <- done
	if msg == ErrNotLeader {
		reply.Err = Err(msg)
		reply.WrongLeader = true
	}

	// 4. clean rpcMap
	kv.mu.Lock()
	delete(kv.rpcMap, args.Id)
	kv.mu.Unlock()

	return nil
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//func (kv *KVServer) cidGenerator() {
//	i := 0
//	for {
//		kv.cidCh <- i
//		i++
//	}
//}

func (kv *KVServer) leaderDetector() {
	for {
		time.Sleep(raft.ElectionTimeOut)
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			// leader change
			// tell client to find new leader
			kv.mu.Lock()
			for id, ch := range kv.rpcMap {
				DPrintf("%d detect not leader, return %#v rpc", kv.me, id)
				_ = id
				select {
				case ch <- ErrNotLeader:
				//default:
				}
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) stateCoordinator() {

	for {
		select {
		// handle only a request one time
		// in `select`
		case op := <- kv.tch:

			// 3b snapshot before commit an entry

			bytes, err := json.Marshal(op)

			// commit a operation, return immediately
			idx, term, isleader := kv.rf.Start(bytes)

			// todo if not leader return

			DPrintf("start (idx %d term %d leader %v) op %#v \n", idx, term, isleader, op)
			if err != nil {
				DPrintf("%s\n", err)
			}

		case committed := <- kv.applyCh:
			DPrintf("server %d receive new apply %#v\n", kv.me, committed)

			// no_op
			if committed.CommandValid == false {
				// snapshot
				if committed.CommandIndex == -1 {
					snapshot := committed.Command.(*raft.Snapshot)
					kv.decodeCommitMap(snapshot.CommitMap)
					kv.store = snapshot.StateMap
				}
				continue
			} else if v, ok := committed.Command.(int); ok && v == -1 {
				continue
			}

			// get an Op struct json data
			JsonData := committed.Command.([]byte)
			op := Op{}
			if err := json.Unmarshal(JsonData, &op); err != nil {
				DPrintf("%#v", err)
			}

			switch op.Class {
			case r:
				DPrintf("server %d apply rop %#v ", kv.me, op.Read)
				rop := op.Read
				kv.cleanRPC(rop.Id, kv.store[rop.Key])

			case w:
				DPrintf("server %d apply wop %#v ", kv.me, op.Write)

				err := kv.processWrite(op.Write)

				kv.cleanRPC(op.Write.Id, string(err))

			}

			// detects that the Raft state size is approaching this threshold
			if snap := kv.rf.DetectThreshold(kv.maxraftstate); snap == true {
				// this server at here is holding the raft lock,
				// because raft is in the appendEntry function
				stateMachineMap := map[string]string{}
				// deep copy there
				for k, v := range kv.store {
					stateMachineMap[k] = v
				}
				kv.rf.Snapshot(&raft.Snapshot{
					StateMap:         stateMachineMap,
					CommitMap:        raft.EncodeOne(kv.commitMap),
					LastIncludeIndex: committed.CommandIndex,
					LastIncludeTerm:  -1,
				})
			}

		}
	}
}

func (kv *KVServer) processWrite (wop PutAppendArgs) Err {

	id := wop.Id
	nid := Namespace{
		Sid: id.Sid,
		Cid: id.Cid,
	}

	if id.Cid > kv.cidRecord {
		kv.cidRecord = id.Cid
	}


	DPrintf("commit progress %#v ", kv.commitMap)
	if v, has := kv.commitMap[nid]; !has || id.SeriesN > v {
		// can write into state machine
		DPrintf("server %d %s ['%s':'%s'->'%s']\n",
			kv.me, wop.Op, wop.Key, wop.Value, kv.store[wop.Key])
		if wop.Op == "Put" {
			kv.store[wop.Key] = wop.Value
		} else {
			kv.store[wop.Key] += wop.Value
		}

		kv.commitMap[nid] = id.SeriesN

	} else {
		DPrintf("reject operation id %v", kv.commitMap[nid])
		return ErrDuplicateOp
	}

	return Err("")
}

func (kv *KVServer) cleanRPC (id UniqueID, msg string) {
	kv.mu.Lock()
	done, ok := kv.rpcMap[id]
	kv.mu.Unlock()

	if ok {
		// leader server
		done <- msg
	}
}

func (kv *KVServer) decodeCommitMap(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	tmpMap := map[Namespace]int{}
	if d.Decode(&tmpMap) != nil {
		panic("decode error")
	} else {
		kv.commitMap = tmpMap
	}
}

// read GOB-data from persister and
// set resume state machine
func (kv *KVServer) resumeState (persister *raft.Persister) {
	if persister.SnapshotSize() > 0 {
		snapshot := raft.ReadSnapshot(persister.ReadSnapshot())
		kv.store = make(map[string]string)
		kv.decodeCommitMap(snapshot.CommitMap)
		for k, v := range snapshot.StateMap {
			kv.store[k] = v
		}
		DPrintf("server %d resume to %#v", kv.me, kv.store)
	}
}

func (kv *KVServer) GetRaft ()*raft.Raft {
	return kv.rf
}



//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*realrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	DPrintf("[new server]")

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.store = make(map[string] string)
	kv.commitMap = make(map[Namespace] int)
	kv.rpcMap = make(map[UniqueID]chan string)
	/////////////////////////////////////////

	kv.applyCh = make(chan raft.ApplyMsg)


	// resume server state machine
	kv.resumeState(persister)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.tch = make(chan Op)
	go kv.stateCoordinator()
	//kv.cidCh = make(chan int)
	//kv.leaderChange = make
	//go kv.cidGenerator()
	kv.cidRecord = 0
	go kv.leaderDetector()

	return kv
}

