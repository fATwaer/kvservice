package raftkv

import (
	"labrpc"
	"time"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader int
	cid	   int // client id
	sid    int // server id
	seriesN int
	failedDelay time.Duration
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	DPrintf("make new clerk")
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.failedDelay = 1 * time.Second
	ck.seriesN = 0
	ck.cid = -1
	ck.leader = -1

	return ck
}

func (ck *Clerk) register() {
	DPrintf("---leader change---")

	find := make(chan bool)

	for {
		nleader := 0
		// rpc to all server to find leader
		for i := range ck.servers {
			go func(idx int) {
				args := RegisterArgs{}
				reply := RegisterReply{}
				ok := ck.servers[idx].Call("KVServer.Register", &args, &reply)
				if !ok {
					//DPrintf("not ok")
					find <- false
				}
				if reply.IsLeader {
					ck.cid = reply.Cid
					ck.sid = reply.Sid
					ck.leader = idx
					find <- true
				} else {
					find <- false
				}
			}(i)
		}

		// count the leader number
		for i := range ck.servers {
			_ = i
			isleader := <- find
			if isleader {
				nleader += 1
			}
		}
		DPrintf("nleader %d\n", nleader)
		if nleader == 1 {
			break
		}
		time.Sleep(ck.failedDelay)
	}

	DPrintf("[Register] cid %d sid %d \n", ck.cid, ck.sid)
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	if ck.cid == -1 {
		ck.register()
	}

	//Lab3A hint:
	// - It's OK to assume that a client will make only one call into a Clerk at a time.
	ck.seriesN += 1

	args := GetArgs{
		Key:     key,
		Id:UniqueID{
			Cid:     ck.cid,
			Sid:     ck.sid,
			SeriesN: ck.seriesN,
		},
	}
	res := GetReply{}

	for {
		reply := GetReply{}
		DPrintf("send %d server get request\n", ck.leader)
		for {
			ok := ck.servers[ck.leader].Call("KVServer.Get", &args, &reply)
			if ok == true {
				break
			}
		}
		DPrintf("%#v", reply)
		//DPrintf("%d rpc done, is leader? %v\n", ck.leader, !reply.WrongLeader)

		// true leader, rpc done
		if !reply.WrongLeader {
			DPrintf("[Get] Key '%s' Value '%s'\n", args.Key, reply.Value)
			res = reply
			break
		}

		// not leader may across network partition
		// get new leader and cid, then retry
		ck.register()

	}
	return res.Value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	if ck.cid == -1 {
		ck.register()
	}

	//Lab3A hint:
	// - It's OK to assume that a client will make only one call into a Clerk at a time.
	DPrintf("new put op\n")
	ck.seriesN += 1
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		Id:	UniqueID{
			Cid:   ck.cid,
			SeriesN: ck.seriesN,
			Sid: ck.sid,
		},
	}


	for {

		reply := PutAppendReply{}
		DPrintf("send server %d put request\n", ck.leader)
		for {
			ok := ck.servers[ck.leader].Call("KVServer.PutAppend", &args, &reply)
			//fmt.Println(ok)
			if ok == true {
				break
			}
			DPrintf("[%#v]rpc fault \n", args)
		}

		//DPrintf("%d rpc done, is leader? %v\n", ck.leader, !reply.WrongLeader)
		// true leader, rpc done
		if !reply.WrongLeader {
			DPrintf("put '%s'->'%s' RPC done ", key, value)
			break
		}

		if reply.Err != "" {
			DPrintf("Err: %s\n", reply.Err)
		}


		// not leader may across network partition
		// get new leader and cid, but the cid/sid/seriesN of
		// this operation will not change, just redirect the
		// server. every operation has unique id and cannot be
		// duplicated.
		ck.register()
	}

}




func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
