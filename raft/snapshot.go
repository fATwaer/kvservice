package raft

import (
	"bytes"
	"fmt"
	"kvservice/labgob"
)

type Snapshot struct {
	StateMap map[string] string
	CommitMap []byte
	LastIncludeIndex int
	LastIncludeTerm int
}

func (rf *Raft) DetectThreshold (maxRaftState int) bool {
	if maxRaftState == -1 {
		return false
	}
	currentRaftStateSize := rf.persister.RaftStateSize()
	fmt.Printf("%d detect current Raft size: %d, maxstate size: %d\n",
		rf.me, currentRaftStateSize, maxRaftState)
	if currentRaftStateSize <= maxRaftState {
		return false
	} else {
		return true
	}
}

func (rf *Raft) Snapshot (snapshot *Snapshot) {
	fmt.Printf("%d snap begin at %d, log len %d\n",
		rf.me, snapshot.LastIncludeIndex, len(rf.log))


	rf.mu.Lock()
	defer fmt.Printf("%d snapshot locker unlocked\n", rf.me)
	defer rf.mu.Unlock()

	logIdx := rf.logIndex(snapshot.LastIncludeIndex)
	snapshot.LastIncludeTerm = rf.log[logIdx].Term

	//w := new(bytes.Buffer)
	//e := labgob.NewEncoder(w)
	//e.Encode(snapshot)
	snap := EncodeOne(snapshot)

	// trim raft log
	rf.log = rf.log[logIdx+1:]
	rf.snapshotLastIndex = snapshot.LastIncludeIndex
	rf.snapshotLastTerm = snapshot.LastIncludeTerm

	state := rf.PersistentState()
	rf.persister.SaveStateAndSnapshot(state, snap)

	fmt.Printf("%d snap done, rf log length %d\n", rf.me, len(rf.log))
}


func (rf *Raft) WriteSnapshot (offset int, data []byte) {
	_ = offset
	logData := EncodeOne(rf.log)
	rf.persister.SaveStateAndSnapshot(logData, data)
	rf.persist()
}

func ReadSnapshot (data []byte) *Snapshot {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	snap := &Snapshot{}
	if d.Decode(snap) != nil {
		panic("decode data error")
	} else {
		DPrintf("read snapshot %#v", snap)
		return snap
	}
}
// calculate the real index
func (rf *Raft) realTerm(entryIndex int) int {
	DPrintf("[realTerm] log len: %d entryIndex: %d, snapshot: %d",
		len(rf.log), entryIndex, rf.snapshotLastIndex)
	if len(rf.log) == 0 {
		return rf.snapshotLastTerm
	}

	if rf.snapshotLastIndex == entryIndex {
		return rf.snapshotLastTerm
	}

	if rf.snapshotLastIndex != 0 {
		return rf.log[entryIndex - rf.snapshotLastIndex - 1].Term
	}
	return rf.log[entryIndex].Term
}

func (rf *Raft) lastIndex() int {
	if len(rf.log) == 0 {
		return rf.snapshotLastIndex
	}
	return rf.log[len(rf.log)-1].Index
}

// TODO: remove this function
// this index can be calculate by lastIndex
func (rf *Raft) logIndex (entryIndex int) int {
	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Index == entryIndex {
			return i
		}
	}
	return -1
}





// part II



type InstallSnapshotArgs struct {
	Term 		int
	LeaderId	int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset		int
	Data		[]byte
	Done		bool
}

type InstallSnapshotReply struct {
	Term 		int
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	DPrintf("[snapshot] %d->%d\n\t\t%#v", args.LeaderId, rf.me, args)

	DPrintf("\t[snapshot] %d server try lock", rf.me)
	if rf.currentTerm == args.Term {
		rf.ElectionTimeOut()
	}

	rf.mu.Lock()
	DPrintf("\t[snapshot] %d server locked", rf.me)
	defer DPrintf("\t[snapshot] %d server done", rf.me)
	defer rf.mu.Unlock()


	// 1. Reply immediately if term < currentTerm
	if rf.currentTerm > args.Term {
		DPrintf("\t[snapshot] %d server term not right", rf.me)
		reply.Term = rf.currentTerm
		return
	}
	DPrintf("\t[snapshot] %d server term right", rf.me)

	// 2. Create new snapshot file if first chunk (offset is 0)
	// 3. Write data into snapshot file at given offset
	if args.Offset == 0 {
		// create snapshot file
	} else {
		// Offset implementation
		panic("Offset not implement")
	}

	// 4. Reply and wait for more data chunks if done is false
	if args.Done == false {
		panic("done == false") // not implement
		return
	}

	// 5. Save snapshot file, discard any existing or partial snapshot
	//    with a smaller index
	if args.LastIncludedIndex <= rf.snapshotLastIndex {
		DPrintf("\t[snapshot] %d server snapshot size > %d snapshot size", rf.me, args.LeaderId)
		return
	}



	//6. If existing log entry has same index and term as snapshot’s
	//   last included entry, retain log entrines following it and reply (it: same entry)
	if rf.lastIndex() > args.LastIncludedIndex {
		sameIdx := args.LastIncludedIndex
		if rf.snapshotLastIndex  != 0 {
			sameIdx -= rf.snapshotLastIndex+1
		}
		DPrintf("\t[snapshot] %d lastIndex %d arg.lastIndex %d snapshot %d sameIdx %d len log %d",
			rf.me,
			rf.lastIndex(), args.LastIncludedIndex,
			rf.snapshotLastIndex, sameIdx, len(rf.log))
		if len(rf.log) > sameIdx && rf.log[sameIdx].Term == args.LastIncludedTerm {
			DPrintf("\t[snapshot] server %d trim at %d]", rf.me, sameIdx)
			trimmedLog := []Entry{}
			rf.log = append(trimmedLog, rf.log[sameIdx+1:]...)
			rf.snapshotLastIndex = args.LastIncludedIndex
			rf.snapshotLastTerm = args.LastIncludedTerm
			rf.lastApplied = args.LastIncludedIndex
			rf.commitIndex = args.LastIncludedIndex
			rf.WriteSnapshot(0 , args.Data)
			rf.apch <- ApplyMsg{
				CommandValid: false,
				Command:      ReadSnapshot(args.Data),
				CommandIndex: -1,
			}
			return
		}
	}




	rf.WriteSnapshot(0 , args.Data)


	//7. Discard the entire log
	rf.log = []Entry{}


	//8. Reset state machine using snapshot contents (and load
	//   snapshot’s cluster configuration)
	snapshot := ReadSnapshot(rf.persister.ReadSnapshot())
	rf.apch <- ApplyMsg{
		CommandValid: false, //todo
		Command:      snapshot,
		CommandIndex: -1,
	}
	rf.snapshotLastIndex = args.LastIncludedIndex
	rf.snapshotLastTerm = args.LastIncludedTerm
	// --
	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex

	// tell kv service receiving a snapshot
	// reset state machine
	rf.apch <- ApplyMsg{
		CommandValid: false,
		Command:      ReadSnapshot(args.Data),
		CommandIndex: -1,
	}
}


