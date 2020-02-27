package raft

import (
	"6.824/src/labgob"
	"bytes"
)

func EncodeOne(variable interface{}) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(variable)
	return w.Bytes()
}

func (rf *Raft) PersistentState () []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.snapshotLastIndex)
	e.Encode(rf.snapshotLastTerm)
	return w.Bytes()
}

// resumeRaftStat decode in a particular
// order, only call this function
// in initialization
func (rf *Raft) resumeRaftStat (data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var vote int
	var lastIndex int
	var lastTerm int
	var log []Entry
	if err := d.Decode(&term); err != nil {
		DPrintf("err %v", err)
	} else if err := d.Decode(&vote); err != nil {
		DPrintf("err %v", err)
	} else if err := d.Decode(&log); err != nil {
		DPrintf("err %v", err)
	} else if err := d.Decode(&lastIndex); err != nil {
		DPrintf("err %v", err)
	} else if err := d.Decode(&lastTerm); err != nil {
		DPrintf("err %v", err)
	} else {
		DPrintf("readpersister: server %d state:" +
			"vote %v term %d, lastindex %d, lastterm %d, log %v",
			rf.me, vote, term, lastIndex, lastTerm, log)
		rf.votedFor = vote
		rf.currentTerm = term
		rf.log = log

		// if don't persist lastIndex and lastTerm,
		// it need ReadSnapshot(rf.persister.snapshot) to
		// get snapshot and resume state.
		rf.snapshotLastIndex = lastIndex
		rf.snapshotLastTerm = lastTerm
		// all entry be snapshot is applied
		rf.lastApplied = lastIndex
		rf.commitIndex = lastIndex
	}
}

