package raft


// If AppendEntries fails because of log inconsistency:
// this function **only** set the nextIndex[clientIndex]
// to an appropriate index, those entry before this index
// is consistent.
func (rf *Raft) checkConsistency (clientIndex int) {
	DPrintf("[Consistency check %d] begin check", clientIndex)

	if rf.nextIndex[clientIndex] == 0 {
		// when a checkConsistency done, and release the lock[i], and
		// ready to send a snapshot at next period, but at the same time
		// a heartbeat goroutine for this client end replied false, and
		// check the client with nextIndex = 0, so it should return
		// immediately here.
		return
	}


	//lastLog := preidx
	term := rf.currentTerm
	appendArgs := &AppendEntryArgs{}
	appendArgs.Entries = make([]Entry, 1)
	appendArgs.Entries[0].Comm = consistency;
	appendArgs.Term = term
	appendArgs.LeaderId = rf.me

	// in the consistency check processing,
	// follower can not doing commit.
	appendArgs.LeaderCommit = 0

	reply := &AppendEntryReply{}
	reply.Success = false
	preTerm := term

	beforeSnapshot := false

	for rf.nextIndex[clientIndex] != rf.snapshotLastIndex + 1 {
		DPrintf("[Consistency check %d] nextI %d", clientIndex, rf.nextIndex[clientIndex])

		// 1. for those slow heartbeats rpc reply false, and start
		// consistency check, but leader may snapshot those log
		// to state machine, just set rf.nextIndex to 0.
		// 2. In the consistency check processing, server may do
		// a snapshot, so it should be return and send snapshot
		if rf.nextIndex[clientIndex] <= rf.snapshotLastIndex {
			rf.nextIndex[clientIndex] = 0
			return
		}



		rf.mu.Lock()
		if !rf.isLeader {
			DPrintf("[%d Revert to follower in Consistency check]", rf.me)
			rf.mu.Unlock()
			return
		}

		// decrement nextIndex for a follower
		// if RPC return false
		preTermIdx := -1;
		for preTermIdx = rf.nextIndex[clientIndex]-1;
			(rf.snapshotLastIndex == 0 && preTermIdx >= 0) ||
			(rf.snapshotLastIndex > 0 && preTermIdx > rf.snapshotLastIndex);
			preTermIdx-- {
			// when preTermIdx decrement to 0,
			// break here
			if rf.realTerm(preTermIdx) != preTerm {
				rf.nextIndex[clientIndex] = preTermIdx + 1
				preTerm = rf.realTerm(preTermIdx)
				break;
			}
		}
		DPrintf("[consistency check] snap %d pretermIdx %d nextIdx %d",
			rf.snapshotLastIndex, preTermIdx, rf.nextIndex[clientIndex])
		// not find the appropriate preTerm before decrement to snapshotLastIndex
		if rf.snapshotLastIndex != 0 && rf.snapshotLastIndex == preTermIdx  {
			appendArgs.PrevLogIndex = rf.snapshotLastIndex
			appendArgs.PrevLogTerm = rf.snapshotLastTerm
			rf.nextIndex[clientIndex] = rf.snapshotLastIndex + 1
			beforeSnapshot = true
		} else {
			appendArgs.PrevLogIndex = preTermIdx
			appendArgs.PrevLogTerm = rf.realTerm(preTermIdx)
		}


		rf.mu.Unlock()
		//DPrintf("c to %d %#v", i, appendArgs)
		// send RPC to follower
		ok := false
		for ok == false {
			ok = rf.sendAppendEntry(clientIndex, appendArgs, reply)
		}

		rf.mu.Lock()
		if reply.Success {
			DPrintf("[%d Checking %d end] next i %d", rf.me, clientIndex, rf.nextIndex[clientIndex])
			rf.mu.Unlock()
			return
		} else if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.isLeader = false
		} else if reply.Success == false && beforeSnapshot == true {
			// should send InstallSnapshotRPC
			DPrintf("%d should send %d InstallSnapshotRPC", rf.me, clientIndex)
			rf.nextIndex[clientIndex] = 0
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
	}


}