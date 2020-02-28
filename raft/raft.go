package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sort"
	"sync"
	"time"
	labrpc "kvservice/realrpc"
)
// import "labrpc"

// import "bytes"
// import "labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// A

	// persistent state
	currentTerm 	int
	votedFor 		int
	log             []Entry
	// volatile state
	commitIndex 	int
	lastApplied     int		// applied to state machine
	// leader state
	isLeader		bool
	nextIndex		[]int
	matchIndex 		[]int

	role 			state	// follower, candidate or leader
	reElectionTime  time.Time

	// for test
	apch 			chan ApplyMsg

	// snapshot
	snapshotLastIndex int  // remember rf.log[0] restore index 0 entry firstly
	snapshotLastTerm int

	// terminate some routine
	exit 			bool

}


//
// A entry has index, term, command and command id
// first index is 1
//
type Entry struct {
	 Index 	int
	 Term 	int
	 Comm 	interface{}
	 Commid int
}
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.isLeader
	rf.mu.Unlock()
	return term, isleader
}


//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	rf.persister.SaveRaftState(rf.PersistentState())
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	rf.resumeRaftStat(data)
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

type AppendEntryArgs struct {
	Term  			int
	LeaderId 		int
	PrevLogIndex 	int
	PrevLogTerm 	int
	Entries			[]Entry
	LeaderCommit 	int
}

type AppendEntryReply struct {
	Term  			int
	Success 		bool
}

//
// example RequestVote RPC handler.
// locked
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	// Your code here (2A, 2B).
	// 2A
	lastLogIndex := rf.lastIndex()

	DPrintf("[RequestsVote] candidate %d(%d), server %d(%d) \n", args.CandidateId, args.Term, rf.me, rf.currentTerm,)
	if rf.realTerm(lastLogIndex) > args.LastLogTerm {
		DPrintf("\tLastLogTerm Follower s%d(%d) > Candidate%d(%d)", rf.me, rf.realTerm(lastLogIndex), args.CandidateId, args.LastLogTerm)
	} else if rf.realTerm(lastLogIndex) == args.LastLogTerm && lastLogIndex > args.LastLogIndex {
		DPrintf("\tLastLogTerm equal, but Follower Log length s%d(%d) > Candidate%d(%d)", rf.me, lastLogIndex, args.CandidateId, args.LastLogIndex)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// for a leader
	if args.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.isLeader = false
		rf.persist()
		rf.role = follower
	}



	reply.VoteGranted = false
	if rf.currentTerm >= args.Term {
		reply.Term = rf.currentTerm
		DPrintf("[x][server %d] term < [server %d]", args.CandidateId, rf.me)
		return nil
	}
	rf.currentTerm = args.Term
	if rf.votedFor != -1 {
		DPrintf("\t[x][server %d] not get vote(%d) from [server %d]\n", args.CandidateId, rf.votedFor, rf.me)
		return nil
	}
	// "If votedFor is null or candidateId, and candidate’s log is
	// at least as up-to-date as receiver’s log, grant vote"
	lastLogIndex = rf.lastIndex()
	lastLogTerm := rf.realTerm(lastLogIndex)
	if 	lastLogTerm > args.LastLogTerm ||
		lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex {
		DPrintf("\t[x][server %d] not get vote(%d) from [server %d]\n", args.CandidateId, rf.votedFor, rf.me)
		return nil
	}

	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	rf.persist()
	rf.mu.Unlock()
	// "If election timeout elapses without receiving AppendEntries
	// RPC from current leader or granting vote to candidate:
	// convert to candidate"
	rf.resetElectionTimeOut()
	rf.mu.Lock()
	DPrintf("\t[+][server %d] get vote from [server %d]\n", args.CandidateId, rf.me)
	return nil
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) error {
	//a.Lock()
	//defer  a.Unlock()
	//DPrintf("s%d %#v", rf.me, args)
	DPrintf("%#v", args)
	DPrintf("[AppendEntry] term %d(%d)->%d(%d)", args.LeaderId, args.Term,	rf.me, rf.currentTerm)


	if len(rf.log) == 0 {
	// trimmed log
		DPrintf("load snapshot to verify leader authority")
	} else if len(rf.log) > args.PrevLogIndex {
		DPrintf("\t At %d leader(%d) follower(%d)", args.PrevLogIndex, args.PrevLogTerm, rf.log[args.PrevLogIndex].Term)
		DPrintf("\t\t %#v", rf.log[args.PrevLogIndex])
	} else {
		DPrintf("\t At %d leader(%d) follower(x)", args.PrevLogIndex, args.PrevLogTerm)
	}

	// "If election timeout elapses without receiving AppendEntries
	// RPC from current leader or granting vote to candidate:
	// convert to candidate"
	if args.Term == rf.currentTerm {
		// occupy lock
		rf.resetElectionTimeOut()
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false

	/* Term check */
	if args.Term < rf.currentTerm {
		DPrintf("(follower %d(%d) > leader %d(%d))", rf.me, rf.currentTerm, args.LeaderId, args.Term)
		reply.Term = rf.currentTerm
		return nil
	}

	/* PrevLogIndex and PrevLogTerm check */
	// "• If two entries in different logs have the same index
	// and term, then they store the same command."
	if rf.snapshotLastIndex == 0 {
		if len(rf.log) <= args.PrevLogIndex {
			DPrintf("\tnormal case: doesn't contain the entry")
			return nil
		}
		if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			DPrintf("\tnormal case: prevLogTerm don't match follower %d, leader %d",
				rf.log[args.PrevLogIndex].Term, args.Term)
			return nil
		}
	} else {
		//if args.PrevLogIndex == rf.snapshotLastIndex && args.PrevLogTerm != rf.snapshotLastTerm {
		//	DPrintf("\tSnapshot case: lastTerm not equal")
		//	return
		//}
		//
		//lastIndex := rf.lastIndex()
		//if lastIndex < args.PrevLogIndex {
		//	DPrintf("\tSnapshot case: prevLogIndex > lastIndex")
		//	return
		//}
		//
		//DPrintf("args.PrevLogIndex %d, lastIndex %d len(rf.log) %d snapshot %d",
		//	args.PrevLogIndex, rf.lastIndex(), len(rf.log), rf.snapshotLastIndex)
		//if len(rf.log) != 0 && rf.realTerm(args.PrevLogIndex) != args.PrevLogTerm {
		//	DPrintf("\t\tSnapshot case: prevLogTerm don't match")
		//	return
		//}
		ok := false
		if args.PrevLogIndex == rf.snapshotLastIndex && args.PrevLogTerm == rf.snapshotLastTerm {
			ok = true
		}
		if args.PrevLogIndex > rf.snapshotLastIndex &&
			(args.PrevLogIndex - rf.snapshotLastIndex) <= len(rf.log) &&
			rf.realTerm(args.PrevLogIndex) == args.PrevLogTerm {
			ok = true
		}
		if !ok {
			return nil
		}
	}


	rf.currentTerm = args.Term
	DPrintf("\t[AppendEntry Entry] %#v", args.Entries)
	rf.votedFor = -1
	if rf.isLeader {			// older leader should revert to follower if receive the heartbeats
		rf.isLeader = false
		rf.role = follower
	}
	reply.Success = true


	DPrintf("\t[commit] leader vs follower[%d] = (%d, %d)", rf.me, args.LeaderCommit, rf.commitIndex)
	// "If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)"
	if args.LeaderCommit >  rf.commitIndex {
		lastIndex := rf.lastIndex()
		if args.LeaderCommit < lastIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastIndex
		}
	}
	DPrintf("\t%d commitIdx set to %d", rf.me, rf.commitIndex)

	// HeartBeats
	if cap(args.Entries) == 0 {
		DPrintf("\t[heartbeats] server %d apply %d/%d", rf.me, rf.lastApplied, rf.commitIndex)

	} else  { // new agreement
		DPrintf("\t[Command] %#v", args.Entries)
		if args.Entries[0].Comm != consistency {
			// Truncate the log array
			if args.PrevLogIndex < rf.lastIndex() {
				DPrintf("\t[Log Truncate] log last %d truncate at %d", rf.lastIndex(), args.PrevLogIndex)
				newlog := []Entry{}
				if rf.snapshotLastIndex != 0 && len(rf.log) != 0 {
					newlog = append(newlog, rf.log[:args.PrevLogIndex-rf.snapshotLastIndex]...)
				} else {
					newlog = append(newlog, rf.log[:args.PrevLogIndex+1]...)
				}
				rf.log = newlog
			}
			rf.log = append(rf.log, args.Entries...)
			rf.persist()
		} else {
			DPrintf("%#v", rf.log)
		}
	}


	DPrintf("\t[Apply Command] server %d apply %d/%d", rf.me, rf.lastApplied, rf.commitIndex)
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		applyIndex := rf.lastApplied
		if rf.snapshotLastIndex != 0 {
			applyIndex -= rf.snapshotLastIndex+1
		}
		DPrintf("\t %d lastapply %d log len %d, subindex %d",
			rf.me, rf.lastApplied, len(rf.log), applyIndex)
		am := ApplyMsg{true, rf.log[applyIndex].Comm, rf.log[applyIndex].Commid}
		if rf.log[applyIndex].Comm == no_op {
			am.CommandValid = true
		}
		// am := ApplyMsg{true, rf.log[rf.lastApplied].Comm, rf.log[rf.lastApplied].Index}
		DPrintf("\tserver:[%d] %#v", rf.me, am)
		rf.mu.Unlock()
		rf.apch <- am
		rf.mu.Lock()
		DPrintf("\tserver[%d] applied %d ok", rf.me, am.CommandIndex)
	}

	DPrintf("\t server %d appendEntry from %d end", rf.me, args.LeaderId)
	return nil
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	// Your code here (2B).

	rf.mu.Lock()
	if !rf.isLeader {
		isLeader = false
	} else {
		// TODO Command ID
		//index = len(rf.log) - 1
		//if index != 0 {
		//	for ; index >= 1; index-- {
		//		if rf.log[index].Index == 0 {
		//			continue
		//		} else {
		//			index = rf.log[index].Index
		//			break
		//		}
		//	}
		//}
		//index++
		term = rf.currentTerm

		newIndex := -1
		if len(rf.log) == 0 {
			snapshot := ReadSnapshot(rf.persister.ReadSnapshot())
			newIndex = snapshot.LastIncludeIndex + 1
		} else {
			newIndex = rf.log[len(rf.log)-1].Index + 1
		}
		rf.log = append(rf.log, Entry{newIndex, rf.currentTerm, command, newIndex})
		//rf.log = append(rf.log, Entry{index, term, command, index})
		index = newIndex
		rf.persist()

		DPrintf("[%d New Agreement] %#v", rf.me, rf.log[len(rf.log)-1])
	}

	rf.mu.Unlock()

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.

	// TODO: I need a more elegant method to terminate the leader server
	rf.isLeader = false
	rf.exit = true
	DPrintf("[Kill] server %d will terminate", rf.me)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.snapshotLastIndex = 0

	// Your initialization code here (2A, 2B, 2C).
	// 2A

	// initialize the server state
	rf.currentTerm = 0
	rf.log = make([]Entry, 1)
	rf.log[0] = Entry{0, 0, nil, 0}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.votedFor = -1
	rf.role = follower
	rf.apch = applyCh

	// start a goroutine to start election periodically
	go func() {
		for {
			//election timeout
			rf.resetElectionTimeOut()
			rf.ElectionTimeOut()

			if rf.exit {
				return
			}

			DPrintf("server %d timeout to election\n", rf.me)
			// ready for new election
			rf.newElection()
			switch rf.role {
			case leader:
				rf.LeaderLoop()
			case follower:
				continue
			}


		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

// TimeOut never return until raft.reElectionTime elapsed
func (rf *Raft) ElectionTimeOut() {
	for {
		rf.mu.Lock()
		if dur := rf.reElectionTime.Sub(time.Now()) ; dur > 0  {
			rf.mu.Unlock()
			time.Sleep(dur)
		} else {
			rf.mu.Unlock()
			break
		}
	}
}

// reset to now + 150~300 ms
func (rf *Raft) resetElectionTimeOut() time.Time {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.reElectionTime = time.Now().Add(time.Duration(rand.Int31n(150)) * time.Millisecond + ElectionTimeOut)
	return rf.reElectionTime
}

// in order to the raft server processing request

func (rf *Raft) newElection() {


	DPrintf("server %d start new election, request term %d", rf.me, rf.currentTerm+1)
	done := make(chan vote, len(rf.peers))
	maxTerm := rf.currentTerm+1

	// timeout or received the heartbeat
	go func() {
		st := rf.resetElectionTimeOut()
		rf.mu.Lock()
		dur := rf.reElectionTime.Sub(time.Now())
		rf.mu.Unlock()
		time.Sleep(dur)
		rf.mu.Lock()
		ed := rf.reElectionTime
		rf.mu.Unlock()
		if st == ed {
			done <- timeout
		} else {
			done <- heartbeats
		}
	}()

	// request vote
	rf.mu.Lock()
	rf.role = candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me

	lastIndex := rf.lastIndex()
	lastTerm := rf.realTerm(lastIndex)

	DPrintf("lastIndex %d, len log %d", lastIndex, len(rf.log))

	voteArgs := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}

	rf.mu.Unlock()

	// send RPC to all server to get votes
	go func() {
		votes := make(chan bool, len(rf.peers))
		for i, _ := range rf.peers {
			if i == rf.me {
				continue
			}

			go func(i int) {
				reply := &RequestVoteReply{}
				ok := rf.sendRequestVote(i, voteArgs, reply)
				rf.mu.Lock()
				if ok && reply.VoteGranted == true {
					votes <- true
					DPrintf("server %d get vote from %d ", rf.me, i)
				} else if reply.Term > maxTerm {
					maxTerm = reply.Term
				}
				rf.mu.Unlock()
			}(i)
		}

		voteSum := 1
		for v := range votes {
			if v {
				voteSum += 1
				if voteSum > len(rf.peers) / 2 {
					done <- elected
					return
				}
			}
		}


	}()

	//
	result := <- done
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	rf.votedFor = -1
	switch result {
	case elected:
		rf.isLeader = true
		rf.role = leader
		DPrintf("--[New Leader %d]", rf.me)
		return
	case timeout:
		DPrintf("%d Timeout MaxTerm %d selfTerm %d", rf.me, maxTerm, rf.currentTerm)
		if rf.currentTerm < maxTerm {
			rf.currentTerm = maxTerm
		}
		rf.role = follower
	case heartbeats:	//receive heartbeat from leader, candidate revert to follower
		rf.role = follower
	}

	DPrintf("[candidate %d] revert to follower", rf.me)
}

func (rf *Raft) LeaderLoop() {
	ch := make(chan bool)
	DPrintf("[Leader log] %#v", rf.log)

	// when candidate become leader, start a new agreement
	newIndex := 0
	if len(rf.log) == 0 {
		snapshot := ReadSnapshot(rf.persister.ReadSnapshot())
		newIndex = snapshot.LastIncludeIndex + 1
	} else {
		newIndex = rf.log[len(rf.log)-1].Index + 1
	}
	rf.log = append(rf.log, Entry{newIndex, rf.currentTerm, no_op, newIndex})
	DPrintf("\t leader append new entry %v", Entry{newIndex, rf.currentTerm, no_op, 0})
	// initialize next index
	peers := len(rf.peers)
	rf.nextIndex = make([]int, peers)
	rf.matchIndex = make([]int, peers)
	for i := 0; i < peers; i++ {
		rf.nextIndex[i] = rf.lastIndex()+1
		rf.matchIndex[i] = 0
	}

	go rf.sendHeartBeats(ch)

	<- ch

	DPrintf("leader %d revert to follower", rf.me)
}

func (rf *Raft) sendHeartBeats(ch chan bool) {

	peers := len(rf.peers)
	state := make(chan bool, peers)
	lock := make([]int, peers)

	for {
		rf.leaderCheckCommit()
		DPrintf("[Leader %d Term] %d", rf.me, rf.currentTerm)
		for i := 0; i < peers; i++  {
			if i == rf.me {
				continue
			}

			DPrintf("[leader %d] try lock", rf.me)
			rf.mu.Lock()
			DPrintf("[leader %d] locked", rf.me)

			if !rf.isLeader {
				rf.mu.Unlock()
				DPrintf("[leader %d] no longer be a leader", rf.me)
				trySend(false, ch)
				return
			}

			appendArgs := &AppendEntryArgs{}
			reply := &AppendEntryReply{}

			DPrintf("[leader %d last index] %d", rf.me, rf.lastIndex())
			DPrintf("for %d rf.nextIndex[i] %d, len %d, snapshot size %d", i, rf.nextIndex[i], len(rf.log), rf.snapshotLastIndex)

			snapLogSize := 0

			if rf.snapshotLastIndex != 0 && rf.nextIndex[i] - 1 < rf.snapshotLastIndex {
				// this follower is too far behind
				// need send InstallSnapshotRPC

				DPrintf("\tfor %d is too far behind", i)
				snapshotArgs := &InstallSnapshotArgs{
					Term:              rf.currentTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.snapshotLastIndex,
					LastIncludedTerm:  rf.snapshotLastTerm,
					Offset:            0,
					Data:              rf.persister.ReadSnapshot(),
					Done:              true,
				}
				snapshotReply := &InstallSnapshotReply{}
				rf.mu.Unlock()

				go func(i int) {
					DPrintf("\tleader %d send to %d snapshot", rf.me, i)
					ok := rf.sendInstallSnapshot(i, snapshotArgs, snapshotReply)
					DPrintf("\tleader %d send to %d snapshot end", rf.me, i)
					rf.mu.Lock()
					defer rf.mu.Unlock()

					if !ok {
						DPrintf("\tleader %d send to %d snapshot lost", rf.me, i)
						return
					}


					if snapshotReply.Term > rf.currentTerm {
						DPrintf("\t[%d revert to follower in snapshot]: leader term %d < follower term %d",
							rf.me, rf.currentTerm, snapshotReply.Term)
						rf.currentTerm = snapshotReply.Term
						rf.isLeader = false
						trySend(false, state)
						return
					}
					// Install snapshot OK
					rf.nextIndex[i] = rf.snapshotLastIndex+1
					rf.matchIndex[i] = rf.snapshotLastIndex
					DPrintf("\t %d->%d snapshot rpc done, set %d nextIndex to %d",
						rf.me, i, i, rf.nextIndex[i])
				}(i)

				continue
			} else if rf.snapshotLastIndex != 0 && rf.nextIndex[i] - 1 == rf.snapshotLastIndex {
				// raft log may be trimmed to zero
				// need load snapshot to get lastIncludedIndex and lastIncludedTerm
				DPrintf("middle case")
				snapshot := ReadSnapshot(rf.persister.ReadSnapshot())

				appendArgs.PrevLogIndex = snapshot.LastIncludeIndex
				appendArgs.PrevLogTerm = snapshot.LastIncludeTerm
				appendArgs.Entries = rf.log

				snapLogSize = snapshot.LastIncludeIndex

			} else {
				// normal case
				DPrintf("normal case")
				preIndex := rf.nextIndex[i] - 1
				if rf.snapshotLastIndex != 0 {
					preIndex -= rf.snapshotLastIndex + 1
				}
				appendArgs.Entries = rf.log[preIndex+1:]
				appendArgs.PrevLogIndex = rf.log[preIndex].Index
				appendArgs.PrevLogTerm = rf.log[preIndex].Term

			}

			// other information
			appendArgs.LeaderCommit = rf.commitIndex
			appendArgs.LeaderId = rf.me
			appendArgs.Term = rf.currentTerm

			beforeSendNextIndex := rf.nextIndex[i]

			rf.mu.Unlock()

			go func (i int) {
				DPrintf("%d send heartbeats to %d\n", rf.me, i)
				DPrintf("append %#v", appendArgs)

				ok := rf.sendAppendEntry(i, appendArgs, reply)

				if !reply.Success {
					if !ok {
						DPrintf("\tleader %d send to %d heartbeats lost", rf.me, i)
						// send AppendEntry at next period
						return
					}
					DPrintf("\tleader %d send to %d heartbeats reply false", rf.me, i)
					// revert to follower if follower term > leader term
					rf.mu.Lock()
					if reply.Term > appendArgs.Term {
						DPrintf("\t[%d revert to follower]: term < follower term", rf.me)
						rf.isLeader = false
						rf.currentTerm = reply.Term
						rf.mu.Unlock()
						trySend(false, state)

					} else if !rf.isLeader {
						DPrintf("\t[%d revert to follower]: isn't a leader", rf.me)
						rf.mu.Unlock()
						trySend(false, state)
					} else {
						DPrintf("\tConsistency check")
						if lock[i] == 0 {
							lock[i] = 1
							rf.mu.Unlock()
							rf.checkConsistency(i)
							lock[i] = 0
							return
						}
						rf.mu.Unlock()
					}
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()

				// nextIndex may be trimmed by snapshot
				if rf.nextIndex[i] != beforeSendNextIndex {
					return
				}

				if len(appendArgs.Entries) > 0 {
					rf.nextIndex[i] = appendArgs.Entries[len(appendArgs.Entries)-1].Index + 1
					rf.matchIndex[i] = rf.nextIndex[i] - 1
				}
				//rf.nextIndex[i] = appendArgs.PrevLogIndex + len(appendArgs.Entries) + 1 - snapLogSize
				_ = snapLogSize
			} (i)

		}


		select {
		case <-time.After(HeartBeatPeriod):
			continue
		case <- state:
			ch <- false
			return
		}

	}
}

func (rf *Raft) leaderCheckCommit() {
	// can commit

	DPrintf("%d leaderCheckCommit try lock", rf.me)
	rf.mu.Lock()
	defer DPrintf("%d leaderCheckCommit unlock", rf.me)
	defer rf.mu.Unlock()
	DPrintf("%d leaderCheckCommit locked", rf.me)
	sortIdx := make([]int, len(rf.peers))
	copy(sortIdx, rf.matchIndex)
	sort.Ints(sortIdx)

	low := -1
	if len(rf.log) == 0 {
		return
	} else {
		low = rf.log[len(rf.log)-1].Index
	}


	DPrintf("[Leader Commit] leader %d [matchArray] %#v ", rf.me, sortIdx)
	for ep := len(rf.peers)/2+1; ep < len(rf.peers); ep++ {
		DPrintf("ep %d, low %d", sortIdx[ep], low)
		if sortIdx[ep] < low {
			low = sortIdx[ep]
		}
	}

	lowIdx := low
	if rf.snapshotLastIndex != 0 {
		if low <= rf.snapshotLastIndex {
			return
		}
		low -= rf.snapshotLastIndex + 1
	}
	// "If there exists an N such that N > commitIndex, a majority
	//	of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	//	set commitIndex = N"

	if rf.log[low].Term != rf.currentTerm {
		// TODO: W!H!Y!
		return
	}

	rf.commitIndex = lowIdx
	DPrintf("leader low %d, apply %d", rf.commitIndex, rf.lastApplied)

	// "If commitIndex > lastApplied: increment
	// lastApplied, apply log[lastApplied] to
	// state machine"
	for ; rf.lastApplied < rf.commitIndex; {
		rf.lastApplied++

		applyIndex := rf.lastApplied
		if rf.snapshotLastIndex != 0 {
			applyIndex -= rf.snapshotLastIndex + 1
		}
		am := ApplyMsg{true, rf.log[applyIndex].Comm, rf.log[applyIndex].Commid}
		if rf.log[applyIndex].Comm == no_op {
			am.CommandValid = true
			// for lab2
		}
		DPrintf("Leader  %d Commit %#v", rf.me, am)
		rf.mu.Unlock()
		rf.apch <- am
		rf.mu.Lock()
	}
	DPrintf("leader %d check commit ok, unlock(), begin send heartbeats", rf.me)
}

func trySend(val bool, ch chan bool) bool {
	ok := false
	select {
	case ch <-val:
		ok = true
	default:
	}
	return ok
}


// "broadcastTime ≪ electionTimeout ≪ MTBF"
const (
	ElectionTimeOut = 100 * time.Second
	HeartBeatPeriod = 10 * time.Second
)

type state int
type vote int
type apent int
const (
	follower state = 1 + iota
	candidate
	leader
)

const (
	elected vote = 3 + iota
	heartbeats
	timeout
)

const (
	no_op = -1
	consistency = -2
)