package raft

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"time"
)

const (
	logname = "/data/menu/kv/raft.info"
	dumpname = "/data/menu/kv/raft.dump"
)

// return immediately
func (rf *Raft) logger ()  {
	logf := getFile(logname)
	log.SetOutput(logf)
	go periodTruncate(logf)
	go rf.periodDump()
}

// write raft's entry and snapshot into
func (rf *Raft) periodDump()  {
	f := getFile(dumpname)
	w := bufio.NewWriter(f)
	for {
		<- time.After(5 * time.Second)
		rf.mu.Lock()


		// DUMP Raft state
		s := fmt.Sprintf("Raft State:\n" +
			"\tme: %d\n" +
			"\tcurrentTerm: %d\n" +
			"\tisLeader: %v\n" +
			"\tHeartbeats period : %v\n" +
			"\telection timeout: %v\n",
			rf.me, rf.currentTerm,
			rf.isLeader, HeartBeatPeriod,
			ElectionTimeOut)

		// DUMP SNAPSHOT
		if len(rf.persister.snapshot) == 0 {
			s += fmt.Sprintf("\nSnapshot\nNULL\n")
		} else {
			snap := ReadSnapshot(rf.persister.snapshot)
			s += fmt.Sprintf("\nSnapshot:\n" +
				"\tLastIncludeIndex: %d\n" +
				"\tLastIncludeTerm: %d\n" +
				"\tState in snapshot: \n", snap.LastIncludeIndex, snap.LastIncludeTerm)

			for k, v := range snap.StateMap {
				s += fmt.Sprintf("\t\tKey:(%s)->Value:(%s)\n", k, v)
			}
		}

		// DUMP ENTRY
		s += fmt.Sprintf("\nlogIndex\t\tlogTerm\t\t\t\tlogCommand\n")
		for i, v := range rf.log {
			_ = i
			s += fmt.Sprintf("%d\t\t\t%d\t\t\t\t%v\n", v.Index, v.Term, v.Comm)
		}
		rf.mu.Unlock()
		f.Truncate(0)
		f.Seek(0, io.SeekStart)
		w.WriteString(s)
		w.Flush()
	}
}

func periodTruncate(f *os.File)  {
	for {
		<- time.After(600 * time.Second)
		f.Truncate(0)
		f.Seek(0, io.SeekStart)
	}
}

func getFile(filename string) *os.File {
	fp, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Fatal(err)
	}
	return fp
}