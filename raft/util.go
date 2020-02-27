package raft

import (
	"log"
	"sync"
)

// Debugging
const Debug = 1
var a sync.Mutex

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}
