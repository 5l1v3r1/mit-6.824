package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func GenTimeoutDuration(base int, scope int) time.Duration {
	return time.Millisecond * time.Duration(base+rand.Intn(scope))
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
