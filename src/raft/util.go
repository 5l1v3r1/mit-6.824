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

func poll(voteMap map[int]bool, totalNum int) bool {
	voteNum := 0
	for _, v := range voteMap {
		if v == true {
			voteNum++
		}
	}
	return voteNum >= (totalNum/2 + 1)
}
