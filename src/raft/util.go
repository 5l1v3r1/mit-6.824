package raft

import (
	"log"
	"math/rand"
	"sort"
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

func min(a int, b ...int) int {
	minVal := a
	for _, v := range b {
		if v < minVal {
			minVal = v
		}
	}
	return minVal
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

//一半数以上的最小值
func pollCommitIndex(matchIndexMap map[int]int) int {
	indexSlice := make([]int, 0)
	for _, v := range matchIndexMap {
		indexSlice = append(indexSlice, v)
	}
	sort.Ints(indexSlice)

	if len(matchIndexMap)%2 == 0 {
		return indexSlice[len(indexSlice)/2-1]
	}
	return indexSlice[len(indexSlice)/2]

}
