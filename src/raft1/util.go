package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

const heartbeatInterval = 150 * time.Millisecond

func randomElectionTimeout() time.Duration {
	return time.Duration(400+rand.Intn(300)) * time.Millisecond
}
