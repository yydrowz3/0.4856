package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	lockname string
	ownerID  string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.lockname = lockname
	lk.ownerID = kvtest.RandValue(8)

	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		value, version, err := lk.ck.Get(lk.lockname)
		if err == rpc.ErrNoKey { // first, create lock kv
			err = lk.ck.Put(lk.lockname, lk.ownerID, 0)
			if err == rpc.OK {
				return
			}
		}
		if err == rpc.OK {
			if value == "" { // lock is available
				err = lk.ck.Put(lk.lockname, lk.ownerID, version) // may fail with ErrVersion
				if err == rpc.OK {
					return
				}
				if err == rpc.ErrMaybe {
					value, _, tmpErr := lk.ck.Get(lk.lockname)
					if tmpErr == rpc.OK && value == lk.ownerID {
						return
					}
				}
			} else { // lock is held by someone else, wait and retry
				continue
			}
		}
		if err == rpc.ErrMaybe {
			value, _, tmpErr := lk.ck.Get(lk.lockname)
			if tmpErr == rpc.OK && value == lk.ownerID {
				return
			}
		}

		time.Sleep(time.Second)
	}
}

func (lk *Lock) Release() {
	// Your code here
	for {
		value, version, err := lk.ck.Get(lk.lockname)
		if err == rpc.ErrNoKey {
			return
		}
		if err == rpc.OK {
			if value == "" {
				return
			}
			if value == lk.ownerID {
				err = lk.ck.Put(lk.lockname, "", version)
			} else {
				return
			}
		}
		if err == rpc.ErrMaybe {
			value, _, tmpErr := lk.ck.Get(lk.lockname)
			if tmpErr == rpc.OK && value == "" {
				return
			}
		}

		time.Sleep(time.Second)
	}
}
