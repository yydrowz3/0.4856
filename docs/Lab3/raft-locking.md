Raft Locking Advice

If you are wondering how to use locks in the 6.824 Raft labs, here are
some rules and ways of thinking that might be helpful.

Rule 1: Whenever you have data that more than one goroutine uses, and
at least one goroutine might modify the data, the goroutines should
use locks to prevent simultaneous use of the data. The Go race
detector is pretty good at detecting violations of this rule (though
it won't help with any of the rules below).

Rule 2: Whenever code makes a sequence of modifications to shared
data, and other goroutines might malfunction if they looked at the
data midway through the sequence, you should use a lock around the
whole sequence.

An example:

  rf.mu.Lock()
  rf.currentTerm += 1
  rf.state = Candidate
  rf.mu.Unlock()

It would be a mistake for another goroutine to see either of these
updates alone (i.e. the old state with the new term, or the new term
with the old state). So we need to hold the lock continuously over the
whole sequence of updates. All other code that uses rf.currentTerm or
rf.state must also hold the lock, in order to ensure exclusive access
for all uses.

The code between Lock() and Unlock() is often called a "critical
section." The locking rules a programmer chooses (e.g. "a goroutine
must hold rf.mu when using rf.currentTerm or rf.state") are often
called a "locking protocol".

Rule 3: Whenever code does a sequence of reads of shared data (or
reads and writes), and would malfunction if another goroutine modified
the data midway through the sequence, you should use a lock around the
whole sequence.

An example that could occur in a Raft RPC handler:

  rf.mu.Lock()
  if args.Term > rf.currentTerm {
   rf.currentTerm = args.Term
  }
  rf.mu.Unlock()

This code needs to hold the lock continuously for the whole sequence.
Raft requires that currentTerm only increases, and never decreases.
Another RPC handler could be executing in a separate goroutine; if it
were allowed to modify rf.currentTerm between the if statement and the
update to rf.currentTerm, this code might end up decreasing
rf.currentTerm. Hence the lock must be held continuously over the
whole sequence. In addition, every other use of currentTerm must hold
the lock, to ensure that no other goroutine modifies currentTerm
during our critical section.

Real Raft code would need to use longer critical sections than these
examples; for example, a Raft RPC handler should probably hold the
lock for the entire handler.

Rule 4: It's usually a bad idea to hold a lock while doing anything
that might wait: reading a Go channel, sending on a channel, waiting
for a timer, calling time.Sleep(), or sending an RPC (and waiting for the
reply). One reason is that you probably want other goroutines to make
progress during the wait. Another reason is deadlock avoidance. Imagine
two peers sending each other RPCs while holding locks; both RPC
handlers need the receiving peer's lock; neither RPC handler can ever
complete because it needs the lock held by the waiting RPC call.

Code that waits should first release locks. If that's not convenient,
sometimes it's useful to create a separate goroutine to do the wait.

Rule 5: Be careful about assumptions across a drop and re-acquire of a
lock. One place this can arise is when avoiding waiting with locks
held. For example, this code to send vote RPCs is incorrect:

  rf.mu.Lock()
  rf.currentTerm += 1
  rf.state = Candidate
  for <each peer> {
    go func() {
      rf.mu.Lock()
      args.Term = rf.currentTerm
      rf.mu.Unlock()
      Call("Raft.RequestVote", &args, ...)
      // handle the reply...
    } ()
  }
  rf.mu.Unlock()

The code sends each RPC in a separate goroutine. It's incorrect
because args.Term may not be the same as the rf.currentTerm at which
the surrounding code decided to become a Candidate. Lots of time may
pass between when the surrounding code creates the goroutine and when
the goroutine reads rf.currentTerm; for example, multiple terms may
come and go, and the peer may no longer be a candidate. One way to fix
this is for the created goroutine to use a copy of rf.currentTerm made
while the outer code holds the lock. Similarly, reply-handling code
after the Call() must re-check all relevant assumptions after
re-acquiring the lock; for example, it should check that
rf.currentTerm hasn't changed since the decision to become a
candidate.

