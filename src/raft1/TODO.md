https://pdos.csail.mit.edu/6.824/labs/lab-raft1.html

- [x] Part 3A: leader election (moderate)

Implement Raft leader election and heartbeats (AppendEntries RPCs with no log entries). The goal for Part 3A is for a single leader to be elected, for the leader to remain the leader if there are no failures, and for a new leader to take over if the old leader fails or if packets to/from the old leader are lost. Run make RUN="-run 3A" raft1 in the src directory to test your 3A code. 

  - [x] Leader Election
  - [x] Hearbeat

> Follow the paper's Figure 2. At this point you care about sending and receiving RequestVote RPCs, the Rules for Servers that relate to elections, and the State related to leader election,
> Add the Figure 2 state for leader election to the Raft struct in raft.go.
> Fill in the RequestVoteArgs and RequestVoteReply structs. Modify Make() to create a background goroutine that will kick off leader election periodically by sending out RequestVote RPCs when it hasn't heard from another peer for a while. Implement the RequestVote() RPC handler so that servers will vote for one another.
> To implement heartbeats, define an AppendEntries RPC struct (though you may not need all the arguments yet), and have the leader send them out periodically. Write an AppendEntries RPC handler method.
> The tester requires that the leader send heartbeat RPCs no more than ten times per second.
> The tester requires your Raft to elect a new leader within five seconds of the failure of the old leader (if a majority of peers can still communicate).
> The paper's Section 5.2 mentions election timeouts in the range of 150 to 300 milliseconds. Such a range only makes sense if the leader sends heartbeats considerably more often than once per 150 milliseconds (e.g., once per 10 milliseconds). Because the tester limits you tens of heartbeats per second, you will have to use an election timeout larger than the paper's 150 to 300 milliseconds, but not too large, because then you may fail to elect a leader within five seconds.
> You may find Go's rand useful.
> You'll need to write code that takes actions periodically or after delays in time. The easiest way to do this is to create a goroutine with a loop that calls time.Sleep(); see the ticker() goroutine that Make() creates for this purpose. Don't use Go's time.Timer or time.Ticker, which are difficult to use correctly.
> If your code has trouble passing the tests, read the paper's Figure 2 again; the full logic for leader election is spread over multiple parts of the figure.
> Don't forget to implement GetState().
> Go RPC sends only struct fields whose names start with capital letters. Sub-structures must also have capitalized field names (e.g. fields of log records in an array). The labgob package will warn you about this; don't ignore the warnings.
> The most challenging part of this lab may be the debugging. Refer to the Guidance page for debugging tips.
> If you fail a test, the tester produces a file that visualizes a timeline with events marked along it, including network partitions, crashed servers, and checks performed. Here's an example of the visualization. Further, you can add your own annotations by writing, for example, tester.Annotate("Server 0", "short description", "details"). 

```
make RUN="-run 3A" raft1
```

- [x] Part 3B: log (hard)

Implement the leader and follower code to append new log entries, so that make RUN="-run 3B" raft1 passes all tests. 

> Run git pull to get the latest lab software. The Raft paper views the log as 1-indexed, but we suggest that you implement it as 0-indexed, starting with a dummy entry at index=0 that has term 0. That allows the very first AppendEntries RPC to contain 0 as PrevLogIndex, and be a valid index into the log.
> Your first goal should be to pass TestBasicAgree3B(). Start by implementing Start(), then write the code to send and receive new log entries via AppendEntries RPCs, following Figure 2. Send each newly committed entry on applyCh on each peer.
> You will need to implement the election restriction (section 5.4.1 in the paper).
> Your code may have loops that repeatedly check for certain events. Don't have these loops execute continuously without pausing, since that will slow your implementation enough that it fails tests. Use Go's condition variables, or insert a time.Sleep(10 * time.Millisecond) in each loop iteration.
> Do yourself a favor for future labs and write (or re-write) code that's clean and clear.
> If you fail a test, look at raft_test.go and trace the test code from there to understand what's being tested. 

```
make RUN="-run 3B" raft1
```

- [x] Part 3C: persistence (hard)

Complete the functions persist() and readPersist() in raft.go by adding code to save and restore persistent state. You will need to encode (or "serialize") the state as an array of bytes in order to pass it to the Persister. Use the labgob encoder; see the comments in persist() and readPersist(). labgob is like Go's gob encoder but prints error messages if you try to encode structures with lower-case field names. For now, pass nil as the second argument to persister.Save(). Insert calls to persist() at the points where your implementation changes persistent state. Once you've done this, and if the rest of your implementation is correct, you should pass all of the 3C tests. 

> The 3C tests are more demanding than those for 3A or 3B, and failures may be caused by problems in your code for 3A or 3B. 

```
make RUN="-run 3C" raft1
```

- [x] Part 3D: log compaction (hard)

Implement Snapshot() and the InstallSnapshot RPC, as well as the changes to Raft to support these (e.g, operation with a trimmed log). Your solution is complete when it passes the 3D tests (and all the previous Lab 3 tests). 

> git pull to make sure you have the latest software.
> A good place to start is to modify your code to so that it is able to store just the part of the log starting at some index X. Initially you can set X to zero and run the 3B/3C tests. Then make Snapshot(index) discard the log before index, and set X equal to index. If all goes well you should now pass the first 3D test.
> A common reason for failing the first 3D test is that followers take too long to catch up to the leader.
> Next: have the leader send an InstallSnapshot RPC if it doesn't have the log entries required to bring a follower up to date.
> Send the entire snapshot in a single InstallSnapshot RPC. Don't implement Figure 13's offset mechanism for splitting up the snapshot.
> Raft must discard old log entries in a way that allows the Go garbage collector to free and re-use the memory; this requires that there be no reachable references (pointers) to the discarded log entries.
> When a Raft peer is re-started, the persister passed to Make() will contain a snapshot of application state as well as Raft's saved state. Raft must include a non-nil snapshot with every call to persister.Save() (if the log has been trimmed), which means that it's a good idea for Make() to call persister.ReadSnapshot() and save the result.
> A reasonable amount of time to consume for the full set of Lab 3 tests (3A+3B+3C+3D) without -race is 6 minutes of real time and one minute of CPU time. When running with -race, it is about 10 minutes of real time and two minutes of CPU time. 

  - [x] Index x + snapshot, snapshot() => discard before index, move index to X
  - [x] leader send InstallSnapshotRPC -> fail to bring a follower up to date, send entire snapshot in a single rpc

```
make RUN="-run 3D" raft1
```