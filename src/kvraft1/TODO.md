
Clients will interact with your key/value service through a Clerk, as in Lab 2. A Clerk implements the Put and Get methods with the same semantics as Lab 2: Puts are at-most-once and the Puts/Gets must form a linearizable history.

Puts -> at-most-once
Puts/Gets -> linearizable history

Providing linearizability is harder if the service is replicated, since all servers must choose the same execution order for concurrent requests, must avoid replying to clients using state that isn't up to date, and must recover their state after a failure in a way that preserves all acknowledged client updates. 


- [x] Part A: replicated state machine (RSM)


Implement rsm.go: the Submit() method and a reader goroutine. You have completed this task if you pass the rsm 4A tests: 


> You should not need to add any fields to the Raft ApplyMsg, or to Raft RPCs such as AppendEntries, but you are allowed to do so.
> Your solution needs to handle an rsm leader that has called Start() for a request submitted with Submit() but loses its leadership before the request is committed to the log. One way to do this is for the rsm to detect that it has lost leadership, by noticing that Raft's term has changed or a different request has appeared at the index returned by Start(), and return rpc.ErrWrongLeader from Submit(). If the ex-leader is partitioned by itself, it won't know about new leaders; but any client in the same partition won't be able to talk to a new leader either, so it's OK in this case for the server to wait indefinitely until the partition heals. 

  - [x] Submit Method
  - [x] reader goroutine

```
make RUN="-run 4A" rsm1
```

- [x] Part B: Key/value service without snapshots

Your first task is to implement a solution that works when there are no dropped messages, and no failed servers.

Feel free to copy your client code from Lab 2 (kvsrv1/client.go) into kvraft1/client.go. You will need to add logic for deciding which kvserver to send each RPC to.

You'll also need to implement Put() and Get() RPC handlers in server.go. These handlers should submit the request to Raft using rsm.Submit(). As the rsm package reads commands from applyCh, it should invoke the DoOp method, which you will have to implement in server.go.

You have completed this task when you reliably pass the first test in the test suite, with make RUN="-run TestBasic4B" kvraft1. 

> A kvserver should not complete a Get() RPC if it is not part of a majority (so that it does not serve stale data). A simple solution is to enter every Get() (as well as each Put()) in the Raft log using Submit(). You don't have to implement the optimization for read-only operations that is described in Section 8.
> It's best to add locking from the start because the need to avoid deadlocks sometimes affects overall code design. The tester runs your code with the race detector by default. 

Add code to handle failures. Your Clerk can use a similar retry plan as in lab 2, including returning ErrMaybe if a response to a retried Put RPC is lost. You are done when your code reliably passes all the 4B tests, with make RUN="-run 4B" kvraft1. 

> Recall that the rsm leader may lose its leadership and return rpc.ErrWrongLeader from Submit(). In this case you should arrange for the Clerk to re-send the request to other servers until it finds the new leader.
> You will probably have to modify your Clerk to remember which server turned out to be the leader for the last RPC, and send the next RPC to that server first. This will avoid wasting time searching for the leader on every RPC, which may help you pass some of the tests quickly enough. 

```
make RUN="-run 4B" kvraft1
```

- [x] Part C: Key/value service with snapshots

Modify your rsm so that it detects when the persisted Raft state grows too large, and then hands a snapshot to Raft. When a rsm server restarts, it should read the snapshot with persister.ReadSnapshot() and, if the snapshot's length is greater than zero, pass the snapshot to the StateMachine's Restore() method. You complete this task if you pass TestSnapshot4C in rsm. 

> Think about when rsm should snapshot its state and what should be included in the snapshot beyond just the server state. Raft stores each snapshot in the persister object using Save(), along with corresponding Raft state. You can read the latest stored snapshot using ReadSnapshot().
> Capitalize all fields of structures stored in the snapshot. 

Implement the kvraft1/server.go Snapshot() and Restore() methods, which rsm calls. Modify rsm to handle applyCh messages that contain snapshots. 

> You may have bugs in your Raft and rsm library that this task exposes. If you make changes to your Raft implementation make sure it continues to pass all of the Lab 3 tests.
> A reasonable amount of time to take for the Lab 4 tests is 400 seconds of real time and 700 seconds of CPU time. 

  - [x] KVServer Snapshot & Restore
  - [x] modify rsm structure
  - [x] add rsm encode / decode function
  - [x] make rsm restore rsm -> restore sm (KVServer)
  - [x] installSnapshot helper function
  - [x] modify reader()
  - [x] modify Submit()


```
make RUN="-run 4C" kvraft1
```