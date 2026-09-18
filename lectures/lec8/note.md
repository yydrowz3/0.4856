6.5840 2026 Lecture 8: Consistency, Linearizability

today's topic: consistency models, specifically linearizability

consistency model:
  a spec for the relationship of different clients' views of a service
  I'll focus on key/value storage with network clients
    given a bunch of put/get calls, what outcome(s) are valid?
  assuming individual operations, in isolation, are correct

in ordinary sequential programming, there's nothing to talk about:
  we expect a read to yield the last written value

when might there be any question about what's correct?
  [simple client/server diagrams]
  read simultaneous with write
  replicas
  caches
  failure, recovery
  lost messages + retransmission

why does a storage system need an explicit consistency model?
  for applications, hard to be correct w/o guarantees from storage
    e.g. producer computes, then executes
      put("result", 27)
      put("done", true)
    consumer executes
      while get("done") == false:
        pause
      v = get("result")
    is v guaranteed to be 27?
  for services, hard to design/optimize w/o a specification
    e.g. OK for clients to read from GFS replicas? from Raft followers?

there are lots of consistency models
  today: linearizability
  but we'll also see:
    eventual consistency
    causal consistency
    fork consistency
    serializability
  driving force: tradeoffs among
    performance
    simplicity
    fault tolerance

linearizability
  it's a specification -- a requirement for how a service must behave
    from clients' point of view: from outside the service
  it's usually what people mean by "strong consistency".
    few "anomalies" / surprises
    a gold standard
    but rules out many optimizations.
  important for us:
    Raft is intended for building fault-tolerant linearizable services
    Lab 4 will be a linearizable key/value store on Raft

starting point
  we assume that there's a serial spec for what individual operations do
  serial = a single server executing operations one at a time
  db[]
  put(k, v):
    db[k] = v
    return true
  get(k):
    return db[k]
  no surprises here.

what about concurrent client operations?
  "concurrent" = overlap in time
  a client sends an RPC request;
    takes some time crossing the network;
    server computes, talks to replicas, &c;
    reply moves through network;
    client receives reply
  other clients may send/receive/wait during that time!
  the serial spec doesn't tell us what results are legal
  we need a way to describe concurrent scenarios,
    so we can talk about which results are/aren't allowed

definition: a history
  describes a time-line of possibly-concurrent operations
  each operation tagged w/ client start and finish times
    time client sent RPC request; time received reply
    as well as argument and return values
  example:
    C1: |-Wx1-| |-Wx2-|
    C2:   |---Rx2---|
  the x-axis is real time
    |- indicates the time at which client sent request
    -| indicates the time at which the client received the reply
  "Wx1" means put(x, 1)
  "Rx2" means get(x) -> 2
  records assumed to start with some undefined value
  C1 sent put(x, 1), recv reply, sent put(x, 2), recv reply
    writes have responses, signifying completion
  C2 sent get(x), recv reply=2

a history is a trace of what clients saw (or might see)
  used to check whether the execution was linearizable
  used by designers in "would this be OK" thought experiments

definition: a history is linearizable if
  * you can find a point in time for each operation
    between its start and finish, and
  * the history's result values are the same as serial
    execution in point order.

example history 1:
  |--Wx1--| |--Wx2--|
     |----Rx2----|
       |--Rx1--|

is this history linearizable?
  can we find a linearization point for each operation?
  we may need to try a few different point assignments.
  this order of points satisfies the rules:
    Wx1 Rx1 Wx2 Rx2
  1. each point lies between start and finish.
  2. the sequence satisfies the serial put/get spec.
  so: yes, this history is linearizable
    (and it's an answer to The Question)

note: either read could have returned either 1 or 2.
  so linearizability often allows multiple different outcomes.
  so we often can't predict in advance, but we can check afterwards.

note: the service may not have executed the operations at those points!
  we're not concerned here with how the service operated internally
  we only care that the client-visible results could have
    resulted from execution in some point order

what can we do with the linearizability definition?
  for designer: could this optimization result in non-linearizable results?
  for programmer: what can I assume / expect as a client?
  for testing: generate requests, check observed history.

why is it called "linearizability"?
  the linearization points turn concurrent operations
    into a serial execution -- "linear".
  thus "linearizable" in the sense that the results are the same
    as some linear execution of the operations.

example 2:
  |-Wx1-| |----Wx2----|
                  |-Rx1-|
  linearizable?
    if yes, linearization points?
  so: concurrent read/write can go either way

example 3: like example 2, but...
  |-Wx1-| |----Wx2----|
    |---Rx2---|
                |-Rx1-|
we can try a few assignments of linearization points.
  how about Wx1 Wx2 Rx2 Rx1?
  not valid because "Wx2 Rx1" doesn't conform to serial spec.
how to show something *isn't* linearizable?
  show that no assignment of points works.
  i.e. breaks either time rule or value rule.
no assignment works for example 2!
  Wx2's point must be before Rx2's point
  so Wx2's point is also before Rx1's point
  so the second read got an impossible value
thus, if a system can produce this history, we know the
  system isn't linearizable: has a bug, or never promised linearizability.
the Rx1 *would* have been legal if there had been no Rx2
  reads -- not just writes -- can affect what's subsequently legal
so, if we want linearizability:
  once any read sees a write, all strictly-subsequent reads must also see it.
    rules out split-brain
  can't forget a revealed write
    rules out e.g. forgetting data due to a crash

GFS is not linearizable: it can produce example 3 since
  the Rx1 could come from a replica that hasn't yet been updated.
  if we wanted GFS to be linearizable,
    one approach is to have client reads go through the primary too.
    would be slower!

example 4:
|--Wx0--|  |--Wx1--|
            |--Wx2--|
C1:      |-Rx2-| |-Rx1-|
C2:      |-Rx1-| |-Rx2-|
can there be a serial order?
  C1 needs Wx2 Rx2 Wx1 Rx1
  C2 needs Wx1 Rx1 Wx2 Rx2
  we can't have both Wx2 before Wx1, and Wx2 after Wx1.
  so not linearizable.
so:
  service can choose either order for concurrent writes
  but all clients must see the writes in the same order
  this is important when there are replicas or caches
    they must all appear to execute operations in the same order

example 5:
|-Wx1-|
        |-Wx2-|
                |-Rx1-|
no order is possible -- not linearizable
so:
  linearizability rules out "stale" reads
  even if the reader doesn't know there was a write
    the time rule requires reads to see the latest completed update
  "no stale data" restricts caching and replication designs

linearizability outlaws many design possibilities / anomalies:
  split brain (two active leaders)
  forgetting completed writes after a crash+reboot
  reading from lagging replicas or out-of-date caches

example 6:
[client / network / server diagram]
C1 sends put(x, 1)
C2 sends put(x, 2)
service receives C1's request;
  network drops response;
  C1's RPC library re-sends request
is it legal for service to execute *both* of C1's request messages?
we then might see this if C3 reads three times:
C1: |--------Wx1---------| (due to retransmission)
C2:        |-Wx2-|
C3:   |-Rx1-| |-Rx2-|  |-Rx1-|
this history is not linearizable!
so, if we want linearizability:
  repeated requests from retransmissions should only execute once!
  the Raft paper mentions this issue (and a solution) in Section 8

linearizable systems are not limited to just read and write operations
  increment
  append
  test-and-set (to implement locks)
  any self-contained single-object operation

application programmers like linearizability:
  * reads see latest data -- never stale
  * all clients see the same data (when there aren't writes)
  * all clients see data changes in the same order
    so my put(v,27); put(done,true); example works
  these benefits will be clearer when we look at weaker consistencies.

can we have better semantics than linearizability?
  or, what might you want that linearizability won't support?
  * transactions: atomic *groups* of operations on different objects
    linearizability only thinks about individual operations, not groups of ops
  * operations that themselves invoke other services

how can we implement linearizability?
  depends on how much replication, caching, and fault-tolerance we want.

single serial server that doesn't crash.
  [diagram: clients, server, op queue, state]
  server picks an order for concurrently arriving client requests.
  executes them in that order, one at a time,
    replies to each before starting the next.
  plus duplicate request suppression

this simple implementation directly implements linearizability definition
  executes in serial order, one at a time
  executes an operation after it is received
    and doesn't reply until after operation is finished
  (thus can't eagerly reply to put()s)

good news: server does not have to reason about histories,
  linearization points, or concurrency.

what about fault-tolerant linearizability?
  primary-backup replication
    like GFS but would need modifications e.g. read only via primary
  Raft!
  and we'll see more examples

what about the performance of linearizable systems?

a fundamental performance limit imposed by no-stale-reads rule:
  [W, E]
  a write on the west coast, which completes
  then a read on the east coast
  there *must* be communication of the written value
    *and* either reader or writer must wait for communication
  speed of light is a serious problem:
    dozens of milliseconds cross-country
    can limit serial throughput to a few tens per second

generally:
  bad news: serial aspect makes it hard to get parallel speedup for individ objs
  bad news: if replication, then lots of communication and waiting
  bad news: if replication, replicas must be reachable,
            limiting fault tolerance
  good news: you can shard by key

what about other consistency models?
  can they allow better performance?
  do they have intuitive semantics?

example: eventual consistency -- a weak model
  [diagram: two copies, clients]
  multiple copies of the data (e.g. in different datacenters, for speed)
  a read consults any replica (e.g. closest)
  a write updates any replica (e.g. closest)
    replica sends response when that one update is done
  replicas synchronize updates in the background
    eventually, other replicas will see my update

many systems provide eventual consistency
  faster than linearizability
    especially if replicas are in different cities for fault-tolerance
  and more available -- any one replica will do
    no waiting for primary/backup communication or Raft quorum
  Amazon's Dynamo; Cassandra; GFS (sort of)

but eventual consistency exposes some anomalies to application programmer:
  * reads can see stale data
    a problem for password change, ACL change
  * writes may appear out of order
    breaks my result/done example
  * different clients may see different data
  * concurrent writes to same item need to be resolved somehow!
    C1: put(x, 1)
    C2: put(x, 2)
    may initially be applied at different replicas
    only later will they be pushed to other replicas
    how to merge concurrent new values?
    how to ensure all replicas choose the same final value?
      so that, eventually, they are identical?
  * eventual consistency cannot support e.g. test-and-set

A general pattern: you can usually choose only one of these:
  Strong consistency
  Maximum availability

But not both.
  Strong consistency makes you wait to update replicas,
    and can't proceed if too many replicas are unavailable.
    Thus poor availability.
  Eventual consistency can proceed even if no other replicas are reachable.
    But has poor consistency.

Next week:
  ZooKeeper, an interesting service built on a Raft-like protocol
    combines strong and weak consistency

---

https://jepsen.io/consistency/models

