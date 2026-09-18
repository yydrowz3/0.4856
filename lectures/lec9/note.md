6.5840 2026 Lecture 9: Zookeeper Case Study

Reading: "ZooKeeper: Wait-free coordination for Internet-scale
systems", Patrick Hunt, Mahadev Konar, Flavio P. Junqueira, Benjamin
Reed. 2010 USENIX Annual Technical Conference.

today's lecture considers ZooKeeper from two angles:
  * a simpler foundation for fault-tolerant applications.
  * an example use of Raft-like replication
  ZooKeeper is very widely used, so worth paying attention
  Etcd, another popular coordination service, is influenced by ZooKeeper's design

if we wanted to make a fault-tolerant service like MR coordinator,
  we could replicate with Raft, and that would be OK!
  [diagram: Raft-replicated MR coordinator, workers]

but building directly on Raft is hard
  a replicated state machine is awkward to program
  everything framed as events, commit them, then execute them
  is there a simpler way?

you can think of state machine replication (Raft) as replicating
  the computation; the state is replicated as a side-effect.

can we have fault-tolerance without replicating computation?
  yes!
  ordinary non-replicated server
  server maintains state in fault-tolerant storage service
  server crash -> new server, reads state from storage

ZooKeeper is designed to be the required fault-tolerant storage
  [MR coord, workers, ZK black box]
  MR coordinator can be written in ordinary straight-line code
    write state updates to ZK
    much like saving in a file

what might MR coord store in ZK?
  coord's IP addr, set of jobs, status of tasks, set of workers, assignments
  update data in ZK on each change
  (but big data itself in GFS, not ZK)
  workers can read coord's IP address, maybe even task assignments, from ZK
  MR using ZK for "configuration management"
    keep track of a collection of servers
    help servers find each other

what if MR coord fails?
  we weren't replicating it on a backup coord server
  but we don't need one!
  just pick any computer, start MR coord s/w on it,
    have it read state from ZK.
  new coord can pick up where failed one left off.
  makes a lot of sense in a big cloud
    easy to allocate a replacement server

challenges
  * failure detection (of MR coord)
  * election (just one MR coord at a time -- no split brain!)
  * recover/repair state from ZK (old MR coord might
    have been in the middle of updating it)
  * deal with possibility old MR coord is still alive and active!
  * performance
  ZK helps with all of these

ZooKeeper server arrangement
  [ZK leader, ZK followers, clients, writes, ZXIDs, reads, watches]
  Raft-like leader
  Raft-like log, commit, replicated execution and state
  ZK leader chooses order for incoming writes,
    assigns ZXIDs,
    all followers execute writes in the same order
  ZK followers execute client reads (reads are not sent to ZK leader)

for now I'll treat ZK as a black box

Zookeeper data model (Figure 1)
  a file-system-like tree of znodes
  znode names, znode content, children, path names
    names and hierarchy help different apps avoid interfering
  each znode has a version number
  types of znodes:
    regular
    ephemeral
    sequential: name + seqno

Operations (Section 2.2)
  s = openSession()
  create(s, path, data, flags)
    exclusive -- fails if path already exists
  exists(s, path, watch)
    watch=true asks for notification if path is later created/deleted
  getData(s, path, watch) -> data, version
  setData(s, path, data, version)
    if znode.version = version, then update
    (same version scheme as Lab 2)
  getChildren(s, path, watch)
  exception if the ZK server says it has terminated the session
    so that application won't continue

ZooKeeper API designed for synchronization and concurrent access:
  + exclusive znode creation; exactly one concurrent create returns success
  + getData()/setData(x, version) supports mini-transactions
  + sessions/ephemeral help cope with client failure
  + sequential znodes create order among multiple clients
  + watches avoid costly polling

Example: MapReduce coordinator election
  this is the paper's Simple Lock in Section 2.4
    s = openSession()
    while true:
      if create(s, "/mr/c", ephemeral=true)
        // we won this election and are now coordinator
        setData(s, "/mr/ip", ...)
        setData(s, "/mr/...", ...)
	...
	break
      else if exists(s, "/mr/c", watch=true)
        // we lost this election
        wait for watch event

note:
  exclusive create
    if multiple clients concurrently attempt, only one will succeed
  ephemeral znode
    coordinator failure automatically lets new coordinator be elected
  watch
    potential replacement coordinators can wait w/o polling

what do we want if the elected coordinator fails?
  * elect a replacement
  * cope with crash in the middle of updating state in ZK
  * cope with possibility that the coordinator *didn't* fail!
  even though /mr/c looks like a lock,
    the possibility of coordinator failure makes the situation
    different from e.g. Go sync.Mutex

what does ZK do on failure of client (e.g. MR coordinator)?
  client failure -> client stops sending keep-alive messages to ZK
  no keep-alives -> ZK leader times out and terminates the session
  session termination -> ZK leader deletes session's ephemeral znodes
                         *and* ignores further requests from that session
                         (ephemeral deletions are A-linearizable ZK ops)
  now a new MR coordinator can elect itself

what if the MR coordinator crashed while updating state in ZK?
  this is crash recovery, related to DB logging; requires care.
  simplest: MR coord stores all info in a single ZK znode
    individual setData() calls are atomic (all or nothing vs failure)
  what if MR coord stores state in multiple znodes?
    use paper's "ready" znode scheme (Section 2.3)
      delete "ready"; update znodes; create "ready"
      leader:                          worker:
         delete(s, "ready")              
	 setData(s, z1)
	 setData(s, z2)                if exits("ready", watch=ready):
	 create(s, "ready)                read z1
	 	                          read z2
        
    newly elected MR coord can then tell if update was partial
  better: write entirely new set of znodes, then update
    a znode that indicates which set is current
  all three end with "single committing write"
  
what if the old coordinator is alive and thinks it is still coordinator?
  but ZK has decided it is dead and deleted its ephemeral /mr/c znode?
  and a new coordinator is elected?
  will two computers think they are the coordinator?
    this could happen.
  can the old coordinator modify state in ZK?
    this cannnot happen!
  when ZK times out a client's session, two things happen atomically:
    ZK deletes the client's ephemeral nodes.
    ZK stops listening to the session -- will reject all operations.
  so old coordinator can no longer modify or read data in ZK!
    if it tries, its client ZK library will raise an exception
    forcing the client to realize it is no longer coordinator

"Fencing" is a term for ignoring requests from a client declared dead
  even if it is actually alive

an important pattern in distributed systems:
  a single entity (e.g. ZK) decides which computers are alive or dead
    "failure detector"
  it may not be correct, e.g. if the network drops messages
  but everyone obeys its decisions
  agreement is more important than being right, to avoid split brain
  but possibility of being wrong => need to fence
    thus ZK's session termination

how is ZK designed for good performance?
  optimized primarily for read/watch performance
    write performance is secondary
  [diagram: leader, lots of followers, clients talk to followers]
  1) many ZK follower servers; clients are spread over them for parallelism
     client sends all operations to its ZK follower
     ZK follower executes reads locally, from its replica of ZK data
       to avoid loading the ZK leader
     ZK follower forwards only writes to ZK leader
  2) watch, not poll
     the ZK follower (not the ZK leader) keeps watch info
  3) clients of ZK can launch async operations
     i.e. send request; completion notification delivered to code separately
          unlike RPC
     a client can launch many ops without waiting
     ZK processes async ops efficiently in a batch; fewer msgs, disk writes
     client library numbers them, ZK executes them in that order
     e.g. to update a bunch of znodes then create "ready" znode

a ZK read may not see latest completed writes!
  since client's follower may be behind (not in write's majority)
  when is it ok for reads not to see recent writes?
    data merely displayed to humans
    read-only data
    data that can be checked e.g. GFS chunk-server
  when is it not ok?
    read-modify-write e.g. to increase a counter
    when a group of items needs to be consistent

ZK does provide some guarantees for reads
  every client sees writes appear in the same order (ZXID)
  a client read sees all of its own previous writes
    so follower may have to delay a read
  a client's reads move only forward in time (by ZXID)
    even if client switches ZK followers!
  "client FIFO order"
     good enough the examples above

Some implementation details related to performance:
  Data must fit in memory, so reads are fast (no need to read disk).
    So you can't store huge data in ZooKeeper.
  ZK logs writes to disk.
    So committed updates aren't lost in a crash or power failure.
    Hurts performance; batching can help throughput.
  Periodically, ZK writes complete snapshots to disk.
    So it can truncate on-disk log.
    Fuzzy technique allows snapshotting concurrently with write operations.

How is the performance?

Figure 5 -- throughput.
  Overall, can handle 10s of thousands of operations / second.
    Is this a lot? Enough?
  Why do the lines go up as they move to the right?
  Why does the x=0 performance go down as the number of servers increases?
  Why does the "3 servers" line change to be worst at 100% reads?
  What might limit it at x=0 to 20,000?
    Each op is a 1000-byte write...

What about latency?
  Why might we care about latency? What's a good target?
  Table 2 / Section 5.2 implies 1.3 milliseconds (1 / 776).
    For a single worker (client) waiting after each write request.
  Where might the 1.3 milliseconds come from?
    Disk writes? Communication? Computation?
    (How can it be this fast, given mechanical disk rotation times?)
  Why only ~2000 req/s rather than Figure 5's 20,000?

How long to recover from a ZK server failure?
  Is this an important consideration?
  Figure 8
  Leader failure -> a pause for timeout and election.
    Visually, on the order of a few seconds.
  Follower failure -> brief decrease in total throughput.
    Why not a long pause for timeout?
  What are the leader recovery time tradeoffs/obstacles likely to be?

ZooKeeper has been very successful
  good foundation for building fault-tolerant applications
  see ZooKeeper's Wikipedia page for a list of projects that use it

Areas it could be improved?
  too bad reads aren't linearizable
  sessions are pretty blunt; maybe better to have per-znode leases
  not easy to shard:
    znode tree has no obvious places to slice
    sessions are global
  multi-znode transactions would be nice
  see etcd and consul for other design decisions

Next week:
  distributed transactions

References:
  https://zookeeper.apache.org/doc/r3.4.8/api/org/apache/zookeeper/ZooKeeper.html
  ZAB: http://dl.acm.org/citation.cfm?id=2056409
  https://zookeeper.apache.org/
  https://cs.brown.edu/~mph/Herlihy91/p124-herlihy.pdf  (wait free, universal
  objects, etc.)

