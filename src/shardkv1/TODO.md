
We supply you with tests and skeleton code in src/shardkv1:

- client.go for the shardkv clerk
- shardcfg package for computing shard configurations
- shardgrp package: for the shardgrp clerk and server.
- shardctrler package, which contains shardctrler.go with methods for the controller to change a configuration (ChangeConfigTo) and to get a configuration (Query) 

Reference: BigTable, Spanner, FAWN, Apache HBase, Rosebud, Spinnaker. 

---

- [ ] Part A: Moving shards

In Part A: 
  implement shardctrler -> store and retrieve configurations in a kvsrv
  implement shardgrp -> replicated with your Raft rsm package, and a corresponding shardgrp clerk. 
  The shardctrler talks to the shardgrp clerks to move shards between different groups.

Implement InitConfig and Query, and store the configuration in kvsrv. You're done when your code passes the first test. Note this task doesn't require any shardgrps. 

> Implement InitConfig and Query by storing and reading the initial configuration from kvsrv: use the Get/Put methods of ShardCtrler.IKVClerk to talk to kvsrv, use the String method of ShardConfig to turn a ShardConfig into a string that you can pass to Put, and use the shardcfg.FromString() function to turn a string into a ShardConfig. 

```
make RUN="-run TestInitQuery5A" shardkv
```

Implement an initial version of shardgrp in shardkv1/shardgrp/server.go and a corresponding clerk in shardkv1/shardgrp/client.go by copying code from your Lab 4 kvraft solution.

Implement a clerk in shardkv1/client.go that uses the Query method to find the shardgrp for a key, and then talks to that shardgrp. You're done when your code passes the Static test. 

> Copy code from your kvraft client.go and server.go for Put and Get, and any other code you need from kvraft.
> The code in shardkv1/client.go provides the Put/Get clerk for the overall system: it finds out which shardgrp holds the desired key's shard by invoking the Query method, and then talks to the shardgrp that holds that shard.
> Implement shardkv1/client.go, including its Put/Get methods. Use shardcfg.Key2Shard() to find the shard number for a key. The tester passes a ShardCtrler object to MakeClerk in shardkv1/client.go. Retrieve the current configuration using the Query method.
> To put/get a key from a shardgrp, the shardkv clerk should create a shardgrp clerk for the shardgrp by calling shardgrp.MakeClerk, passing in the servers found in the configuration and the shardkv clerk's ck.clnt. Use the GidServers() method from ShardConfig to get the group for a shard.
> shardkv1/client.go's Put must return ErrMaybe when the reply was maybe lost, but this Put invokes shardgrp's Put to talk a particular shardgrp. The inner Put can signal this with an error.
> Upon creation, the first shardgrp (shardcfg.Gid1) should initialize itself to own all shards. 

```
make RUN="-run TestStatiOneShardGroup5A" shardkv
```

Implement ChangeConfigTo (in shardctrler/shardctrler.go) and extend shardgrp to support freeze, install, and delete. ChangeConfigTo should always succeed in Part A because the tester doesn't induce failures in this part. You will need to implement FreezeShard, InstallShard, and DeleteShard in shardgrp/client.go and shardgrp/server.go using the RPCs in the shardgrp/shardrpc package, and reject old RPCs based on Num. You will also need modify the shardkv clerk in shardkv1/client.go to handle ErrWrongGroup, which a shardgrp should return if it isn't reponsible for the shard.

You have completed this task when you pass the JoinBasic and DeleteBasic tests. These tests focus on adding shardgrps; you don't have to worry about shardgrps leaving just yet. 

> A shardgrp should respond with an ErrWrongGroup error to a client Put/Get with a key that the shardgrp isn't responsible for (i.e., for a key whose shard is not assigned to the shardgrp). You will have to modify shardkv1/client.go to reread the configuration and retry the Put/Get.
> Note that you will have to run FreezeShard, InstallShard, and DeleteShard through your rsm package, just like Put and Get.
> You can send an entire map as your state in an RPC request or reply, which may help keep the code for shard transfer simple.
> If one of your RPC handlers includes in its reply a map (e.g. a key/value map) that's part of your server's state, you may get bugs due to races. The RPC system has to read the map in order to send it to the caller, but it isn't holding a lock that covers the map. Your server, however, may proceed to modify the same map while the RPC system is reading it. The solution is for the RPC handler to include a copy of the map in the reply. 

Extend ChangeConfigTo to handle shard groups that leave; i.e., shardgrps that are present in the current configuration but not in the new one. Your solution should pass TestJoinLeaveBasic5A now. (You may have handled this scenario already in the previous task, but the previous tests didn't test for shardgrps leaving.) 

Make your solution pass all Part A tests, which check that your sharded key/value service supports many groups joining and leaving, shardgrps restarting from snapshots, processing Gets while some shards are offline or involved in a configuration change, and linearizability when many clients interact with the service while the tester concurrently invokes the controller's ChangeConfigTo to rebalance shards. 

```
make RUN="-run 5A" shardkv
```


- [ ] Part B: Handling a failed controller

In Part B: 
  modify shardctrler -> handle failures and partitions during config changes

Modify shardctrler to implement the above approach. A controller that picks up the work from a failed controller may repeat FreezeShard, InstallShard, and Delete RPCs; shardgrps can use Num to detect duplicates and reject them. You have completed this task if your solution passes the Part B tests. 

> The tester calls InitController when starting a controller; you can implement recovery in that method in shardctrler/shardctrler.go. 

```
make RUN="-run 5B" shardkv
```

- [ ] Part C: Concurrent configuration changes

In Part C: 
  extend shardctrler -> allow for concurrent controllers without interfering with each other.

Modify your controller so that only one controller can post a next configuration for a configuration Num. The tester will start many controllers but only one should start ChangeConfigTo for a new configuation. You have completed this task if you pass the concurrent tests of Part C: 

> See concurCtrler in test.go to see how the tester runs controllers concurrently. 

```
make RUN="-run TestConcurrentReliable5C" shardkv
```

In this exercise you will put recovery of an old controller together with a new controller: a new controller should perform recovery from Part B. If the old controller was partitioned during ChangeConfigTo, you will have to make sure that the old controller doesn't interfere with the new controller. If all the controller's updates are already properly fenced with Num checks from Part B, you don't have to write extra code. You have completed this task if you pass the Partition tests. 

```
make RUN="-run Partition" shardkv
```

- [ ] Part D: Extend your solution

in Part D: 
  extend your solution in any way you like.

Implement one of the ideas below or come up with your own idea. Write a paragraph in a file extension.md describing your extension, and upload extension.md to Gradescope. If you would like to do one of the harder, open-ended extensions, feel free to partner up with another student in the class. 

- (easy) Change the tester to use kvraft instead of kvsrv (i.e., replace the kvsrv.StartKVServer in MakeTestMaxRaft in test.go with kvraft.StartKVServer) so that the controller uses your kvraft to store its configuration. Write a test that checks that the controller can query and update the configuration while one of the kvraft peers is down. The existing code for the tester is distributed across src/kvtest1, src/shardkv1, and src/tester1.
- (moderate) Change kvsrv to implement exactly-once semantics for Put/Get as in Lab 2 from last year (see the dropped messages part). You may be able to port over some tests from 2024 instead of having to write your own from scratch. Implement exactly-once also in your kvraft.
- (moderate) Change kvsrv to support a Range function, which returns all keys in the range low key to high key. The lazy way to implement Range is to iterate through the key/value map that the server maintains; a better way is to use a data structure that supports range searches (e.g., B-tree). Include a test that fails the lazy solution but passes on the better solution.
- (hard) Modify your kvraft implementation to allow the leader to serve Gets without running the Get through rsm. That is, implement the optimization described at the end of section 8 of the raft paper, including leases, to ensure that kvraft maintains linearizability. Your implementation should pass the existing kvraft tests. You should also add a test that checks that your optimized implementation is faster (e.g., by comparing the number of RPCs) and a test that checks that term switches are slower because a new leader must wait until the lease has expired.
- (hard) Support transactions in kvraft so that a developer can perform several Put and Gets atomically. Once you have transaction, you don't need version Puts anymore; transactions subsume versioned Puts. Look at etcd's transactions for an example interface. Write tests to demonstrate your extension works.
- (hard) Modify shardkv to support transactions so that a developer can perform several Puts and Gets atomically across shards. This requires implementing two-phase-commit and two-phase locking. Write tests to demonstrate that your extension works. 