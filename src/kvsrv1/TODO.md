
- [x] Key/value server with reliable network

Your first task is to implement a solution that works when there are no dropped messages. You'll need to add RPC-sending code to the Clerk Put/Get methods in client.go, and implement Put and Get RPC handlers in server.go.

You have completed this task when you pass the Reliable tests in the test suite: 

`make RUN="-run Reliable" kvsrv1`

- [x] Implementing a lock using key/value clerk

* application 上 处理并发

KV 的 version 相当于 CAS 版本号 (compare-and-swap)

Implement Acquire and Release. You have completed this exercise when your code passes these tests: 

You will need a unique identifier for each lock client; call kvtest.RandValue(8) to generate a random string. 

> If a client crashes while holding a lock, the lock will never be released. In a design more sophisticated than this lab, the client would attach a lease to a lock. When the lease expires, the lock server would release the lock on behalf of the client. In this lab clients don't crash and you can ignore this problem. 

`make RUN="-run Reliable" lock1`

- [x] Key/value server with dropped messages

* kv 上处理网络错误

To recover from discarded requests/replies, the Clerk must keep re-trying each RPC until it receives a reply from the server. 

* request fail: client rpc -> fail to arrive server -> client resend
* reply fail: client rpc -> server receive -> ok -> reply -> fail

GET: 
  client retry -> server resend

PUT:
  client retry -> server fail ErrVersion
  or
  Other client sent -> server faile ErrVersion

Therefore, if a Clerk receives rpc.ErrVersion for a retransmitted Put RPC, Clerk.Put must return rpc.ErrMaybe to the application instead of rpc.ErrVersion since the request may have been executed.

It is then up to the application to handle this case. If the server responds to an initial (not retransmitted) Put RPC with rpc.ErrVersion, then the Clerk should return rpc.ErrVersion to the application, since the RPC was definitely not executed by the server. 

* 如果要在 Put 上保证实现 exactly once，需要在 server 上为每个 Clerk 维护 state
* 在 Application 上实现 at-most-once Clerk.Put

modify your kvsrv1/client.go to continue in the face of dropped RPC requests and replies. 

A return value of true from the client's ck.clnt.Call() indicates that the client received an RPC reply from the server; a return value of false indicates that it did not receive a reply (more precisely, Call() waits for a reply message for a **timeout interval**, and returns false if no reply arrives within that time). 

Your Clerk should keep re-sending an RPC until it receives a reply. Keep in mind the discussion of rpc.ErrMaybe above. Your solution shouldn't require any changes to the server. 

Before the client retries, it should wait a little bit; you can use go's time package and call time.Sleep(100 * time.Millisecond)

Add code to Clerk to retry if doesn't receive a reply. Your have completed this task if your code passes all the tests for kvsrv1: 

`make kvsrv1`

- [ ] Implementing a lock using key/value clerk and unreliable network

Modify your lock implementation to work correctly with your modified key/value client when the network is not reliable. You have completed this exercise when your code passes all the lock1 tests: 

`make lock1`