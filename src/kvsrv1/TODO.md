
- [x] Key/value server with reliable network

Your first task is to implement a solution that works when there are no dropped messages. You'll need to add RPC-sending code to the Clerk Put/Get methods in client.go, and implement Put and Get RPC handlers in server.go.

You have completed this task when you pass the Reliable tests in the test suite: 

`make RUN="-run Reliable" kvsrv1`

- [x] Implementing a lock using key/value clerk

KV 的 version 相当于 CAS 版本号 (compare-and-swap)

Implement Acquire and Release. You have completed this exercise when your code passes these tests: 

You will need a unique identifier for each lock client; call kvtest.RandValue(8) to generate a random string. 

`make RUN="-run Reliable" lock1`

- [ ] Key/value server with dropped messages

Add code to Clerk to retry if doesn't receive a reply. Your have completed this task if your code passes all the tests for kvsrv1: 

`make kvsrv1`

- [ ] Implementing a lock using key/value clerk and unreliable network

Modify your lock implementation to work correctly with your modified key/value client when the network is not reliable. You have completed this exercise when your code passes all the lock1 tests: 

`make lock1`