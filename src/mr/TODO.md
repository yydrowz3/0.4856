- [x] 1. 定义 RPC 类型
- [x] 2. Coordinator 分配 Map task
- [x] 3. Worker 执行 Map task
  - [x] 8. 使用临时文件和原子 rename
- [x] 4. Coordinator 等待所有 Map 完成
- [x] 5. Worker 执行 Reduce task
- [x] 6. 实现 Done 和 Exit
- [ ] 7. 加入任务超时重试
- [ ] 9. 运行 crash、parallel、early-exit 测试


---

Simple Verfication Steps:

```go
cd src/main
go build -buildmode=plugin ../mrapps/wc.go
go run mrcoordinator.go sock123 pg-being_ernest.txt
```

```go
cd src/main
go run mrworker.go wc.so sock123
```

