package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	data    map[string]string
	history map[int64]string // 用来记录已经处理过的请求ID
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.data[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.Type == ACK {
		// 表示客户端已收到消息
		delete(kv.history, args.ReceivedID)
		return
	}
	val, ok := kv.history[args.ReqID]
	if ok {
		reply.Value = val
		return
	}
	oldVal := kv.data[args.Key]
	kv.data[args.Key] = args.Value
	reply.Value = oldVal
	kv.history[args.ReqID] = oldVal
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.Type == ACK {
		// 表示客户端已收到消息
		delete(kv.history, args.ReceivedID)
		return
	}
	val, ok := kv.history[args.ReqID]
	if ok {
		reply.Value = val
		return
	}
	oldVal := kv.data[args.Key]
	kv.data[args.Key] = oldVal + args.Value
	reply.Value = oldVal
	kv.history[args.ReqID] = oldVal
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.history = make(map[int64]string)
	return kv
}
