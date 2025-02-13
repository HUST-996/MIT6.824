package kvsrv

type MessageType int

const (
	ACK = iota
	MODIFY
)

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type       MessageType
	ReqID      int64
	ReceivedID int64
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Value string
}
