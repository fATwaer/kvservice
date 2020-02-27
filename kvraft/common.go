package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
	// define for debug
	ErrNotLeader = "Server Is Not Leader"
	ErrDuplicateOp = "Operation has been committed."
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string `json: "Key"`
	Value string `json: "Value"`
	Op    string `json: "Op"`// "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id     UniqueID `json: "Id"`
}

type UniqueID struct {
	Cid int		`json: "Cid"`
	Sid int 	`json: "Sid"`
	SeriesN int `json: "SeriesN"`
}

type Namespace struct {
	Sid int
	Cid int
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string	`json: "Key"`
	// You'll have to add definitions here.
	Id  UniqueID `json: "Id"`
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

type RegisterArgs struct {

}

type RegisterReply struct {
	Cid int
	Sid int
	IsLeader bool
}