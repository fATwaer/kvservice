package realrpc

import (
	"net/rpc"
)

type ClientEnd interface {
	call(method string, args interface{}, reply interface{}) bool
	conn() *rpc.Client
}


