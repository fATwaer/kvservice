package realrpc

import (
	"log"
	"net/rpc"
	"sync"
)

type ClientEnd struct {
	Endname string   // this end-point's name
	Peer string 	 // hostname:port
	mu sync.Mutex
	conn *rpc.Client
}


// send an RPC, wait for the reply.
// the return value indicates success; false means that
// no reply was received from the server.
func (e *ClientEnd) Call(svcMethod string, args interface{}, reply interface{}) bool {
	if e.conn == nil && e.connect() == false {
		return false
	}

	err := e.conn.Call(svcMethod, args, reply)
	if err != nil {
		log.Println("error:", err)
		e.mu.Lock()
		if e.conn != nil {
			e.conn.Close()
		}
		e.conn = nil
		e.mu.Unlock()
		return false
	}
	return  true
}

func (e *ClientEnd) connect() bool {
	client, err := rpc.Dial("tcp", e.Peer)
	if err != nil {
		log.Println(err)
		return false
	}
	e.conn = client
	return true
}