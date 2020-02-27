package realrpc

import (
	"log"
	"net/rpc"
)

type ClerkEnd struct {
	endname string   // this end-point's name
	peer string 	 // hostname:port
}


// send an RPC, wait for the reply.
// the return value indicates success; false means that
// no reply was received from the server.
func (e *ClerkEnd) Call(svcMethod string, args interface{}, reply interface{}) bool {
	client := e.connect()
	err := client.Call(svcMethod, args, reply)
	if err != nil {
		log.Fatal("error:", err)
		return false
	}
	client.Close()
	return  true
}

func (e *ClerkEnd) connect() *rpc.Client {
	client, err := rpc.Dial("tcp", e.peer)
	if err != nil {
		log.Fatal("dailing:", err)
	}
	return client
}

