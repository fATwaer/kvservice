package realrpc

import (
	"fmt"
	"log"
	"net/rpc"
)

type ClientEnd struct {
	Endname string   // this end-point's name
	Peer string 	 // hostname:port
}


// send an RPC, wait for the reply.
// the return value indicates success; false means that
// no reply was received from the server.
func (e *ClientEnd) Call(svcMethod string, args interface{}, reply interface{}) bool {
	client, err := rpc.Dial("tcp", e.Peer)
	if err != nil {
		log.Println(err)
		return false
	}
	fmt.Printf("mtd %s %v \n", svcMethod, args)
	err = client.Call(svcMethod, args, reply)
	if err != nil {
		log.Println("error:", err)
		return false
	}
	client.Close()
	fmt.Println("rpc done")
	return  true
}

