package main

import (
	kv "kvservice/kvraft"
	"kvservice/realrpc"
	"log"
	"strconv"
)

var kvpeers = []string {"localhost:20201", "localhost:20202", "localhost:20203"}

//
// Client
//

func GetPeers(allPeers []string) []*realrpc.ClientEnd {
	peers := []*realrpc.ClientEnd{}
	for idx, host := range allPeers {
		peers = append(peers, &realrpc.ClientEnd{
			Endname: string(idx),
			Peer:    host,
		})
	}
	return peers
}


func main()  {
	cli := kv.MakeClerk(GetPeers(kvpeers))
	cli.Put("hello", "world")
	log.Println(cli.Get("hello"))

	cli.Put("1", "")
	for i := 0; i < 100; i++ {
		cli.Append("1",strconv.Itoa(i)+" ")
	}
	log.Println(cli.Get("1"))
}
