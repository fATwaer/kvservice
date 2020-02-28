package main

import (
	kv "kvservice/kvraft"
	"kvservice/realrpc"
	"log"
	"strconv"
)

var kvpeers = []string {"106.13.211.207:20201", "fatwaer.store:20202", "34.92.0.178:20203"}

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
