package main

import (
	kv "kvservice/kvraft"
	"kvservice/realrpc"
	"log"
	"strconv"
)

var kvpeers = []string {"106.13.211.207:20201", "fatwaer.store:20202", "18.162.39.157:20203"}

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

	for j := 0; j < 10; j++ {
		cli.Put(strconv.Itoa(j), "")
		for i := 0; i < 20; i++ {
			cli.Append(strconv.Itoa(j),strconv.Itoa(i)+" ")
		}
		log.Println(cli.Get(strconv.Itoa(j)))
	}
}
