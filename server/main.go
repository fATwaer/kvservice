package main

import (
	"fmt"
	raftkv "kvservice/kvraft"
	"kvservice/raft"
	"kvservice/realrpc"
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
)

// listen on local
var rfpeers = []string {"localhost:20001", "localhost:20002", "localhost:20003"}
// listen on local
var kvpeers = []string {"localhost:20201", "localhost:20202", "localhost:20203"}

type server struct {
	me int
	kv *raftkv.KVServer
	rf *raft.Raft
}

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

func listen(rpcs *rpc.Server, host string) {
	listenQueue, err := net.Listen("tcp", host)
	if err != nil {
		log.Fatal("listen error", err)
	}
	fmt.Println("kv server listen at", host)
	for {
		conn, err := listenQueue.Accept()
		if err != nil {
			log.Println(err)
		} else {
			go rpcs.ServeConn(conn)
		}
	}
	listenQueue.Close()
}

func (s *server)listenAsKvServer() {
	rfs := GetPeers(rfpeers)
	s.kv = raftkv.StartKVServer(rfs, s.me, &raft.Persister{}, -1)
	s.rf = s.kv.GetRaft()
	kvrpcs := rpc.NewServer()
	kvrpcs.Register(s.kv)
	go listen(kvrpcs, kvpeers[s.me])
}

func (s *server)listenAsRaft() {
	rfrpcs := rpc.NewServer()
	rfrpcs.Register(s.rf)
	listen(rfrpcs, rfpeers[s.me])
}

func (s *server)serv() {
	// load peer information
	ch := make(chan int)
	s.listenAsKvServer()
	s.listenAsRaft()
	<- ch
}

func main() {
	if len(os.Args) < 2 {
		fmt.Printf("usage: %s server idx\n", os.Args[0])
		for i, v := range kvpeers {
			fmt.Printf("%d : %s\n", i, v)
		}
		return
	}

	me, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println(err)
		return
	}

	s := server{me:me}
	s.serv()
}
