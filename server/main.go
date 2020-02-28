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
	"strings"
)

type server struct {
	me int
	kv *raftkv.KVServer
	rf *raft.Raft
	rfpeers []string
	kvpeers []string
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

func listen(rpcs *rpc.Server, port string) {
	listenQueue, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatal("listen error", err)
	}
	fmt.Println("kv server listen at", port)
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
	rfs := GetPeers(s.rfpeers)
	s.kv = raftkv.StartKVServer(rfs, s.me, &raft.Persister{}, 2000)
	s.rf = s.kv.GetRaft()
	kvrpcs := rpc.NewServer()
	kvrpcs.Register(s.kv)
	port := ":"+strings.Split(s.kvpeers[s.me], ":")[1]
	go listen(kvrpcs, port)
}

func (s *server)listenAsRaft() {
	rfrpcs := rpc.NewServer()
	rfrpcs.Register(s.rf)
	port := ":"+strings.Split(s.rfpeers[s.me], ":")[1]
	listen(rfrpcs, port)
}

func (s *server)serv() {
	// load peer information
	ch := make(chan int)
	s.listenAsKvServer()
	s.listenAsRaft()
	<- ch
}

func main() {

	cfg := GetConfig("server/config.json")

	if len(os.Args) < 2 {
		fmt.Printf("usage: %s idx\n", os.Args[0])
		for i := 0; i < len(cfg.KVPeers); i++ {
			fmt.Printf("%d : kv(%s) rf(%s)\n", i, cfg.KVPeers[i], cfg.RFPeers[i])
		}
		return
	}
	me, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println(err)
		return
	}

	s := server{
		me:      me,
		kv:      nil,
		rf:      nil,
		rfpeers: cfg.RFPeers,
		kvpeers: cfg.KVPeers,
	}
	s.serv()
}
