package raft

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
)

func (rf *Raft) listen() {
	rpcs := rpc.NewServer()
	rpcs.Register(rf)
	listenQueue, err := net.Listen("tcp", rf.peers[rf.me].Peer)
	if err != nil {
		log.Fatal("listen error", err)
	}
	fmt.Println("raft server listen at", rf.peers[rf.me].Peer)
	for {
		conn, err := listenQueue.Accept()
		fmt.Println("new connect", conn.RemoteAddr())
		if err != nil {
			log.Println(err)
		} else {
			go rpc.ServeConn(conn)
		}
	}
	listenQueue.Close()
}
