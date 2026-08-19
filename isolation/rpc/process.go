package main

import (
	"fmt"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

var localAddrStr string
var nServers int
var ClientConn []*rpc.Client

func CheckError(err error) {
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(0)
	}
}

func PrintError(err error) {
	if err != nil {
		fmt.Println("Error:", err)
	}
}

type Server struct{}

func (s *Server) ExecuteJob(msg string, response *string) error {
	fmt.Println(msg)
	*response = "Success"
	return nil
}

func doClientJob(i int, j int) {
	client := ClientConn[j]
	var response string
	msg := "Process " + localAddrStr + " sent " + strconv.Itoa(i)
	err := client.Call("Server.ExecuteJob", msg, &response)
	PrintError(err)
}

func initConn() {
	localAddrStr = os.Args[1]
	nServers = len(os.Args) - 2
	ClientConn = make([]*rpc.Client, nServers)
	server := new(Server)
	rpc.Register(server)
	listener, err := net.Listen("tcp", localAddrStr)
	CheckError(err)
	go rpc.Accept(listener)
	for i := 0; i < nServers; i++ {
		var client *rpc.Client
		var err error
		nRetries := 10
		for j := 0; j < nRetries; j++ {
			client, err = rpc.Dial("tcp", os.Args[i+2])
			if err == nil {
				break
			}
			PrintError(err)
			time.Sleep(time.Second * 1)
		}
		CheckError(err)
		ClientConn[i] = client
	}
}

func main() {
	initConn()
	for i := 0; i < nServers; i++ {
		defer ClientConn[i].Close()
	}
	i := 0
	for {
		for j := 0; j < nServers; j++ {
			go doClientJob(i, j)
		}
		time.Sleep(time.Second * 1)
		i++
	}
}
