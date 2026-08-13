package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"time"
)

var nServers int
var ClientConn []*net.UDPConn
var ServerConn *net.UDPConn

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

func doServerJob() {
	buf := make([]byte, 1024)
	for {
		n, addr, err := ServerConn.ReadFromUDP(buf)
		fmt.Println("Received", string(buf[0:n]), "from", addr)
		PrintError(err)
	}
}

func doClientJob(i int, j int) {
	msg := strconv.Itoa(i)
	buf := []byte(msg)
	_, err := ClientConn[j].Write(buf)
	PrintError(err)
}

func initConn() {
	localPort := os.Args[1]
	nServers = len(os.Args) - 2
	ClientConn = make([]*net.UDPConn, nServers)
	ServerAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1"+localPort)
	CheckError(err)
	ServerConn, err = net.ListenUDP("udp", ServerAddr)
	CheckError(err)
	for i := 0; i < nServers; i++ {
		ServerAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1"+os.Args[i+2])
		PrintError(err)
		Conn, err := net.DialUDP("udp", nil, ServerAddr)
		PrintError(err)
		ClientConn[i] = Conn
	}
}

func main() {
	initConn()
	defer ServerConn.Close()
	for i := 0; i < nServers; i++ {
		defer ClientConn[i].Close()
	}
	go doServerJob()
	i := 0
	for {
		for j := 0; j < nServers; j++ {
			go doClientJob(i, j)
		}
		time.Sleep(time.Second * 1)
		i++
	}
}
