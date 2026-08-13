package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"time"
)

type Message struct {
	Code int
	Text string
}

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

func readInput(ch chan string) {
	reader := bufio.NewReader(os.Stdin)
	for {
		text, _, _ := reader.ReadLine()
		ch <- string(text)
	}
}

func doServerJob() {
	buf := make([]byte, 1024)
	for {
		n, addr, err := ServerConn.ReadFromUDP(buf)
		PrintError(err)
		var msg Message
		err = json.Unmarshal(buf[:n], &msg)
		fmt.Println("Received", msg.Code, "-", msg.Text, "from", addr)
		PrintError(err)
	}
}

func doClientJob(i int, j int) {
	msg := Message{200, "Communication test"}
	jsonMsg, err := json.Marshal(msg)
	PrintError(err)
	_, err = ClientConn[j].Write(jsonMsg)
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
	ch := make(chan string)
	go readInput(ch)
	go doServerJob()
	for {
		select {
		case text, valid := <-ch:
			if valid {
				fmt.Printf("Sent %s from keyboard\n", text)
				for j := 0; j < nServers; j++ {
					go doClientJob(0, j)
				}
			} else {
				fmt.Println("Closed channel")
			}
		default:
			time.Sleep(time.Second * 1)
		}
		time.Sleep(time.Second * 1)
	}
}
