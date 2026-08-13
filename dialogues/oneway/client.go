package main

import (
	"fmt"
	"net"
	"strconv"
	"time"
)

func PrintError(err error) {
	if err != nil {
		fmt.Println("Error:", err)
	}
}

func main() {
	ServerAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:10001")
	PrintError(err)

	LocalAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	PrintError(err)

	Conn, err := net.DialUDP("udp", LocalAddr, ServerAddr)
	PrintError(err)

	defer Conn.Close()

	i := 0
	for {
		msg := strconv.Itoa(i)
		i++
		buf := []byte(msg)
		_, err := Conn.Write(buf)
		PrintError(err)
		time.Sleep(time.Second * 1)
	}
}
