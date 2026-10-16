package mapreduce

import (
	"io"
	"log"
	"net"
	"net/rpc"
	"sync"
	"time"
)

const (
	IDLE_WORKER_BUFFER     int    = 100
	RETRY_OPERATION_BUFFER int    = 100
	WORKER_IDLE            string = "idle"
	WORKER_RUNNING         string = "running"
)

type Master struct {
	task             *Task
	address          string
	rpcServer        *rpc.Server
	listener         net.Listener
	workersMu        sync.Mutex
	workers          map[int]*RemoteWorker
	totalWorkers     int
	idleWorkerChan   chan *RemoteWorker
	failedWorkerChan chan *RemoteWorker
	//Add more properties
}

type RemoteWorker struct {
	id       int
	hostname string
	status   string
}

type Operation struct {
	proc     string
	id       int
	filePath string
}

func newMaster(address string) *Master {
	master := new(Master)
	master.address = address
	master.workers = make(map[int]*RemoteWorker, 0)
	master.idleWorkerChan = make(chan *RemoteWorker, IDLE_WORKER_BUFFER)
	master.failedWorkerChan = make(chan *RemoteWorker, IDLE_WORKER_BUFFER)
	master.totalWorkers = 0
	return master
}

func (master *Master) acceptMultipleConnections() {
	var (
		err     error
		newConn net.Conn
	)
	log.Printf("Accepting connections on: %v\n", master.listener.Addr())
	for {
		newConn, err = master.listener.Accept()
		if err == nil {
			go master.handleConnection(&newConn)
		} else {
			log.Println("Failed to accept connection. Error:", err)
			break
		}
	}
	log.Println("Stopped accepting connections.")
}

func (master *Master) handleFailingWorkers() {
	//Coming soon...
}

func (master *Master) handleConnection(conn *net.Conn) error {
	master.rpcServer.ServeConn(*conn)
	(*conn).Close()
	return nil
}

func (worker *RemoteWorker) callRemoteWorker(proc string, args any, reply any) error {
	var (
		err    error
		client *rpc.Client
	)
	client, err = rpc.Dial("tcp", worker.hostname)
	if err != nil {
		return err
	}
	defer client.Close()
	err = client.Call(proc, args, reply)
	if err == io.ErrUnexpectedEOF {
		var tmpClient *rpc.Client
		time.Sleep(time.Second)
		tmpClient, err = rpc.Dial("tcp", worker.hostname)
		if err == nil {
			tmpClient.Close()
			return nil
		}
		return err
	} else if err != nil {
		return err
	}
	return nil
}
