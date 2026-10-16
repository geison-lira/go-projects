package mapreduce

import (
	"log"
	"net"
	"net/rpc"
	"sync"
)

const (
	IDLE_WORKER_BUFFER     = 100
	RETRY_OPERATION_BUFFER = 100
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
