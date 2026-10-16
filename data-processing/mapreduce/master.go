package mapreduce

import (
	"log"
	"net"
	"net/rpc"
	"sync"
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

func (master *Master) schedule(task *Task, proc string, filePathChan chan string) int {
	//Change this code
	var (
		waitGroup sync.WaitGroup
		filePath  string
		worker    *RemoteWorker
		operation *Operation
		counter   int
	)
	log.Printf("Scheduling %v operations\n", proc)
	counter = 0
	for filePath = range filePathChan {
		operation = &Operation{proc, counter, filePath}
		counter++
		worker = <-master.idleWorkerChan
		waitGroup.Add(1)
		go master.runOperation(worker, operation, &waitGroup)
	}
	waitGroup.Wait()
	log.Printf("%vx %v operations completed\n", counter, proc)
	return counter
}

func (master *Master) runOperation(remoteWorker *RemoteWorker, operation *Operation, waitGroup *sync.WaitGroup) {
	//Change this code
	var (
		err  error
		args *RunArgs
	)
	log.Printf("Running %v (ID: '%v' File: '%v' Worker: '%v')\n", operation.proc, operation.id, operation.filePath, remoteWorker.id)
	args = &RunArgs{operation.id, operation.filePath}
	err = remoteWorker.callRemoteWorker(operation.proc, args, new(struct{}))
	if err != nil {
		log.Printf("Operation %v '%v' Failed. Error: %v\n", operation.proc, operation.id, err)
		waitGroup.Done()
		master.failedWorkerChan <- remoteWorker
	} else {
		waitGroup.Done()
		master.idleWorkerChan <- remoteWorker
	}
}
