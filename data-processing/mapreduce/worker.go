package mapreduce

import (
	"encoding/json"
	"io"
	"log"
	"net"
	"net/rpc"
	"os"
	"time"
)

type Worker struct {
	id             int
	hostname       string
	masterHostname string
	listener       net.Listener
	rpcServer      *rpc.Server
	task           *Task
	done           chan bool
	taskCounter    int
	numOperations  int
}

type RemoteWorker struct {
	id       int
	hostname string
	status   string
}

func (worker *Worker) register() error {
	var (
		err   error
		args  *RegisterArgs
		reply *RegisterReply
	)
	log.Println("Registering with Master")
	args = new(RegisterArgs)
	args.WorkerHostname = worker.hostname
	reply = new(RegisterReply)
	err = worker.callMaster("Master.Register", args, reply)
	if err == nil {
		worker.id = reply.WorkerId
		worker.task.NumReduceJobs = reply.ReduceJobs
		log.Printf("Registered. WorkerId: %v (Settings = (ReduceJobs: %v))\n", worker.id, worker.task.NumReduceJobs)
	}
	return err
}

func (worker *Worker) acceptMultipleConnections() error {
	var (
		err     error
		newConn net.Conn
	)
	log.Printf("Accepting connections on: %v\n", worker.listener.Addr())
	for {
		newConn, err = worker.listener.Accept()
		if err == nil {
			go worker.handleConnection(&newConn)
		} else {
			log.Println("Failed to accept connection. Error:", err)
			break
		}
	}
	log.Println("Stopped accepting connections.")
	return nil
}

func (worker *Worker) handleConnection(conn *net.Conn) error {
	worker.rpcServer.ServeConn(*conn)
	(*conn).Close()
	return nil
}

func (worker *Worker) callMaster(proc string, args any, reply any) error {
	var (
		err    error
		client *rpc.Client
	)
	client, err = rpc.Dial("tcp", worker.masterHostname)
	if err != nil {
		return err
	}
	defer client.Close()
	err = client.Call(proc, args, reply)
	if err != nil {
		return err
	}
	return nil
}

func (worker *Worker) shouldFail(during bool) bool {
	if worker.numOperations == 0 {
		return false
	}
	worker.taskCounter++
	return worker.taskCounter == worker.numOperations
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

func (worker *Worker) RunMap(args *RunArgs, _ *struct{}) error {
	var (
		err       error
		buffer    []byte
		mapResult []KeyValue
	)
	if worker.shouldFail(false) {
		mapResult = make([]KeyValue, 0)
		storeLocal(worker.task, args.Id, mapResult)
		time.Sleep(time.Duration(100) * time.Millisecond)
		panic("Induced failure.")
	}
	log.Printf("Running map id: %v, path: %v\n", args.Id, args.FilePath)
	buffer, err = os.ReadFile(args.FilePath)
	checkError(err)
	mapResult = worker.task.Map(buffer)
	storeLocal(worker.task, args.Id, mapResult)
	return nil
}

func (worker *Worker) RunReduce(args *RunArgs, _ *struct{}) error {
	log.Printf("Running reduce id: %v, path: %v\n", args.Id, args.FilePath)
	var (
		err          error
		reduceResult []KeyValue
		file         *os.File
		fileEncoder  *json.Encoder
	)
	if worker.shouldFail(false) {
		file, err = os.Create(resultFileName(args.Id))
		checkError(err)
		file.Sync()
		file.Close()
		time.Sleep(time.Duration(100) * time.Millisecond)
		panic("Induced failure.")
	}
	data := loadLocal(args.Id)
	reduceResult = worker.task.Reduce(data)
	file, err = os.Create(resultFileName(args.Id))
	checkError(err)
	fileEncoder = json.NewEncoder(file)
	for _, value := range reduceResult {
		fileEncoder.Encode(value)
	}
	file.Close()
	return nil
}

func (worker *Worker) Done(_ *struct{}, _ *struct{}) error {
	log.Println("Done.")
	defer func() {
		close(worker.done)
	}()
	return nil
}
