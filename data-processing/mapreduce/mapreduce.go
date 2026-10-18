package mapreduce

import (
	"log"
	"net"
	"net/rpc"
	"os"
	"time"
)

func RunSequential(task *Task) {
	var (
		err        error
		mapCounter int = 0
		mapResult  []KeyValue
	)
	log.Print("Running Sequential...")
	err = os.Mkdir(REDUCE_PATH, os.ModePerm)
	checkError(err)
	err = RemoveContents(REDUCE_PATH)
	checkError(err)
	for v := range task.InputChan {
		mapResult = task.Map(v)
		storeLocal(task, mapCounter, mapResult)
		mapCounter++
	}
	mergeMapLocal(task, mapCounter)
	for i := 0; i < task.NumReduceJobs; i++ {
		data := loadLocal(i)
		task.OutputChan <- task.Reduce(data)
	}
	close(task.OutputChan)
}

func RunMaster(task *Task, hostname string) {
	var (
		err                error
		master             *Master
		rpcServer          *rpc.Server
		listener           net.Listener
		reduceFilePathChan chan string
		mapOperations      int
		reduceOperations   int
	)
	log.Println("Running Master on:", hostname)
	err = os.Mkdir(REDUCE_PATH, os.ModePerm)
	checkError(err)
	err = RemoveContents(REDUCE_PATH)
	checkError(err)
	master = newMaster(hostname)
	master.task = task
	rpcServer = rpc.NewServer()
	rpcServer.Register(master)
	if err != nil {
		log.Panicln("Failed to register RPC server. Error:", err)
	}
	master.rpcServer = rpcServer
	listener, err = net.Listen("tcp", master.address)
	if err != nil {
		log.Panicln("Failed to start TCP server. Error:", err)
	}
	master.listener = listener
	go master.acceptMultipleConnections()
	go master.handleFailingWorkers()
	mapOperations = master.schedule(task, "Worker.RunMap", task.InputFilePathChan)
	mergeMapLocal(task, mapOperations)
	reduceFilePathChan = fanReduceFilePath(task.NumReduceJobs)
	reduceOperations = master.schedule(task, "Worker.RunReduce", reduceFilePathChan)
	mergeReduceLocal(reduceOperations)
	log.Println("Closing Remote Workers...")
	for _, worker := range master.workers {
		err = worker.callRemoteWorker("Worker.Done", new(struct{}), new(struct{}))
		if err != nil {
			log.Println("Failed to close Remote Worker. Error:", err)
		}
	}
	log.Println("Done.")
}

func RunWorker(task *Task, hostname string, masterHostname string, numOperations int) {
	var (
		err           error
		worker        *Worker
		rpcServer     *rpc.Server
		listener      net.Listener
		retryDuration time.Duration
	)
	log.Println("Running Worker on:", hostname)
	err = os.Mkdir(REDUCE_PATH, os.ModePerm)
	checkError(err)
	worker = new(Worker)
	worker.hostname = hostname
	worker.masterHostname = masterHostname
	worker.task = task
	worker.done = make(chan bool)
	if numOperations > 0 {
		worker.taskCounter = 0
		worker.numOperations = numOperations
	}
	rpcServer = rpc.NewServer()
	rpcServer.Register(worker)
	worker.rpcServer = rpcServer
	listener, err = net.Listen("tcp", worker.hostname)
	if err != nil {
		log.Panic("Starting RPC listener failed. Error:", err)
	}
	worker.listener = listener
	defer worker.listener.Close()
	retryDuration = time.Duration(2) * time.Second
	for {
		err = worker.register()
		if err == nil {
			break
		}
		log.Printf("Registration failed. Retrying in %v seconds...\n", retryDuration)
		time.Sleep(retryDuration)
	}
	go worker.acceptMultipleConnections()
	<-worker.done
}
