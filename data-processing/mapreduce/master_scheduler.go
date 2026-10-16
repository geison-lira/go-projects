package mapreduce

import (
	"log"
	"sync"
)

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
