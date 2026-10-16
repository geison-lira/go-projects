package main

import (
	"data-processing/mapreduce"
	"flag"
	"log"
	"os"
	"strconv"
)

var (
	mode          = flag.String("mode", "distributed", "Run mode: distributed or sequential")
	nodeType      = flag.String("type", "worker", "Node type: master or worker")
	reduceJobs    = flag.Int("reducejobs", 5, "Number of reduce jobs that should be run")
	file          = flag.String("file", "files/pg1342.txt", "File to use as input")
	chunkSize     = flag.Int("chunksize", 100*1024, "Size of data chunks that should be passed to map jobs(in bytes)")
	addr          = flag.String("addr", "localhost", "IP address to listen on")
	port          = flag.Int("port", 5000, "TCP port to listen on")
	master        = flag.String("master", "localhost:5000", "Master address")
	numOperations = flag.Int("fail", 0, "Number of operations to run before failure")
)

func main() {
	var (
		err      error
		task     *mapreduce.Task
		numFiles int
		hostname string
	)
	flag.Parse()
	err = os.Mkdir(MAP_PATH, os.ModePerm)
	checkError(err)
	err = os.Mkdir(RESULT_PATH, os.ModePerm)
	checkError(err)
	task = &mapreduce.Task{
		Map:           mapFunc,
		Shuffle:       shuffleFunc,
		Reduce:        reduceFunc,
		NumReduceJobs: *reduceJobs,
	}
	log.Println("Running in", *mode, "mode.")
	switch *mode {
	case "sequential":
		var (
			waitForIt chan bool
			fanIn     chan []byte
			fanOut    chan []mapreduce.KeyValue
		)
		err = RemoveContents(MAP_PATH)
		checkError(err)
		err = RemoveContents(RESULT_PATH)
		checkError(err)
		numFiles, err = splitData(*file, *chunkSize)
		checkError(err)
		fanIn = fanInData(numFiles)
		fanOut, waitForIt = fanOutData()
		task.InputChan = fanIn
		task.OutputChan = fanOut
		mapreduce.RunSequential(task)
		<-waitForIt
	case "distributed":
		switch *nodeType {
		case "master":
			var (
				fanIn chan string
			)
			log.Println("NodeType:", *nodeType)
			log.Println("Reduce Jobs:", *reduceJobs)
			log.Println("Address:", *addr)
			log.Println("Port:", *port)
			log.Println("File:", *file)
			log.Println("Chunk Size:", *chunkSize)
			err = RemoveContents(MAP_PATH)
			checkError(err)
			err = RemoveContents(RESULT_PATH)
			checkError(err)
			hostname = *addr + ":" + strconv.Itoa(*port)
			numFiles, err = splitData(*file, *chunkSize)
			checkError(err)
			fanIn = fanInFilePath(numFiles, hostname)
			task.InputFilePathChan = fanIn
			mapreduce.RunMaster(task, hostname)
		case "worker":
			log.Println("NodeType:", *nodeType)
			log.Println("Address:", *addr)
			log.Println("Port:", *port)
			log.Println("Master:", *master)
			if *numOperations > 0 {
				log.Println("Induced failure")
				log.Printf("After %v operations\n", *numOperations)
			}
			hostname = *addr + ":" + strconv.Itoa(*port)
			mapreduce.RunWorker(task, hostname, *master, *numOperations)
		}
	}
}
