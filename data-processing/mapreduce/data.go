package mapreduce

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
)

const (
	REDUCE_PATH         = "reduce/"
	RESULT_PATH         = "result"
	OPEN_FILE_MAX_RETRY = 3
)

func checkError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func mergeReduceName(idReduce int) string {
	return fmt.Sprintf("reduce-%v", idReduce)
}

func reduceName(idMap int, idReduce int) string {
	return fmt.Sprintf("reduce-%v-%v", idMap, idReduce)
}

func resultFileName(id int) string {
	return filepath.Join(RESULT_PATH, fmt.Sprintf("result-%v", id))
}

func storeLocal(task *Task, idMapTask int, data []KeyValue) {
	var (
		err         error
		file        *os.File
		fileEncoder *json.Encoder
	)
	for i := 0; i < task.NumReduceJobs; i++ {
		file, err = os.Create(filepath.Join(REDUCE_PATH, reduceName(idMapTask, i)))
		checkError(err)
		fileEncoder = json.NewEncoder(file)
		for _, keyValue := range data {
			if task.Shuffle(task, keyValue.Key) == i {
				err = fileEncoder.Encode(&keyValue)
				checkError(err)
			}
		}
		file.Sync()
		file.Close()
	}
}

func loadLocal(idReduce int) []KeyValue {
	var (
		err         error
		file        *os.File
		fileDecoder *json.Decoder
	)
	file, err = os.Open(filepath.Join(REDUCE_PATH, mergeReduceName(idReduce)))
	checkError(err)
	fileDecoder = json.NewDecoder(file)
	data := make([]KeyValue, 0)
	for {
		var keyValue KeyValue
		err = fileDecoder.Decode(&keyValue)
		if err != nil {
			break
		}
		data = append(data, keyValue)
	}
	file.Close()
	return data
}

func mergeMapLocal(task *Task, mapCounter int) {
	var (
		err              error
		file             *os.File
		fileDecoder      *json.Decoder
		mergeFile        *os.File
		mergeFileEncoder *json.Encoder
	)
	for i := 0; i < task.NumReduceJobs; i++ {
		mergeFile, err = os.Create(filepath.Join(REDUCE_PATH, mergeReduceName(i)))
		checkError(err)
		mergeFileEncoder = json.NewEncoder(mergeFile)
		for j := 0; j < mapCounter; j++ {
			for k := 0; k < OPEN_FILE_MAX_RETRY; k++ {
				file, err = os.Open(filepath.Join(REDUCE_PATH, reduceName(j, i)))
				if err == nil {
					break
				}
				log.Printf("(%v/%v) Failed to open file %v. Retrying in 1 second...", k+1, OPEN_FILE_MAX_RETRY, filepath.Join(REDUCE_PATH, reduceName(j, i)))
				time.Sleep(time.Second)
			}
			checkError(err)
			file, err = os.Open(filepath.Join(REDUCE_PATH, reduceName(j, i)))
			checkError(err)
			fileDecoder = json.NewDecoder(file)
			for {
				var keyValue KeyValue
				err = fileDecoder.Decode(&keyValue)
				if err != nil {
					break
				}
				mergeFileEncoder.Encode(&keyValue)
			}
			file.Sync()
			file.Close()
		}
		mergeFile.Sync()
		mergeFile.Close()
	}
}

func mergeReduceLocal(reduceCounter int) {
	var (
		err              error
		file             *os.File
		fileDecoder      *json.Decoder
		mergeFile        *os.File
		mergeFileEncoder *json.Encoder
	)
	mergeFile, err = os.Create(filepath.Join(RESULT_PATH, "result-final.txt"))
	checkError(err)
	defer mergeFile.Sync()
	defer mergeFile.Close()
	mergeFileEncoder = json.NewEncoder(mergeFile)
	for i := 0; i < reduceCounter; i++ {
		for j := 0; j < OPEN_FILE_MAX_RETRY; j++ {
			file, err = os.Open(resultFileName(i))
			if err == nil {
				break
			}
			log.Printf("(%v/%v) Failed to open file %v. Retrying in 1 second...", j+1, OPEN_FILE_MAX_RETRY, resultFileName(i))
			time.Sleep(time.Second)
		}
		checkError(err)
		fileDecoder = json.NewDecoder(file)
		for {
			var keyValue KeyValue
			err = fileDecoder.Decode(&keyValue)
			if err != nil {
				break
			}
			mergeFileEncoder.Encode(&keyValue)
		}
		file.Sync()
		file.Close()
	}
}

func RemoveContents(dirName string) error {
	dir, err := os.Open(dirName)
	if err != nil {
		return err
	}
	defer dir.Close()
	names, err := dir.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, name := range names {
		err = os.RemoveAll(filepath.Join(dirName, name))
		if err != nil {
			return err
		}
	}
	return nil
}

func fanReduceFilePath(numReduceJobs int) chan string {
	var (
		outputChan chan string
		filePath   string
	)
	outputChan = make(chan string)
	go func() {
		for i := 0; i < numReduceJobs; i++ {
			filePath = filepath.Join(REDUCE_PATH, mergeReduceName(i))
			outputChan <- filePath
		}
		close(outputChan)
	}()
	return outputChan
}
