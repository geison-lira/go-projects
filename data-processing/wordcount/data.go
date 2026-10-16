package main

import (
	"data-processing/mapreduce"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"unicode"
)

const (
	MAP_PATH           = "map/"
	RESULT_PATH        = "result/"
	MAP_BUFFER_SIZE    = 10
	REDUCE_BUFFER_SIZE = 10
)

func checkError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func fanInData(numFiles int) chan []byte {
	var (
		err    error
		input  chan []byte
		buffer []byte
	)
	input = make(chan []byte, MAP_BUFFER_SIZE)
	go func() {
		for i := 0; i < numFiles; i++ {
			if buffer, err = os.ReadFile(mapFileName(i)); err != nil {
				close(input)
				log.Fatal(err)
			}
			log.Println("Fanning in file", mapFileName(i))
			input <- buffer
		}
		close(input)
	}()
	return input
}

func fanInFilePath(numFiles int, fileHostname string) chan string {
	var (
		inputChan chan string
		filePath  string
	)
	inputChan = make(chan string)
	go func() {
		for i := 0; i < numFiles; i++ {
			filePath = mapFileName(i)
			inputChan <- filePath
		}
		close(inputChan)
	}()
	return inputChan
}

func fanOutData() (chan []mapreduce.KeyValue, chan bool) {
	var (
		err           error
		file          *os.File
		fileEncoder   *json.Encoder
		reduceCounter int
		output        chan []mapreduce.KeyValue
		done          chan bool
	)
	output = make(chan []mapreduce.KeyValue, REDUCE_BUFFER_SIZE)
	done = make(chan bool)
	go func() {
		for v := range output {
			log.Println("Fanning out file", resultFileName(reduceCounter))
			file, err = os.Create(resultFileName(reduceCounter))
			checkError(err)
			fileEncoder = json.NewEncoder(file)
			for _, value := range v {
				fileEncoder.Encode(value)
			}
			file.Close()
			reduceCounter++
		}
		done <- true
	}()
	return output, done
}

func splitData(fileName string, chunkSize int) (numMapFiles int, err error) {
	var (
		file         *os.File
		tmpFile      *os.File
		chunkBuffer  []byte
		paddedBuffer []byte
		bytesRead    int
		pad          int
	)
	numMapFiles = 0
	if file, err = os.Open(fileName); err != nil {
		return numMapFiles, err
	}
	defer file.Close()
	chunkBuffer = make([]byte, chunkSize)
	paddedBuffer = chunkBuffer
	pad = 0
	for {
		if bytesRead, err = file.Read(paddedBuffer); err != nil {
			if err != io.EOF {
				return numMapFiles, err
			}
		}
		paddedBuffer = chunkBuffer
		bytesRead += pad
		if bytesRead > 0 {
			if bytesRead == chunkSize {
				pad = 0
				for r := rune(paddedBuffer[bytesRead-1-pad]); unicode.IsLetter(r) || unicode.IsNumber(r); r = rune(paddedBuffer[bytesRead-1-pad]) {
					pad++
					if pad == bytesRead {
						pad = 0
						break
					}
				}
			} else {
				pad = chunkSize - bytesRead
			}
			paddedBuffer = chunkBuffer[:chunkSize-pad]
			if tmpFile, err = os.Create(mapFileName(numMapFiles)); err != nil {
				return numMapFiles, err
			}
			numMapFiles++
			if _, err = tmpFile.Write(paddedBuffer); err != nil {
				tmpFile.Close()
				return numMapFiles, err
			}
			tmpFile.Close()
			_ = copy(chunkBuffer, chunkBuffer[chunkSize-pad:])
			paddedBuffer = chunkBuffer[pad:]
		}
		if bytesRead < chunkSize {
			break
		}
	}
	return numMapFiles, nil
}

func mapFileName(id int) string {
	return filepath.Join(MAP_PATH, fmt.Sprintf("map-%v", id))
}

func resultFileName(id int) string {
	return filepath.Join(RESULT_PATH, fmt.Sprintf("result-%v", id))
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
