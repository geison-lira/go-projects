package main

import (
	//"fmt" //(for testing only)
	"data-processing/mapreduce"
	"hash/fnv"

	//"strconv" //()
	"strings"
	"unicode"
)

func mapFunc(input []byte) (result []mapreduce.KeyValue) {
	var (
		text          string
		delimiterFunc func(c rune) bool
		words         []string
	)
	text = string(input)
	delimiterFunc = func(c rune) bool {
		return !unicode.IsLetter(c) && !unicode.IsNumber(c)
	}
	words = strings.FieldsFunc(text, delimiterFunc)
	//fmt.Printf("%v\n", words) //(for testing only)
	result = make([]mapreduce.KeyValue, 0)
	for _, word := range words {
		//Change this code, add <word,"1"> in result
	}
	//fmt.Printf("%v\n", result) //(for testing only)
	return result
}

func reduceFunc(input []mapreduce.KeyValue) (result []mapreduce.KeyValue) {
	//Change this code
	//Maybe an auxiliary strucutre: var mapAux map[string]int = make(map[string]int)
	//Need a loop in input: for _,item := range input { ... }
	//Can check if a map have a key as following: _, ok := mapAux[item.Key]
	//ok (true) means that the map has this key and !ok means that the map does not have this key
	//May need to convert KeyValue pairs in the input variable that Reduce will receive from string to int before using in operations as following: package strconv: func Atoi(s string) (int, error)
	//Result needs KeyValue pairs with strings, convert int to string as following: package strconv: func Itoa(i int) string
	//fmt.Printf("%v\n", result) //(for testing only)
	return result
}

func shuffleFunc(task *mapreduce.Task, key string) (reduceJob int) {
	hash := fnv.New32a()
	hash.Write([]byte(key))
	return int(hash.Sum32() % uint32(task.NumReduceJobs))
}
