package main

import (
	"example/myhello/greetings"
	"fmt"

	"rsc.io/quote"
)

func main() {
	fmt.Println("Hellow World")
	fmt.Println(greetings.Welcome())
	fmt.Println(quote.Go())
}
