package main

import (
	"fmt"

	"github.com/geison-lira/go-projects/myhello/greetings"
	"rsc.io/quote"
)

func main() {
	fmt.Println("Hello World.")
	fmt.Println(greetings.Welcome())
	fmt.Println(quote.Go())
}
