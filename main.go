package main

import (
	"log"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage: danube-demo [producer|consumer] [flags]")
	}

	mode := os.Args[1]
	switch mode {
	case "producer":
		startProducer()
	case "consumer":
		startConsumer()
	default:
		log.Fatalf("Unknown mode: %s", mode)
	}
}
