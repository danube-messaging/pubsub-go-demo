package main

import (
	"log"
	"os"

	"gopkg.in/yaml.v2"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage: pubsub-go-demo [producer|consumer] [flags]")
	}

	mode := os.Args[1]
	switch mode {
	case "producer":
		startProducer()
	case "consumer":
		startConsumer()
	default:
		log.Fatalf("Unknown mode: %s. .\nUsage:\n"+
			"pubsub-go-demo [producer|consumer] [flags]\n", mode)
	}
}

func parseConfig(filename string, config interface{}) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Failed to open config file: %v", err)
	}
	defer file.Close()

	decoder := yaml.NewDecoder(file)
	if err := decoder.Decode(config); err != nil {
		log.Fatalf("Failed to parse config file: %v", err)
	}
}
