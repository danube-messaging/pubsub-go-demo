package main

import (
	"flag"
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
		// Define flags specific to producer mode
		serverAddr := flag.String("server-addr", "0.0.0.0:4040", "Address to bind the HTTP server")
		prodConfig := flag.String("prod-config", "", "Producer configuration YAML file")
		danubeAddr := flag.String("danube-addr", "0.0.0.0:6650", "Address of the Danube Broker")

		// Parse flags for producer
		flag.CommandLine.Parse(os.Args[2:])

		// Validate required flags
		if *prodConfig == "" {
			log.Fatalf("Error: Producer config file is required.\nUsage:\n"+
				"  --server-addr (default: %s) : Address to bind the HTTP server\n"+
				"  --prod-config (required)    : Producer configuration YAML file\n"+
				"  --danube-addr (default: %s) : Address of the Danube Broker\n",
				*serverAddr, *danubeAddr)
		}

		startProducer(serverAddr, prodConfig, danubeAddr)

	case "consumer":
		consConfig := flag.String("cons-config", "", "Consumer configuration YAML file")
		danubeAddr := flag.String("danube-addr", "0.0.0.0:6650", "Address of the Danube Broker")

		// Parse flags for consumer
		flag.CommandLine.Parse(os.Args[2:])

		// Validate required flags
		if *consConfig == "" {
			log.Fatalf("Error: Consumer config file is required.\nUsage:\n"+
				"  --cons-config (required)    : Consumer configuration YAML file\n"+
				"  --danube-addr (default: %s) : Address of the Danube Broker\n",
				*danubeAddr)
		}

		startConsumer(consConfig, danubeAddr)

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
