package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/danrusei/danube-go"
)

type ProducerConfig struct {
	Producers []struct {
		Name       string `yaml:"name"`
		Topic      string `yaml:"topic"`
		Schema     string `yaml:"schema"`
		JsonSchema string `yaml:"json_schema,omitempty"` // Optional, but required if Schema is "json"
	} `yaml:"producers"`
}

func startProducer() {
	serverAddr := flag.String("server-addr", "0.0.0.0:4040", "Address to bind the HTTP server")
	prodConfig := flag.String("prod-config", "", "Producer configuration YAML file")
	danubeAddr := flag.String("danube-addr", "0.0.0.0:6500", "Address of the Danube Broker")
	flag.Parse()

	// Validate required flags
	if *prodConfig == "" {
		log.Fatalf("Error: Producer config file is required.\nUsage:\n"+
			"  --server-addr (default: %s) : Address to bind the HTTP server\n"+
			"  --prod-config (required)    : Producer configuration YAML file\n"+
			"  --danube-addr (default: %s) : Address of the Danube Broker\n",
			*serverAddr, *danubeAddr)
	}

	// Parse producer config
	var config ProducerConfig
	parseConfig(*prodConfig, &config)

	// Initialize producers based on config
	producers := initializeProducers(config, danubeAddr)

	// Start HTTP server
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /pubsub/json", createHandler(producers, "json"))
	mux.HandleFunc("PUT /pubsub/string", createHandler(producers, "string"))
	mux.HandleFunc("PUT /pubsub/number", createHandler(producers, "number"))

	log.Printf("Starting producer HTTP server on %s", *serverAddr)
	log.Fatal(http.ListenAndServe(*serverAddr, mux))
}

func initializeProducers(config ProducerConfig, danubeAddr *string) map[string]*danube.Producer {
	producers := make(map[string]*danube.Producer)
	client := danube.NewClient().ServiceURL(*danubeAddr).Build()

	for _, p := range config.Producers {
		ctx := context.Background()
		producerBuilder := client.NewProducer(ctx).
			WithName(p.Name).
			WithTopic(p.Topic)

		switch p.Schema {
		case "json":
			// Ensure the user has provided a jsonSchema
			if p.JsonSchema == "" {
				log.Fatalf("JSON schema is required for producer %s with schema type 'json'", p.Name)
			}
			producerBuilder.WithSchema("json_schema", danube.SchemaType_JSON, p.JsonSchema)

		case "string":
			producerBuilder.WithSchema("string_schema", danube.SchemaType_STRING, "")

		case "number":
			producerBuilder.WithSchema("number_schema", danube.SchemaType_INT64, "")

		default:
			log.Fatalf("Unknown schema type %s for producer %s", p.Schema, p.Name)
		}

		producer, err := producerBuilder.Build()
		if err != nil {
			log.Fatalf("Failed to initialize producer: %v", err)
		}

		if err := producer.Create(ctx); err != nil {
			log.Fatalf("Failed to create producer: %v", err)
		}

		producers[p.Schema] = producer
		log.Printf("Producer %s created for topic %s", p.Name, p.Topic)
	}

	return producers
}

func createHandler(producers map[string]*danube.Producer, schemaType string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := context.Background()
		producer, ok := producers[schemaType]
		if !ok {
			http.Error(w, fmt.Sprintf("No producer found for schema type: %s", schemaType), http.StatusBadRequest)
			return
		}

		payload := make([]byte, r.ContentLength)
		_, err := r.Body.Read(payload)
		if err != nil {
			http.Error(w, "Failed to read payload", http.StatusInternalServerError)
			return
		}

		messageID, err := producer.Send(ctx, payload, nil)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to send message: %v", err), http.StatusInternalServerError)
			return
		}

		log.Printf("Message sent by producer %s with ID %v", schemaType, messageID)
		fmt.Fprintln(w, "Message sent")
	}
}
