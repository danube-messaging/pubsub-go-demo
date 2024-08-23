package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"

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

func startProducer(serverAddr *string, prodConfig *string, danubeAddr *string) {
	// Parse producer config
	var config ProducerConfig
	parseConfig(*prodConfig, &config)

	// Initialize producers based on config
	producers := initializeProducers(config, danubeAddr)

	// Start HTTP server
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /pubsub/json", createJsonHandler(producers["json"]))
	mux.HandleFunc("PUT /pubsub/string", createStringHandler(producers["string"]))
	mux.HandleFunc("PUT /pubsub/number", createNumberHandler(producers["number"]))

	log.Printf("Starting producer HTTP server on %s", *serverAddr)
	log.Printf("Registered HTTP handlers:\n" +
		"  PUT /pubsub/json   -> Accept JSON format messages\n" +
		"  PUT /pubsub/string -> Accept string format messages\n" +
		"  PUT /pubsub/number -> Accept number format messages\n")

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

func createJsonHandler(producer *danube.Producer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		if r.Header.Get("Content-Type") != "application/json" {
			http.Error(w, "Content-Type must be application/json", http.StatusUnsupportedMediaType)
			return
		}

		ctx := context.Background()

		payload, err := io.ReadAll(r.Body)
		if err != nil || len(payload) == 0 {
			http.Error(w, "Failed to read payload or payload is empty", http.StatusBadRequest)
			return
		}

		// Validate the JSON format
		var jsonData map[string]interface{}
		if err := json.Unmarshal(payload, &jsonData); err != nil {
			http.Error(w, "Invalid JSON format", http.StatusBadRequest)
			return
		}

		// Send the message using the producer
		messageID, err := producer.Send(ctx, payload, nil)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to send message: %v", err), http.StatusInternalServerError)
			return
		}

		log.Printf("JSON Message sent with ID %v", messageID)
		fmt.Fprintln(w, "Message sent")
	}
}

func createStringHandler(producer *danube.Producer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		ctx := context.Background()

		payload, err := io.ReadAll(r.Body)
		if err != nil || len(payload) == 0 {
			http.Error(w, "Failed to read payload or payload is empty", http.StatusBadRequest)
			return
		}

		// Send the message using the producer
		messageID, err := producer.Send(ctx, payload, nil)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to send message: %v", err), http.StatusInternalServerError)
			return
		}

		log.Printf("String Message sent with ID %v", messageID)
		fmt.Fprintln(w, "Message sent")
	}
}

func createNumberHandler(producer *danube.Producer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		ctx := context.Background()

		payload, err := io.ReadAll(r.Body)
		if err != nil || len(payload) == 0 {
			http.Error(w, "Failed to read payload or payload is empty", http.StatusBadRequest)
			return
		}

		// Validate that the payload is a number (e.g., integer)
		numberStr := string(payload)
		if _, err := strconv.ParseInt(numberStr, 10, 64); err != nil {
			http.Error(w, "Payload must be a valid number", http.StatusBadRequest)
			return
		}

		// Send the message using the producer
		messageID, err := producer.Send(ctx, payload, nil)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to send message: %v", err), http.StatusInternalServerError)
			return
		}

		log.Printf("Number Message sent with ID %v", messageID)
		fmt.Fprintln(w, "Message sent")
	}
}
