package main

import (
	"context"
	"flag"
	"log"
	"strings"

	"github.com/danrusei/danube-go"
)

type ConsumerConfig struct {
	Consumers []struct {
		Name             string `yaml:"name"`
		Topic            string `yaml:"topic"`
		SubscriptionName string `yaml:"subscription_name"`
		SubscriptionType string `yaml:"subscription_type"`
	} `yaml:"consumers"`
}

func startConsumer() {
	consConfig := flag.String("cons-config", "", "Consumer configuration YAML file")
	danubeAddr := flag.String("danube-addr", "0.0.0.0:6500", "Address of the Danube Broker")
	flag.Parse()

	// Validate required flags
	if *consConfig == "" {
		log.Fatalf("Error: Consumer config file is required.\nUsage:\n"+
			"  --cons-config (required)    : Consumer configuration YAML file\n"+
			"  --danube-addr (default: %s) : Address of the Danube Broker\n",
			*danubeAddr)
	}

	// Parse consumer config
	var config ConsumerConfig
	parseConfig(*consConfig, &config)

	// Start consumers based on config
	initializeConsumers(config, danubeAddr)
}

func initializeConsumers(config ConsumerConfig, danubeAddr *string) {
	client := danube.NewClient().ServiceURL(*danubeAddr).Build()

	for _, c := range config.Consumers {
		ctx := context.Background()
		subType := getSubscriptionType(c.SubscriptionType)

		consumer, err := client.NewConsumer(ctx).
			WithConsumerName(c.Name).
			WithTopic(c.Topic).
			WithSubscription(c.SubscriptionName).
			WithSubscriptionType(subType).
			Build()
		if err != nil {
			log.Fatalf("Failed to initialize consumer: %v", err)
		}

		if err := consumer.Subscribe(ctx); err != nil {
			log.Fatalf("Failed to subscribe: %v", err)
		}

		log.Printf("Consumer %s created for topic %s", c.Name, c.Topic)

		// Start consuming messages
		go consumeMessages(consumer, c.Name)
	}

	// Block forever to keep consumers running
	select {}
}

func consumeMessages(consumer *danube.Consumer, consumerName string) {
	ctx := context.Background()
	stream, err := consumer.Receive(ctx)
	if err != nil {
		log.Fatalf("Failed to receive messages: %v", err)
	}

	for msg := range stream {
		log.Printf("Consumer %s received message: %s", consumerName, string(msg.GetPayload()))
	}
}

func getSubscriptionType(subTypeStr string) danube.SubType {
	switch strings.ToLower(subTypeStr) {
	case "exclusive":
		return danube.Exclusive
	case "shared":
		return danube.Shared
	case "failover":
		return danube.FailOver
	default:
		log.Fatalf("Unknown subscription type: %s", subTypeStr)
		return danube.Exclusive // Default to exclusive
	}
}
