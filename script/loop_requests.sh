#!/bin/bash

# Loop until user cancels with Ctrl+C
while true
do
  # Invoke the string producer
  hey -n 10 -c 5 -m PUT -H "Content-Type: text/plain" -d "Hello, Danube!" http://localhost:4040/pubsub/string
  # Wait for 2 seconds
  sleep 2
  
  # Invoke the number producer
  hey -n 10 -c 5 -m PUT -H "Content-Type: text/plain" -d "3333" http://localhost:4040/pubsub/number
  # Wait for 2 seconds
  sleep 2

  # Invoke the json producer
  hey -n 10 -c 5 -m PUT -H "Content-Type: application/json" -d "{\"field1\": \"example_danube_json\", \"field2\": 123}" http://localhost:4040/pubsub/json
  # Wait for 2 seconds
  sleep 2
done