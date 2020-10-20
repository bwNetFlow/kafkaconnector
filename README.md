# bwNetFlow Go Kafka Connector

This is a opinionated implementation of a common Connector module for all of
our official components and optionally for users of our platform that intend to
write client applications in Go. It provides an abstraction for plain Sarama
and has support for consuming topics as well as producing to multiple topics,
all while converting any message according to our
[protobuf definition](https://github.com/bwNetFlow/protobuf) for Flow messages
(which is based on [goflow](https://github.com/cloudflare/goflow)'s
definition).

[![Build Status](https://travis-ci.org/bwNetFlow/kafkaconnector.svg)](https://travis-ci.org/bwNetFlow/kafkaconnector)
[![Go Report Card](https://goreportcard.com/badge/github.com/bwNetFlow/kafkaconnector)](https://goreportcard.com/report/github.com/bwNetFlow/kafkaconnector)
[![GoDoc](https://godoc.org/github.com/bwNetFlow/kafkaconnector?status.svg)](https://godoc.org/github.com/bwNetFlow/kafkaconnector)

## Example Usage in Consumer-only mode:

```go
package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/Shopify/sarama"
	kafka "github.com/bwNetFlow/kafkaconnector"
)

var kafkaConn = kafka.Connector{}

func main() {
	fmt.Printf("welcome... let's go!\n")

	// prepare all variables
	broker := "127.0.0.1:9092,[::1]:9092" // TODO: set valid uris
	topic := []string{"flow-messages-anon"}
	consumerGroup := "anon-golang-example"
	kafkaConn.SetAuthAnon() // optionally: change to SetAuthFromEnv() or SetAuth(user string, pass string)

	kafkaConn.EnablePrometheus(":2112") // optionally open up for monitoring

	// ensure a clean exit
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigchan
		fmt.Println("Signal caught, exiting...")
		kafkaConn.Close()
	}()

	// receive flows
	kafkaConn.StartConsumer(broker, topic, consumerGroup, sarama.OffsetNewest)
	var flowCounter, byteCounter uint64
	for flow := range kafkaConn.ConsumerChannel() {
		// process the flow here ...
		flowCounter++
		byteCounter += flow.GetBytes()
		fmt.Printf("\rflows: %d, bytes: %d GB", flowCounter, byteCounter/1024/1024/1024)
	}
}
```

## Example Usage in Consumer/Producer mode:
Check out
[processor_splitter](https://github.com/bwNetFlow/processor_splitter), it is
very simple and consumes a single topic while producing to multiple target
topics.
