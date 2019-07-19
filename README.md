[![Build Status](https://travis-ci.org/bwNetFlow/kafkaconnector.svg)](https://travis-ci.org/bwNetFlow/kafkaconnector)
[![Go Report Card](https://goreportcard.com/badge/github.com/bwNetFlow/kafkaconnector)](https://goreportcard.com/report/github.com/bwNetFlow/kafkaconnector)
[![GoDoc](https://godoc.org/github.com/bwNetFlow/kafkaconnector?status.svg)](https://godoc.org/github.com/bwNetFlow/kafkaconnector)

# bwNetFlow Go Kafka Connector

Example Usage:

```go
package main

import (
	"fmt"

	"github.com/Shopify/sarama"
	kafka "github.com/bwNetFlow/kafka/kafkaconnector"
)

var kafkaConn = kafka.Connector{}

func main() {

	fmt.Printf("welcome ... let's go!\n")

	// connect to the BelWue Kafka cluster
	broker := "127.0.0.1:9092,[::1]:9092" // TODO: set valid uris
	topic := []string{"flow-messages-anon"}
	consumerGroup := "anon-golang-example"
	kafkaConn.SetAuthAnon() // optionally: change to SetAuthFromEnv() or SetAuth(user string, pass string)
	kafkaConn.StartConsumer(broker, topic, consumerGroup, sarama.OffsetNewest)
	defer kafkaConn.Close()

	// receive flows: e.g. count flows & bytes
	var flowCounter, byteCounter uint64
	for {
		flow := <-kafkaConn.ConsumerChannel()
		// process the flow here ...
		flowCounter++
		byteCounter += flow.GetBytes()
		fmt.Printf("\rflows: %d, bytes: %d GB", flowCounter, byteCounter/1024/1024/1024)
	}

}
```

## Step by step guide

 * Make sure you have golang installed
 * Make new directory "go-example", and place file a `client.go` with the above listed code snippet in it
 * Inside "go-example" run `go get ./...` to download the dependencies (will be downloaded to the path $GOPATH)
 * Inside "go-example" run `go run client.go` to execute the example from source code
 * If you want to build a binary, run `go build client.go` which produces the "client" binary (run it with `./client`)

## flowfilter

The kafkaconnector contains also an optional flowfilter, which allows to filter for customer ID, IP address (ranges) and peers. Example usage:

```go
var (
	// filtering
	filterCustomerIDs = flag.String("filter.customerid", "", "If defined, only flows for this customer are considered. Leave empty to disable filter. Provide comma separated list to filter for multiple customers.")
	filterIPsv4       = flag.String("filter.IPsv4", "", "If defined, only flows to/from this IP V4 subnet are considered. Leave empty to disable filter. Provide comma separated list to filter for multiple IP subnets.")
	filterIPsv6       = flag.String("filter.IPsv6", "", "If defined, only flows to/from this IP V6 subnet are considered. Leave empty to disable filter. Provide comma separated list to filter for multiple IP subnets.")
	filterPeers       = flag.String("filter.peers", "", "If defined, only flows to/from this peer are considered. Leave empty to disable filter. Provide comma separated list to filter for multiple peers.")
)

func main() {
	// ... establish connection, etc.

	// initialize filters: prepare filter arrays
	flowFilter = flowFilter.NewFlowFilter(*filterCustomerIDs, *filterIPsv4, *filterIPsv6, *filterPeers)

	// handle kafka flow messages in foreground
	for {
		flow := <-kafkaConn.ConsumerChannel()
		if flowFilter.FilterApplies(flow) {
			handleFlow(flow)
		}
	}
}
```
