# bwNetFlow Go Kafka Connector

Example Usage:

```
package main

import (
	"log"

	"github.com/Shopify/sarama"
	kafka "omi-gitlab.e-technik.uni-ulm.de/bwnetflow/kafka/kafkaconnector"
)

var kafkaConn = kafka.Connector{}

func main() {

	// connect to the BelWue Kafka cluster
	broker := "127.0.0.1:9092,[::1]:9092" // TODO: set valid uris
	topic := "flow-messages-enriched"
	consumerGroup := "example-consumer"
	kafkaConn.StartConsumer(broker, topic, consumerGroup, sarama.OffsetNewest)
	defer kafkaConn.Close()

	// receive flows: e.g. count flows & bytes
	var flowCounter, byteCounter uint64
	for {
		flow := <-kafkaConn.ConsumerChannel()
		// process the flow here ...
		flowCounter++
		byteCounter += flow.GetBytes()
		if flowCounter%1000000 == 0 {
			log.Printf("flows / bytes: %d / %d GB\n", flowCounter, byteCounter/1024/1024/1024)
		}
	}

}
```
