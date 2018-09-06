package kafka

import (
	"log"

	cluster "github.com/bsm/sarama-cluster"
	"github.com/gogo/protobuf/proto"
	flow "omi-gitlab.e-technik.uni-ulm.de/bwnetflow/bwnetflow_api/go"
)

var flowMsg = &flow.FlowMessage{}

func handleMessages(consumer *cluster.Consumer, dst chan *flow.FlowMessage) {
	for {
		msg, ok := <-consumer.Messages()
		if !ok {
			log.Println("Message channel closed.")
		}
		consumer.MarkOffset(msg, "") // mark message as processed
		err := proto.Unmarshal(msg.Value, flowMsg)
		if err != nil {
			log.Printf("Received broken message. Unmarshalling error: %v", err)
			continue
		}
		dst <- flowMsg
	}
}
