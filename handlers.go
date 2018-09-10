package kafka

import (
	"log"

	cluster "github.com/bsm/sarama-cluster"
	"github.com/gogo/protobuf/proto"
	flow "omi-gitlab.e-technik.uni-ulm.de/bwnetflow/bwnetflow_api/go"
)

var flowMsg *flow.FlowMessage

// TODO: this method should be named "decode consumed to channel"
func handleMessages(consumer *cluster.Consumer, dst chan *flow.FlowMessage) {
	for {
		msg, ok := <-consumer.Messages()
		if !ok {
			log.Println("Message channel closed.")
			// TODO: why don't we do something useful here?
		}
		consumer.MarkOffset(msg, "")    // mark message as processed TODO: is this sensible?
		flowMsg = new(flow.FlowMessage) // TODO: does this need to be global considering producing?
		err := proto.Unmarshal(msg.Value, flowMsg)
		if err != nil {
			log.Printf("Received broken message. Unmarshalling error: %v", err)
			continue
		}
		dst <- flowMsg // TODO: investigate how unbuffered channels affect this program as a whole
	}
}
