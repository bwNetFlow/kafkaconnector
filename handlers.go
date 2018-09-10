package kafka

import (
	"log"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/gogo/protobuf/proto"
	flow "omi-gitlab.e-technik.uni-ulm.de/bwnetflow/bwnetflow_api/go"
)

func decodeMessages(consumer *cluster.Consumer, dst chan *flow.FlowMessage) {
	for {
		msg, ok := <-consumer.Messages()
		if !ok {
			log.Println("Message channel closed.")
			// TODO: why don't we do something useful here?
		}
		consumer.MarkOffset(msg, "") // mark message as processed TODO: is this sensible?
		flowMsg := new(flow.FlowMessage)
		err := proto.Unmarshal(msg.Value, flowMsg)
		if err != nil {
			log.Printf("Received broken message. Unmarshalling error: %v", err)
			continue
		}
		dst <- flowMsg // TODO: investigate how unbuffered channels affect this program as a whole
	}
}

func encodeMessages(producer sarama.AsyncProducer, topic string, src chan *flow.FlowMessage) {
	for {
		b, _ := proto.Marshal(<-src)
		select {
		case producer.Input() <- &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.ByteEncoder(b),
		}:
		case err := <-producer.Errors():
			log.Printf("Failed to produce message: %v", err)
		}
	}
}
