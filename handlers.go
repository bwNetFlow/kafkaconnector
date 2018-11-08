package kafka

import (
	"log"

	"github.com/Shopify/sarama"
	"github.com/gogo/protobuf/proto"
	flow "omi-gitlab.e-technik.uni-ulm.de/bwnetflow/bwnetflow_api/go"
)

// Decode Kafka Messages using our API definition
func decodeMessages(connector *Connector) {
	for {
		msg, ok := <-connector.consumer.Messages()
		if !ok {
			log.Println("Message channel closed.")
			// pass to clients using this lib
			close(connector.consumerChannel)        // content
			close(connector.consumerControlChannel) // monitoring
			break
		}

		// ACK message
		connector.consumer.MarkOffset(msg, "") // mark message as processed

		// decode message content
		flowMsg := new(flow.FlowMessage)
		err := proto.Unmarshal(msg.Value, flowMsg)
		if err != nil {
			log.Printf("Received broken message. Unmarshalling error: %v", err)
			continue
		}

		// decode message metadata
		ctrlMsg := ConsumerControlMessage{
			Partition:      msg.Partition,
			Offset:         msg.Offset,
			Timestamp:      msg.Timestamp,
			BlockTimestamp: msg.BlockTimestamp,
		}

		// send messages to channels
		connector.consumerChannel <- flowMsg
		if connector.hasConsumerControlListener {
			connector.consumerControlChannel <- ctrlMsg
		}
	}
}

// Encode Flows using our API definition
func encodeMessages(producer sarama.AsyncProducer, topic string, src <-chan *flow.FlowMessage) {
	for {
		binary, err := proto.Marshal(<-src)
		if err != nil {
			log.Printf("Could not encode message. Marshalling error: %v", err)
			continue
		}
		producer.Input() <- &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.ByteEncoder(binary),
		}
	}
}
