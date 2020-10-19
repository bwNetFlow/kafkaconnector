package kafka

import (
	"context"
	"github.com/Shopify/sarama"
	flow "github.com/bwNetFlow/protobuf/go"
	"github.com/gogo/protobuf/proto"
	"log"
)

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	ready chan bool
	flows chan *flow.FlowMessage
	Close context.CancelFunc
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Create our return channel
	consumer.flows = make(chan *flow.FlowMessage)
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	close(consumer.flows)
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	for message := range claim.Messages() {
		// log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s", string(message.Value), message.Timestamp, message.Topic)
		session.MarkMessage(message, "")
		flowMsg := new(flow.FlowMessage)
		err := proto.Unmarshal(message.Value, flowMsg)
		if err != nil {
			log.Printf("decodeMessages: Received broken message: %v", err)
			continue
		}
		consumer.flows <- flowMsg
	}

	return nil
}
