package kafka

import (
	"log"
	"strings"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	flow "omi-gitlab.e-technik.uni-ulm.de/bwnetflow/bwnetflow_api/go"
)

// Connector handles a connection to read bwNetFlow flows from kafka
type Connector struct {
	consumer       *cluster.Consumer
	producer       sarama.AsyncProducer
	consumeChannel chan *flow.FlowMessage
	produceChannel chan *flow.FlowMessage
	// TODO: by having below signatures use FlowMessage, replacing the handlers is quite useless
	consumerHandler func(*cluster.Consumer, chan *flow.FlowMessage)
	producerHandler func(sarama.AsyncProducer, string, chan *flow.FlowMessage)
}

// Connect establishes the connection to kafka
// TODO: should this return the consumeChannel too?
func (connector *Connector) Consume(broker string, topics []string, consumergroup string, offset int64) {
	brokers := strings.Split(broker, ",")
	consConf := cluster.NewConfig()
	// Enable these unconditionally.
	consConf.Consumer.Return.Errors = true
	consConf.Group.Return.Notifications = true
	// The offset only works initially. When reusing a Consumer Group, it's
	// last state will be resumed automatcally (grep MarkOffset)
	consConf.Consumer.Offsets.Initial = offset

	// everything declared and configured, lets go
	var err error
	connector.consumer, err = cluster.NewConsumer(brokers, consumergroup, topics, consConf)
	if err != nil {
		log.Fatalf("Error initializing the Kafka Consumer: %v", err)
	}
	log.Println("Kafka connection established.")

	// start message handling in background
	connector.consumeChannel = make(chan *flow.FlowMessage) // TODO: make buffer sizes configurable?
	if connector.consumerHandler != nil {
		go connector.consumerHandler(connector.consumer, connector.consumeChannel)
	} else {
		go decodeMessages(connector.consumer, connector.consumeChannel)
	}
}

// TODO: should this return the produceChannel too?
func (connector *Connector) Produce(broker string, topic string) {
	brokers := strings.Split(broker, ",")
	prodConf := sarama.NewConfig()
	prodConf.Producer.Return.Successes = false // this would block until we've read the ACK
	// TODO: The setting will cause deadlocks if not read. If the handler
	// is overwritten, this will most likely be forgotten...
	prodConf.Producer.Return.Errors = true

	// everything declared and configured, lets go
	var err error
	connector.producer, err = sarama.NewAsyncProducer(brokers, prodConf)
	if err != nil {
		log.Fatalf("Error initializing the Kafka Producer: %v", err)
	}

	// start message handling in background
	connector.produceChannel = make(chan *flow.FlowMessage) // TODO: make buffer sizes configurable?
	if connector.producerHandler != nil {
		go connector.producerHandler(connector.producer, topic, connector.produceChannel)
	} else {
		go encodeMessages(connector.producer, topic, connector.produceChannel)
	}
}

func (connector *Connector) ReplaceConsumedMessageHandler(replacement func(*cluster.Consumer, chan *flow.FlowMessage)) {
	connector.consumerHandler = replacement
}

func (connector *Connector) ReplaceProducedMessageHandler(replacement func(sarama.AsyncProducer, string, chan *flow.FlowMessage)) {
	connector.producerHandler = replacement
}

// Close closes the connection to kafka
func (connector *Connector) Close() {
	if connector.consumer != nil {
		connector.consumer.Close()
		log.Println("Kafka Consumer connection closed.")
	}
	if connector.producer != nil {
		connector.producer.Close()
		log.Println("Kafka Producer connection closed.")
	}
}

func (connector *Connector) ConsumedMessages() <-chan *flow.FlowMessage {
	return connector.consumeChannel
}

func (connector *Connector) ProducedMessages() chan *flow.FlowMessage {
	return connector.produceChannel
}

func (connector *Connector) Errors() <-chan error {
	// Consumer Errors are relayed directly from the Kafka Cluster.
	// TODO: what happens if this is not read? These would be logged by default, but where to?
	return connector.consumer.Errors()
}

func (connector *Connector) Notifications() <-chan *cluster.Notification {
	// Consumer Notifications are relayed directly from the Kafka Cluster.
	// These include which topics and partitions are read by this instance
	// and are sent on every Rebalancing Event.
	// TODO: what happens if this is not read? Maybe always enable logging for these, they're useful
	return connector.consumer.Notifications()
}

// EnableLogging prints kafka errors and notifications to log
func (connector *Connector) EnableLogging() {
	go func() { // spawn a logger for Errors
		for err := range connector.Errors() {
			log.Printf("Kafka Error: %s\n", err.Error())
		}
	}()
	go func() { // spawn a logger for Notification
		for ntf := range connector.Notifications() {
			log.Printf("Kafka Notification: %+v\n", ntf)
		}
	}()
}
