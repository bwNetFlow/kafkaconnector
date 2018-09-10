package kafka

import (
	"log"
	"strings"

	cluster "github.com/bsm/sarama-cluster"
	flow "omi-gitlab.e-technik.uni-ulm.de/bwnetflow/bwnetflow_api/go"
)

// Connector handles a connection to read bwNetFlow flows from kafka
type Connector struct {
	// TODO: expand this with producer and another channel
	consumer    *cluster.Consumer
	flowChannel chan *flow.FlowMessage
}

// Connect establishes the connection to kafka
// TODO: rename this method to reflect consumer focus
func (connector *Connector) Connect(broker string, topic string, consumergroup string, offset int64) {
	brokers := strings.Split(broker, ",")
	consConf := cluster.NewConfig()
	consConf.Consumer.Return.Errors = true
	consConf.Consumer.Offsets.Initial = offset // TODO: make clear that this means initial, else it will be inferred using the consumer group
	consConf.Group.Return.Notifications = true
	topics := []string{topic} // TODO: allow listening to multiple topics, but merge them
	var err error             // TODO: but why?
	connector.consumer, err = cluster.NewConsumer(brokers, consumergroup, topics, consConf)
	if err != nil {
		log.Fatalf("Error initializing the Kafka Consumer: %v", err)
	}
	log.Println("Kafka connection established.")

	// start message handling in background
	connector.flowChannel = make(chan *flow.FlowMessage)
	// TODO: make handleMessages a parameter, which is the current one by default
	// this would allow overwriting the handling function... also, rename
	// the current handler to reflect what it does
	go handleMessages(connector.consumer, connector.flowChannel)
}

// Close closes the connection to kafka
// TODO: close should close consumer and producer, if applicable... provide seperate ones too
func (connector *Connector) Close() {
	connector.consumer.Close()
	log.Println("Kafka connection closed.")
}

// Messages provides bwNetFlow messages as channel
// TODO: rename to reflect reading from consumer, add a production channel too
func (connector *Connector) Messages() <-chan *flow.FlowMessage {
	return connector.flowChannel
}

// Errors provides bwNetFlow Errors as channel
func (connector *Connector) Errors() <-chan error {
	// pass through errors from consumer
	return connector.consumer.Errors()
}

// Notifications provides bwNetFlow Notifications as channel
func (connector *Connector) Notifications() <-chan *cluster.Notification {
	// pass through notifications from consumer
	return connector.consumer.Notifications()
}

// EnableLogging prints kafka errors and notifications to log
func (connector *Connector) EnableLogging() {
	// handle kafka errors
	go func() {
		for err := range connector.Errors() { // TODO: is this range without index proper?
			log.Printf("Kafka Error: %s\n", err.Error())
		}
	}()

	// handle kafka notifications
	go func() {
		for ntf := range connector.Notifications() {
			log.Printf("Kafka Notification: %+v\n", ntf) // TODO: investigate why this uses %+v and not %s
		}
	}()
}
