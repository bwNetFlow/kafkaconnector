package kafka

import (
	"log"
	"strings"

	cluster "github.com/bsm/sarama-cluster"
	flow "omi-gitlab.e-technik.uni-ulm.de/bwnetflow/bwnetflow_api/go"
)

// Connector handles a connection to read bwNetFlow flows from kafka
type Connector struct {
	consumer    *cluster.Consumer
	flowChannel chan *flow.FlowMessage
}

// Connect establishes the connection to kafka
func (connector *Connector) Connect(broker string, topic string, consumergroup string, offset int64) {
	brokers := strings.Split(broker, ",")
	consConf := cluster.NewConfig()
	consConf.Consumer.Return.Errors = true
	consConf.Consumer.Offsets.Initial = offset
	consConf.Group.Return.Notifications = true
	topics := []string{topic}
	var err error
	connector.consumer, err = cluster.NewConsumer(brokers, consumergroup, topics, consConf)
	if err != nil {
		log.Fatalf("Error initializing the Kafka Consumer: %v", err)
	}
	log.Println("Kafka connection established.")

	// start message handling in background
	connector.flowChannel = make(chan *flow.FlowMessage)
	go handleMessages(connector.consumer, connector.flowChannel)
}

// Close closes the connection to kafka
func (connector *Connector) Close() {
	connector.consumer.Close()
	log.Println("Kafka connection closed.")
}

// Messages provides bwNetFlow messages as channel
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
		for err := range connector.Errors() {
			log.Printf("Kafka Error: %s\n", err.Error())
		}
	}()

	// handle kafka notifications
	go func() {
		for ntf := range connector.Notifications() {
			log.Printf("Kafka Notification: %+v\n", ntf)
		}
	}()
}
