package kafka

import (
	"log"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	flow "omi-gitlab.e-technik.uni-ulm.de/bwnetflow/bwnetflow_api/go"
)

// Connector handles a connection to read bwNetFlow flows from kafka
type Connector struct {
	consumer        *cluster.Consumer
	producer        sarama.AsyncProducer
	consumerChannel chan *flow.FlowMessage
	producerChannel chan *flow.FlowMessage
	manualErrors    bool
	channelLength   uint
}

// Enable manual error handling by setting the internal manualErros flag to true.
//
// If this is done before any `.Start*` method was called, no go routines will
// be spawned for logging any messages.
// If this is done after any `.Start*` method was called, spawned go routines
// will die after another message has been received, or after a maximum of 5s.
// After their termination, the custom, application-level error handling will
// be guaranteed to receive all messages. It recommended to set this before
// starting anything.
//
// If this is set, all exposed channels of a running component (ConsumerErrors,
// ConsumerNotifications and ProducerErrors) will have to be read by the
// application, or the Kafka libraries will deadlock.
func (connector *Connector) EnableManualErrorHandling() {
	connector.manualErrors = true
}

// Set the channel length to something >0. Maybe read the source before using it.
func (connector *Connector) SetChannelLength(l uint) {
	connector.channelLength = l
}

// Start a Kafka Consumer with the specified parameters. Its output will be
// available in the channel returned by ConsumerChannel.
func (connector *Connector) StartConsumer(broker string, topics []string, consumergroup string, offset int64) error {
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
		return err
	}
	log.Println("Kafka connection established.")

	// start message handling in background
	connector.consumerChannel = make(chan *flow.FlowMessage, connector.channelLength)
	go decodeMessages(connector.consumer, connector.consumerChannel)
	if !connector.manualErrors {
		log.Println("Spawning a logging goroutine, as the manualErrors option is false.")
		go func() {
			for !connector.manualErrors {
				select {
				case msg := <-connector.ConsumerErrors():
					log.Printf("Kafka Consumer Error: %s\n", msg.Error())
				case msg := <-connector.ConsumerNotifications():
					log.Printf("Kafka Consumer Notification: %+v\n", msg)
				case <-time.After(5 * time.Second):
				}
			}
		}()
	}
	return nil
}

// Start a Kafka Producer with the specified parameters. The channel returned
// by ProducerChannel will be accepting your input.
func (connector *Connector) StartProducer(broker string, topic string) error {
	brokers := strings.Split(broker, ",")
	prodConf := sarama.NewConfig()
	prodConf.Producer.Return.Successes = false // this would block until we've read the ACK
	prodConf.Producer.Return.Errors = true

	// everything declared and configured, lets go
	var err error
	connector.producer, err = sarama.NewAsyncProducer(brokers, prodConf)
	if err != nil {
		return err
	}

	// start message handling in background
	connector.producerChannel = make(chan *flow.FlowMessage, connector.channelLength)
	go encodeMessages(connector.producer, topic, connector.producerChannel)
	if !connector.manualErrors {
		log.Println("Spawning a logging goroutine, as the manualErrors option is false.")
		go func() {
			for !connector.manualErrors {
				select {
				case msg := <-connector.ProducerErrors():
					log.Printf("Kafka Producer Error: %s\n", msg.Error())
				case <-time.After(5 * time.Second):
				}
			}
		}()
	}
	return nil
}

// Close closes the connection to kafka, i.e. Consumer and Producer
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

// Close the Kafka Consumer specifically.
func (connector *Connector) CloseConsumer() {
	if connector.consumer != nil {
		connector.consumer.Close()
		log.Println("Kafka Consumer connection closed.")
	} else {
		log.Println("WARNING: CloseConsumer called, but no Consumer was initialized.")
	}
}

// Close the Kafka Producer specifically.
func (connector *Connector) CloseProducer() {
	if connector.producer != nil {
		connector.producer.Close()
		log.Println("Kafka Producer connection closed.")
	} else {
		log.Println("WARNING: CloseProducer called, but no Producer was initialized.")
	}
}

// Return the channel used for receiving Flows from the Kafka Consumer.
// If this channel closes, it means the upstream Kafka Consumer has closed its
// channel previously of the last decoding step. You can restart the Consumer
// by using .StartConsumer() on the same Connector object.
func (connector *Connector) ConsumerChannel() <-chan *flow.FlowMessage {
	return connector.consumerChannel
}

// Return the channel used for handing over Flows to the Kafka Producer.
// If writing to this channel blocks, check the log.
func (connector *Connector) ProducerChannel() chan *flow.FlowMessage {
	return connector.producerChannel
}

// Consumer Errors relayed directly from the Kafka Cluster.
//
// This will only return a channel after EnableManualErrorHandling has been called.
// IMPORTANT: read EnableManualErrorHandling docs carefully
func (connector *Connector) ConsumerErrors() <-chan error {
	if !connector.manualErrors {
	}
	return connector.consumer.Errors()
}

// Consumer Notifications are relayed directly from the Kafka Cluster.
// These include which topics and partitions are read by this instance
// and are sent on every Rebalancing Event.
//
// This will only return a channel after EnableManualErrorHandling has been called.
// IMPORTANT: read EnableManualErrorHandling docs carefully
func (connector *Connector) ConsumerNotifications() <-chan *cluster.Notification {
	return connector.consumer.Notifications()
}

// Producer Errors are relayed directly from the Kafka Cluster.
//
// This will only return a channel after EnableManualErrorHandling has been called.
// IMPORTANT: read EnableManualErrorHandling docs carefully
func (connector *Connector) ProducerErrors() <-chan *sarama.ProducerError {
	return connector.producer.Errors()
}
