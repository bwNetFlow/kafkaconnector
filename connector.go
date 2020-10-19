package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os/signal"
	"sync"
	"syscall"
	// "fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/Shopify/sarama"

	flow "github.com/bwNetFlow/protobuf/go"
	"github.com/gogo/protobuf/proto"
)

// Connector handles a connection to read bwNetFlow flows from kafka.
type Connector struct {
	user             string
	pass             string
	consumer         *Consumer
	producer         sarama.AsyncProducer
	consumerChannel  chan *flow.FlowMessage
	producerChannels map[string](chan *flow.FlowMessage)
	authDisable      bool
	tlsDisable       bool
}

// ConsumerControlMessage takes the control params of *sarama.ConsumerMessage
type ConsumerControlMessage struct {
	Partition      int32
	Offset         int64
	Timestamp      time.Time // only set if kafka is version 0.10+, inner message timestamp
	BlockTimestamp time.Time // only set if kafka is version 0.10+, outer (compressed) block timestamp
}

// DisableAuth disables authentification
func (connector *Connector) DisableAuth() {
	connector.authDisable = true
}

// DisableTLS disables ssl/tls connection
func (connector *Connector) DisableTLS() {
	connector.tlsDisable = true
}

// SetAuth explicitly set which login to use in SASL/PLAIN auth via TLS
func (connector *Connector) SetAuth(user string, pass string) {
	connector.user = user
	connector.pass = pass
}

// Check environment to infer which login to use in SASL/PLAIN auth via TLS
// Requires KAFKA_SASL_USER and KAFKA_SASL_PASS to be set for this process.
func (connector *Connector) SetAuthFromEnv() error {
	connector.user = os.Getenv("KAFKA_SASL_USER")
	connector.pass = os.Getenv("KAFKA_SASL_PASS")
	if connector.user == "" || connector.pass == "" {
		return errors.New("Setting Kafka SASL info from Environment was unsuccessful.")
	}
	return nil
}

// Set anonymous credentials as login method.
func (connector *Connector) SetAuthAnon() {
	connector.user = "anon"
	connector.pass = "anon"
}

// Start a Kafka Consumer with the specified parameters. Its output will be
// available in the channel returned by ConsumerChannel.
func (connector *Connector) StartConsumer(brokers string, topics []string, group string, offset int64) error {
	var err error
	config := sarama.NewConfig()
	version, err := sarama.ParseKafkaVersion("2.4.0") // TODO: get somewhere
	if err != nil {
		log.Panicf("Error parsing Kafka version: %v", err)
	}
	config.Version = version
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky
	config.Consumer.Offsets.Initial = offset

	if !connector.tlsDisable {
		// Enable TLS
		rootCAs, err := x509.SystemCertPool()
		if err != nil {
			log.Panicf("TLS Error: %v", err)
		}
		config.Net.TLS.Enable = true
		config.Net.TLS.Config = &tls.Config{RootCAs: rootCAs}
	}

	if !connector.authDisable {
		config.Net.SASL.Enable = true

		if connector.user == "" && connector.pass == "" {
			log.Println("No Auth information is set. Assuming anonymous auth...")
			connector.SetAuthAnon()
		}
		config.Net.SASL.User = connector.user
		config.Net.SASL.Password = connector.pass
	}

	// Enable these unconditionally. TODO: check for analog in new lib
	// consConf.Consumer.Return.Errors = true
	// consConf.Group.Return.Notifications = true

	// everything declared and configured, lets go
	log.Printf("Trying to connect to Kafka %s", brokers)
	connector.consumer = &Consumer{
		ready: make(chan bool),
	}

	ctx, cancel := context.WithCancel(context.Background())
	connector.consumer.Close = cancel
	client, err := sarama.NewConsumerGroup(strings.Split(brokers, ","), group, config)
	if err != nil {
		log.Panicf("Error creating consumer group client: %v", err)
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() { // this is the goroutine doing the work
		defer wg.Done()
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := client.Consume(ctx, topics, connector.consumer); err != nil {
				log.Panicf("Error from consumer: %v", err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
			connector.consumer.ready = make(chan bool)
		}
	}()

	<-connector.consumer.ready // Await till the consumer has been set up
	log.Println("Sarama consumer up and running!...")

	go func() { // this is the goroutine that sticks around waiting for signals
		sigterm := make(chan os.Signal, 1)
		signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
		select {
		case <-ctx.Done():
			log.Println("terminating: context cancelled")
		case <-sigterm:
			log.Println("terminating: via signal")
		}
		cancel()
		wg.Wait()
		if err = client.Close(); err != nil {
			log.Panicf("Error closing client: %v", err)
		}
	}()
	return nil
}

// Start a Kafka Producer with the specified parameters. The channel returned
// by ProducerChannel will be accepting your input.
func (connector *Connector) StartProducer(broker string) error {
	var err error
	brokers := strings.Split(broker, ",")
	config := sarama.NewConfig()

	if !connector.tlsDisable {
		// Enable TLS
		rootCAs, err := x509.SystemCertPool()
		if err != nil {
			log.Println("TLS Error:", err)
			return err
		}
		config.Net.TLS.Enable = true
		config.Net.TLS.Config = &tls.Config{RootCAs: rootCAs}
	}

	if !connector.authDisable {
		config.Net.SASL.Enable = true
		if connector.user == "" && connector.pass == "" {
			log.Println("No Auth information is set. Assuming anonymous auth...")
			connector.SetAuthAnon()
		}
		config.Net.SASL.User = connector.user
		config.Net.SASL.Password = connector.pass
	}

	config.Producer.RequiredAcks = sarama.WaitForLocal       // Only wait for the leader to ack
	config.Producer.Compression = sarama.CompressionSnappy   // Compress messages
	config.Producer.Flush.Frequency = 500 * time.Millisecond // Flush batches every 500ms

	config.Producer.Return.Successes = false // this would block until we've read the ACK
	config.Producer.Return.Errors = true

	connector.producerChannels = make(map[string](chan *flow.FlowMessage))
	// everything declared and configured, lets go
	connector.producer, err = sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		return err
	}
	log.Println("Kafka Producer TLS connection established.")
	return nil
}

// Return the channel used for receiving Flows from the Kafka Consumer.
// If this channel closes, it means the upstream Kafka Consumer has closed its
// channel previously of the last decoding step. You can restart the Consumer
// by using .StartConsumer() on the same Connector object.
func (connector *Connector) ConsumerChannel() <-chan *flow.FlowMessage {
	return connector.consumer.flows
}

// Return the channel used for handing over Flows to the Kafka Producer.
// If writing to this channel blocks, check the log.
func (connector *Connector) ProducerChannel(topic string) chan *flow.FlowMessage {
	if channel, initialized := connector.producerChannels[topic]; initialized {
		return channel
	}
	connector.producerChannels[topic] = make(chan *flow.FlowMessage)
	go func() {
		for {
			message, more := <-connector.producerChannels[topic]
			binary, err := proto.Marshal(message)
			if err != nil {
				log.Printf("producer: Could not encode message to topic %s with error '%v'", topic, err)
				continue
			}
			connector.producer.Input() <- &sarama.ProducerMessage{
				Topic: topic,
				Value: sarama.ByteEncoder(binary),
			}
			if !more {
				log.Printf("producer: terminating topic %s, channel has closed", topic)
				return
			}
		}
	}()
	return connector.producerChannels[topic]
}
