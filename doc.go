/*
bwNetFlow Go Kafka Connector

To use the bwNetFlow Go Kafka Connector create a new connector:

	var kafkaConn = kafka.Connector{}

Before you connect to kafka, make sure to set brokers, username, etc:

	broker := "127.0.0.1:9092,[::1]:9092"
	topic := []string{"flow-messages-anon"}
	consumerGroup := "anon-golang-example"
	kafkaConn.SetAuthAnon() // optionally: change to SetAuthFromEnv() or SetAuth(user string, pass string)
	defer kafkaConn.Close()

To read flows as a consumer:

	var flowCounter, byteCounter uint64
	for {
		flow := <-kafkaConn.ConsumerChannel()
		// process the flow here ...
	}

And finally start the consumer to connect to the kafka cluster:

	kafkaConn.StartConsumer(broker, topic, consumerGroup, sarama.OffsetNewest)
*/

package kafka
