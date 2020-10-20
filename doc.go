/*
bwNetFlow Go Kafka Connector

To use the bwNetFlow Go Kafka Connector create a new connector:

	var kafkaConn = kafka.Connector{}

Before you connect to kafka, make sure to set any options:

	broker := "127.0.0.1:9092,[::1]:9092"
	topic := []string{"flow-messages-anon"}
	consumerGroup := "anon-golang-example"

	// kafkaConn.SetAuthFromEnv()
	// kafkaConn.SetAuth(user string, pass string)
	kafkaConn.SetAuthAnon()
	// kafkaConn.DisableAuth()

To read flows as a consumer:

	kafkaConn.StartConsumer(broker, topic, consumerGroup, sarama.OffsetNewest)
	for flow := range kafkaConn.ConsumerChannel() {
		// process the flow here ...
	}

Be sure to call kafkaConn.Close() when you're done.
*/

package kafka
