package kafka

import (
	"context"
	"fmt"

	"github.com/esequielvirtuoso/go_utils_lib/logger"
	kafka "github.com/segmentio/kafka-go"
)

func NewClient(firstBroker string, secondBroker string) KafkaClientInterface {
	return &kafkaClient{
		firstBrokerAddress:  firstBroker,
		secondBrokerAddress: secondBroker,
	}
}

type KafkaClientInterface interface {
	Produce(string, string, string, context.Context)
	Consume(string, string, context.Context)
}

type kafkaClient struct {
	firstBrokerAddress  string
	secondBrokerAddress string
}

func (c *kafkaClient) Produce(message string, partition string, topic string, ctx context.Context) {
	// intialize the writer with the broker addresses, and the topic
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{c.firstBrokerAddress, c.secondBrokerAddress},
		Topic:   topic,
	})

	// each kafka message has a key and value. The key is used
	// to decide which partition (and consequently, which broker)
	// the message gets published on
	err := writer.WriteMessages(ctx, kafka.Message{
		Key: []byte(partition),
		// create an arbitrary message payload for the value
		Value: []byte(message),
	})
	if err != nil {
		panic("could not write message " + err.Error())
	}

	logger.Info("message successfully written")
}

func (c *kafkaClient) Consume(topic string, groupID string, ctx context.Context) {
	// initialize a new reader with the brokers and topic
	// the groupID identifies the consumer and prevents
	// it from receiving duplicate messages
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{c.firstBrokerAddress, c.secondBrokerAddress},
		Topic:   topic,
		GroupID: groupID,
	})

	for {
		// the `ReadMessage` method blocks until we receive the next event
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			panic("could not read message " + err.Error())
		}
		// after receiving the message, log its value
		logger.Info(fmt.Sprintf("received %s", msg.Value))
	}
}
