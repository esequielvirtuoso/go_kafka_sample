package main

import (
	"context"

	"github.com/esequielvirtuoso/go_kafka_sample/src/internal/infrastructure/kafka"
	env "github.com/esequielvirtuoso/go_utils_lib/envs"
)

const (
	envFirstBroker         = "BROKER_1"
	envDefaultFirstBroker  = "localhost:29092"
	envSecondBroker        = "BROKER_2"
	envDefaultSecondBroker = "localhost:39092"
)

func main() {
	// create a new context
	ctx := context.Background()

	kafkaClient := kafka.NewClient(getFirstBroker(), getSecondBroker())

	go produceLoop(ctx, kafkaClient)

	consumeLoop(ctx, kafkaClient)
}

func produceLoop(ctx context.Context, client kafka.KafkaClientInterface) {
	for {
		client.Produce("Kafka producer first message", "1", "topic_test", ctx)
	}
}

func consumeLoop(ctx context.Context, client kafka.KafkaClientInterface) {
	for {
		client.Consume("topic_test", "consumer_group_test", ctx)
	}
}

func getFirstBroker() string {
	return env.GetString(envFirstBroker, envDefaultFirstBroker)
}

func getSecondBroker() string {
	return env.GetString(envSecondBroker, envDefaultSecondBroker)
}
