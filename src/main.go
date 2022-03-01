package main

import (
	"context"

	"github.com/esequielvirtuoso/go_kafka_sample/src/internal/infrastructure/kafka"
	env "github.com/esequielvirtuoso/go_utils_lib/envs"
)

const (
	envFirstBroker          = "BROKER_1"
	envDefaultFirstBrocker  = "localhost:29092"
	envSecondBroker         = "BROKER_2"
	envDefaultSecondBrocker = "localhost:39092"
)

func main() {
	// create a new context
	ctx := context.Background()

	kafkaClient := kafka.NewClient(getFirstBroker(), getSecondBroker())

	go kafkaClient.Produce("Kafka producer first message", "1", "topic_test", ctx)
	kafkaClient.Consume("topic_test", "my-group", ctx)
}

func getFirstBroker() string {
	return env.GetString(envFirstBroker, envDefaultFirstBrocker)
}

func getSecondBroker() string {
	return env.GetString(envSecondBroker, envDefaultSecondBrocker)
}
