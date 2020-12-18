package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
)

const (
	kafkaConn = "localhost:9092"
	topic     = "sarama"
)

func main() {
	consumer := initConsumer()

	var defaultPatition int32 = 0
	var defaultOffset int64 = 0
	consumeMessage(consumer, topic, defaultPatition, defaultOffset)
}

func initConsumer() sarama.Consumer {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer([]string{kafkaConn}, config)
	if err != nil {
		panic(err)
	}

	return consumer
}

func consumeMessage(consumer sarama.Consumer, topics string, partition int32, offset int64) {
	partitionData, err := consumer.ConsumePartition(topics, partition, offset)
	if err != nil {
		panic(err)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	msgCount := 0

	doneCh := make(chan struct{})

	go func() {
		for {
			select {
			case err := <-partitionData.Errors():
				fmt.Println(err)
			case msg := <-partitionData.Messages():
				msgCount++
				fmt.Println("Received messages, with key :", string(msg.Key), " value : ", string(msg.Value))
			case <-signals:
				fmt.Println("Interupt is detected")
				doneCh <- struct{}{}
				return
			}
		}
	}()

	<-doneCh
	fmt.Println("Processed", msgCount, "messages")
}
