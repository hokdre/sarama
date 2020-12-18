package main

import (
	"bufio"
	"fmt"
	"log"
	"os"

	"github.com/Shopify/sarama"
)

const (
	kafkaConn = "localhost:9092"
	topic     = "sarama"
)

func main() {
	//create producer
	producer, err := initProducer()
	if err != nil {
		fmt.Println("Error producer : ", err.Error())
	}

	//read command line input
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Println("Enter msg : ")
		msg, _ := reader.ReadString('\n')

		publish(msg, producer)
	}
}

func initProducer() (sarama.SyncProducer, error) {
	//setup sarama loger
	sarama.Logger = log.New(os.Stdout, "", log.Ltime)

	//producer config
	config := sarama.NewConfig()
	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true

	//sync producer
	prd, err := sarama.NewSyncProducer([]string{kafkaConn}, config)
	return prd, err
}

func publish(message string, producer sarama.SyncProducer) {
	//publish sync
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}
	p, o, err := producer.SendMessage(msg)
	if err != nil {
		fmt.Println("error publish : ", err.Error())
	}

	fmt.Println("Partition : ", p)
	fmt.Println("Offset : ", o)
}
