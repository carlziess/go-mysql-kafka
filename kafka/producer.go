package kafka

import (
	"github.com/siddontang/go-log/log"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func (c *Client) send(message []byte, topic string, partition int32) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": c.KafkaBrokerServers})
	if err != nil {
		log.Errorf("Failed to create producer: %s\n", err)
		panic(err)
	}
	log.Infof("Created Producer %v\n", p)
	doneChan := make(chan bool)
	go func() {
		defer close(doneChan)
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				m := ev
				if m.TopicPartition.Error != nil {
					log.Errorf("Delivery failed: %v\n", m.TopicPartition.Error)
				} else {
					log.Infof("Delivered message to topic %s [%d] at offset %v\n", *m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
				}
				return
			default:
				log.Errorf("Ignored event: %s\n", ev)
			}
		}
	}()

	p.ProduceChannel() <- &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: partition}, Value: []byte(message)}

	// wait for delivery report goroutine to finish
	_ = <-doneChan

	p.Close()
	return
}
