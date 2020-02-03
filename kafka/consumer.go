package kafka

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/siddontang/go-log/log"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func (c *Client) Consumer(groupId string, topic string, partition int32) {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	client, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":     c.KafkaBrokerServers,
		"broker.address.family": "v4",
		"group.id":              groupId,
		"session.timeout.ms":    6000,
		"auto.offset.reset":     "earliest"})
	if err != nil {
		log.Errorf("Failed to create consumer: %s\n", err)
	}
	err = client.Subscribe(topic, nil)
	if err != nil {
		log.Errorf("Failed to subscribtion topic:%s\n", err)
	}
	partions, errs := client.Assignment()
	if errs != nil {
		log.Errorf("Failed to getting partions")
	}
	err = client.Assign(partions)
	if err != nil {
		log.Errorf("Failed to assign topic's partition:%s\n", err)
	}
	run := true
	for run == true {
		select {
		case sig := <-sigchan:
			log.Errorf("Caught signal %v: terminating\n", sig)
		default:
			ev := client.Poll(100)
			if ev == nil {
				continue
			}
			switch e := ev.(type) {
			case *kafka.Message:
				if e.Headers != nil {
					log.Errorf("%% Headers: %v\n", e.Headers)
					run = false
				}
				c.event.MsgEventDispatcher(string(e.Value))
			case kafka.Error:
				if e.Code() == kafka.ErrAllBrokersDown {
					log.Errorf("%% Error: %v: %v\n", e.Code(), e)
					run = false
				}
			default:
				log.Infof("Ignored %v\n", e)
			}
		}
	}
	log.Infof("Closing consumer\n")
	client.Close()
}
