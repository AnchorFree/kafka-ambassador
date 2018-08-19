package kafka

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/prometheus/client_golang/prometheus"
)

type T struct {
	Producer   *kafka.Producer
	Prometheus *prometheus.Registry
}

func (p *T) Init(kafkaParams *kafka.ConfigMap) error {
	var err error
	p.Producer, err = kafka.NewProducer(kafkaParams)
	go func() {
		for e := range p.Producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				m := ev
				if m.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
				} else {
					/*
						fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
							*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
					*/
				}
			//return
			default:
				fmt.Printf("Ignored event: %s\n", ev)
			}
		}
	}()
	return err
}

func (p *T) Send(topic *string, message []byte) {

	p.Producer.ProduceChannel() <- &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     topic,
			Partition: kafka.PartitionAny},
		Value: message,
	}

}
