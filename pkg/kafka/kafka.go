package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/prometheus/client_golang/prometheus"
)

type T struct {
	Producer *kafka.Producer
}

func (p *T) Init(kafkaParams *kafka.ConfigMap, prom *prometheus.Registry) error {
	var err error
	p.Producer, err = kafka.NewProducer(kafkaParams)
	if err != nil {
		return err
	}
	registerMetrics(prom)
	go func() {
		for e := range p.Producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				m := ev
				if m.TopicPartition.Error != nil {
					msgNOK.With(prometheus.Labels{
						"topic": *m.TopicPartition.Topic,
						"error": m.TopicPartition.Error.Error()}).Inc()
					// TODO: add message dump of m.Value into log, if WAL is not enabled
				} else {
					msgOK.With(prometheus.Labels{"topic": *m.TopicPartition.Topic}).Inc()
				}
			default:
				// fmt.Printf("Ignored event: %s\n", ev)
			}
		}
	}()
	return err
}

func (p *T) Send(topic *string, message []byte) {

	// TODO: add message dump of message into log, if WAL is enabled
	p.Producer.ProduceChannel() <- &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     topic,
			Partition: kafka.PartitionAny},
		Value: message,
	}
	msgSent.With(prometheus.Labels{"topic": *topic}).Inc()

}
