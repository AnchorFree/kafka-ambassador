package kafka

import (
	"github.com/anchorfree/kafka-ambassador/pkg/logger"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/prometheus/client_golang/prometheus"
)

type T struct {
	Producer *kafka.Producer
	Logger   logger.Logger
}

func (p *T) Init(kafkaParams *kafka.ConfigMap, prom *prometheus.Registry) error {
	var err error
	p.Logger.Info("Creating Kafka producer")
	p.Producer, err = kafka.NewProducer(kafkaParams)
	if err != nil {
		p.Logger.Errorf("Could not create kafka producer due to: %v", err)
		return err
	}
	registerMetrics(prom)
	p.Logger.Info("Starting up kafka events tracker")
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
					p.Logger.Infof("could not send message to kafka due to: %s", m.TopicPartition.Error.Error())
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
