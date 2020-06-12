package kafka

import (
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anchorfree/kafka-ambassador/pkg/logger"
	"github.com/anchorfree/kafka-ambassador/pkg/wal"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sony/gobreaker"
)

type I interface {
	GetProducersCount() int
	GetActiveProducerID() string
	GetProducer() *ProducerWrapper
	AddActiveProducer(ProducerI, *kafka.ConfigMap) error
	Init(*kafka.ConfigMap, *prometheus.Registry) error
	ReSend()
	ListTopics() ([]string, error)
	Send(string, []byte)
	QueueIsEmpty() bool
	Shutdown()
}

type Mode int
type Source int

const (
	Fallback Mode = iota
	Always
	Disable
)

const (
	FromWAL Source = iota
	Direct
)

type ProducerWrapper struct {
	Producer ProducerI
	Transit  int64
	ID       string
}

type EventWrapper struct {
	Event    kafka.Event
	Producer *ProducerWrapper
}

type T struct {
	producers        map[string]*ProducerWrapper
	activeProducerID string
	Logger           logger.Logger
	Config           *Config
	wal              wal.I
	mutex            *sync.RWMutex
	producerMutex    *sync.RWMutex
	producerWg       sync.WaitGroup
	resendMutex      *sync.Mutex
	cb               *gobreaker.TwoStepCircuitBreaker
	rateLimiter      <-chan time.Time
	inShutdown       bool
	events           chan EventWrapper
}

type Config struct {
	ResendPeriod           time.Duration
	OldProducerKillTimeout time.Duration
	WalMode                Mode
	AlwaysWalTopics        []string
	DisableWalTopics       []string
	ResendRateLimit        int
	CBTimeout              time.Duration
	CBInterval             time.Duration
	CBMaxFailures          uint32
	CBMaxRequests          uint32
	GetMetadataTimeout     time.Duration
	Wal                    wal.Config `yaml:"wal"` //it's needed to pass the wal config. All the options should be parsed same way using yaml.Unmarshal()
}

func (p *T) GetProducersCount() int {
	return len(p.producers)
}

func (p *T) GetActiveProducerID() string {
	return p.activeProducerID
}

func (p *T) GetProducer() *ProducerWrapper {
	p.producerMutex.RLock()
	defer p.producerMutex.RUnlock()
	return p.producers[p.GetActiveProducerID()]
}

func (p *T) AddActiveProducer(kp ProducerI, kafkaParams *kafka.ConfigMap) error {
	if p.producers == nil {
		p.producers = map[string]*ProducerWrapper{}
	}
	p.producerMutex.Lock()
	previousActiveID := p.activeProducerID
	pid := kp.String()
	p.activeProducerID = pid
	pw := ProducerWrapper{
		Producer: kp,
		ID:       pid,
		Transit:  int64(0),
	}
	p.producers[p.activeProducerID] = &pw
	p.producerWg.Add(1)
	go func(pwLink *ProducerWrapper) {
		p.Logger.Infof("Running events pass routine for the new lead producer")
		for event := range pwLink.Producer.Events() {
			p.events <- EventWrapper{Event: event, Producer: pwLink}
		}
		p.producerWg.Done()
	}(&pw)
	p.producerMutex.Unlock()
	activeProducer.With(prometheus.Labels{
		"producer_id": pid,
	}).Set(1)
	lastProducerStartTime.Set(float64(time.Now().Unix()))
	if KafkaParamsPathExists(kafkaParams, "ssl.certificate.location") {
		certET, err := ParamsCertExpirationTime(kafkaParams, "ssl.certificate.location")
		if err == nil {
			metricCertExpirationTime.Set(float64(certET.Unix()))
		} else {
			metricCertExpirationTime.Set(float64(0))
		}
	}
	if KafkaParamsPathExists(kafkaParams, "ssl.ca.location") {
		caET, err := ParamsCertExpirationTime(kafkaParams, "ssl.ca.location")
		if err == nil {
			metricCaExpirationTime.Set(float64(caET.Unix()))
		} else {
			metricCaExpirationTime.Set(float64(0))
		}
	}
	if len(p.producers) > 1 {
		//shut down former lead producer when it has no messages in transit
		if oldPW, ok := p.producers[previousActiveID]; ok {
			activeProducer.With(prometheus.Labels{
				"producer_id": oldPW.ID,
			}).Set(0)
			go func() {
				firstAttemptTime := time.Now()
				for {
					if oldPW.Transit > 0 && time.Since(firstAttemptTime) < p.Config.OldProducerKillTimeout {
						p.Logger.Infof("Old producer #%d still has %d messages in queue. Trying for %s since %s.", previousActiveID, oldPW.Transit, time.Since(firstAttemptTime), firstAttemptTime)
						time.Sleep(5 * time.Second)
					} else {
						break
					}
				}
				p.Logger.Infof("Closing old producer %s. Messages in queue: %d (time since first attemts: %s, OldProducerKillTimeout: %s)", previousActiveID, oldPW.Transit, time.Since(firstAttemptTime), p.Config.OldProducerKillTimeout)
				oldPW.Producer.Close()
				go func() {
					timeout := 10 * time.Minute
					p.Logger.Debugf("Going to drop metrics for the old producer %s in %s", oldPW.ID, timeout)
					time.Sleep(timeout)
					p.Logger.Infof("Dropping metrics for the old producer: %s", oldPW.ID)
					p.dropProducerMetrics(oldPW.ID)
				}()
				p.producerMutex.Lock()
				if _, stillOk := p.producers[previousActiveID]; stillOk {
					delete(p.producers, previousActiveID)
				}
				p.producerMutex.Unlock()
			}()
		}
	}
	return nil
}

func (p *T) Init(kafkaParams *kafka.ConfigMap, prom *prometheus.Registry) error {
	var err error
	p.mutex = new(sync.RWMutex)
	p.resendMutex = new(sync.Mutex)
	p.producerMutex = new(sync.RWMutex)
	p.Logger.Info("Creating Kafka producer")
	p.events = make(chan EventWrapper)
	for k, v := range *kafkaParams {
		p.Logger.Infof("Kafka param %s: %v", k, v)
	}
	if p.GetProducersCount() == 0 {
		kp, err := kafka.NewProducer(kafkaParams)
		if err != nil {
			p.Logger.Errorf("Could not create producer due to: %v", err)
			return err
		}
		err = p.AddActiveProducer(kp, kafkaParams)
		if err != nil {
			p.Logger.Error("Could not add active producer")
			return err
		}
	}
	if err != nil {
		p.Logger.Errorf("Could not create initial kafka producer due to: %v", err)
		return err
	}
	//Close internal Events channel when all producers get closed
	go func() {
		p.producerWg.Wait()
		close(p.events)
	}()
	cbSettings := gobreaker.Settings{
		Name:          "kafka",
		MaxRequests:   p.Config.CBMaxRequests,
		Timeout:       p.Config.CBTimeout,
		Interval:      p.Config.CBInterval,
		OnStateChange: p.setCBState,
		ReadyToTrip:   p.readyToTrip,
	}
	libint, libstring := kafka.LibraryVersion()
	libVersion.WithLabelValues(libstring).Set(float64(libint))
	p.cb = gobreaker.NewTwoStepCircuitBreaker(cbSettings)
	p.wal, err = wal.New(p.Config.Wal, prom, p.Logger)
	if err != nil {
		p.Logger.Errorf("Could not initialize WAL due to: %v", err)
		return err
	}

	rlPeriod := time.Second / time.Duration(p.Config.ResendRateLimit)
	p.Logger.Debugf("Period for rate limiter is: %s", rlPeriod)
	p.rateLimiter = time.Tick(rlPeriod)
	registerMetrics(prom)
	p.Logger.Info("Starting up kafka events tracker")
	go p.producerEventsHandler()
	if p.Config.ResendPeriod != 0 {
		p.Logger.Infof("Starting up kafka resend process with period %s", p.Config.ResendPeriod.String())
		go p.ReSend()
	}
	// monitor CB state every 10 seconds
	go p.trackCBState(10 * time.Second)
	go p.kafkaStats(30 * time.Second)
	return err
}

func (p *T) ReSend() {
	var recordLimit int64

	for range time.Tick(p.Config.ResendPeriod) {
		if p.inShutdown {
			p.Logger.Warn("we are shutting down, no need to retry at the moment, exiting the loop")
			return
		}
		switch p.cb.State() {
		// default limit is no limit
		case gobreaker.StateOpen:
			p.Logger.Info("CB is open, skipping resend")
			continue
		case gobreaker.StateHalfOpen:
			recordLimit = 1
			p.Logger.Infof("Running resend with limit %d, as CB is half open", recordLimit)
		default:
			recordLimit = 0
			p.Logger.Infof("Running resend with limit %d, as CB is closed", recordLimit)
		}

		p.iterateLimit(recordLimit)
	}
}

func (p *T) iterateLimit(limit int64) {
	p.resendMutex.Lock()
	defer p.resendMutex.Unlock()
	now := time.Now().Unix()
	for r := range p.wal.Iterate(limit) {
		if p.inShutdown {
			return
		}
		rtime, err := wal.GetTime(r)
		if err != nil {
			rtime = time.Now()
		}
		if now-rtime.Unix() > int64(p.Config.ResendPeriod.Seconds()) {
			if p.cb.State() != gobreaker.StateOpen {
				<-p.rateLimiter
				if !p.inShutdown {
					p.produce(r.Topic, r.Payload, FromWAL)
				}
			} else {
				p.Logger.Infof("We have got state change during retry, current state is %v, abort retry", p.cb.State())
				return
			}
		}
	}
}

func (p *T) ListTopics() ([]string, error) {
	var ret []string
	pw := p.GetProducer()
	md, err := pw.Producer.GetMetadata(nil, true, int(p.Config.GetMetadataTimeout.Nanoseconds()/1000000))
	if err != nil {
		return ret, err
	}
	for t, _ := range md.Topics {
		if !strings.HasPrefix(t, "__") {
			ret = append(ret, t)
		}
	}
	return ret, err
}

func (p *T) Send(topic string, message []byte) {
	if p.cb.State() == gobreaker.StateClosed {
		p.produce(topic, message, Direct)
		if (p.Config.WalMode == Always && !p.isDisableWal(topic)) || p.isAlwaysWal(topic) {
			p.Logger.Debugf("Storing message to topic: %s into WAL", topic)
			_ = p.wal.Set(topic, message)
		}
		msgSent.With(prometheus.Labels{"topic": topic}).Inc()
	} else {
		p.Logger.Debugf("Storing message to topic: %s into WAL as CB is not ready", topic)
		_ = p.wal.Set(topic, message)
	}
}

func (p *T) produce(topic string, message []byte, opaque interface{}) {
	p.Logger.Debugf("Sending to topic: [%s] message %s", topic, string(message))
	pw := p.GetProducer()
	pw.Producer.ProduceChannel() <- &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value:  message,
		Opaque: opaque,
	}
	if v, ok := opaque.(Source); ok && v == Direct {
		atomic.AddInt64(&(pw.Transit), 1)
	}
	msgInTransit.With(prometheus.Labels{
		"producer_id": pw.ID,
	}).Add(1)

}

func (p *T) producerEventsHandler() {
	for eventWrap := range p.events {
		switch ev := eventWrap.Event.(type) {
		case *kafka.Message:
			success, err := p.cb.Allow()
			m := ev
			atomic.AddInt64(&(eventWrap.Producer.Transit), -1)
			msgInTransit.With(prometheus.Labels{
				"producer_id": eventWrap.Producer.ID,
			}).Add(-1)
			if m.TopicPartition.Error != nil {
				msgNOK.With(prometheus.Labels{
					"topic": *m.TopicPartition.Topic,
					"error": m.TopicPartition.Error.Error()}).Inc()
				p.Logger.Debugf("could not send message to kafka due to: %s", m.TopicPartition.Error.Error())
				// we store messages which can be retried only
				if canRetry(m.TopicPartition.Error) {
					_ = p.wal.Set(*m.TopicPartition.Topic, m.Value)
				} else {
					// we could put the message into some malformed topic or similar
					crc := wal.Uint32ToBytes(wal.CrcSum(m.Value))
					p.Logger.Infof("Dropped message CRC: %s as we can't retry it due to: %s", string(crc), m.TopicPartition.Error.Error())
					_ = p.wal.Del(crc)
					msgDropped.With(prometheus.Labels{
						"topic": *m.TopicPartition.Topic,
						"error": m.TopicPartition.Error.Error()}).Inc()
					//skip success(false) as non-retryable message should be just filtered out
					break
				}
				if err != nil {
					// We are not allowed to do anything
					break
				}
				success(false)
			} else {
				msgOK.With(prometheus.Labels{"topic": *m.TopicPartition.Topic}).Inc()
				if m.Opaque == FromWAL || p.isAlwaysWal(*m.TopicPartition.Topic) {
					crc := wal.Uint32ToBytes(wal.CrcSum(m.Value))
					p.Logger.Debugf("removing CRC: %s", string(crc))
					_ = p.wal.Del(crc)
				}
				if err != nil {
					// We are not allowed to do anything
					break
				}
				success(true)
			}
		case kafka.Error:
			p.Logger.Warnf("%v", eventWrap.Event)
			if ev.Code() == kafka.ErrAllBrokersDown {
				p.Logger.Warn("All brokers are down, forcing Circuit Breaker open")
				p.cbOpen()
			}
		case *kafka.Stats:
			err := populateRDKafkaMetrics(ev.String())
			if err != nil {
				p.Logger.Infof("Could not populate librdkafka metrics: %v", err)
			}
		default:
			p.Logger.Warnf("Ignored message: %v", eventWrap.Event)
			eventIgnored.Inc()
		}
	}
}
func (p *T) isAlwaysWal(topic string) bool {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	for i := range p.Config.AlwaysWalTopics {
		if p.Config.AlwaysWalTopics[i] == topic {
			return true
		}
	}
	return false
}

func (p *T) isDisableWal(topic string) bool {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	for i, _ := range p.Config.DisableWalTopics {
		if p.Config.DisableWalTopics[i] == topic {
			return true
		}
	}
	return false
}

func (p *T) kafkaStats(period time.Duration) {
	if p.Config.ResendPeriod != 0 {
		ticker := time.NewTicker(p.Config.ResendPeriod)
		for range ticker.C {
			producerQueueLen.Set(float64(p.GetProducer().Producer.Len()))
			metricKafkaEventsQueueLen.Set(float64(len(p.GetProducer().Producer.Events())))
		}
	}
}

func canRetry(err error) bool {
	switch e := err.(kafka.Error); e.Code() {
	// topics are wrong
	case kafka.ErrTopicException, kafka.ErrUnknownTopic:
		return false
	// message is incorrect
	case kafka.ErrMsgSizeTooLarge, kafka.ErrInvalidMsgSize:
		return false
	default:
		return true
	}
}

func (p *T) QueueIsEmpty() bool {
	allEmpty := true
	for pid, pw := range p.producers {
		p.Logger.Infof("Messages in queue %s (leadProducer: %t): %d", pid, pid == p.GetActiveProducerID(), pw.Transit)
		if pw.Transit > 0 {
			allEmpty = false
		}
	}
	return allEmpty
}

func (p *T) Shutdown() {
	p.Logger.Warn("Got shutdown signal, entering lame duck mode")
	p.inShutdown = true
}

func (p *T) CloseWalDB() error {
	return p.wal.Close()
}
