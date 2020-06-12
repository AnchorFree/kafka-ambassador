package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/anchorfree/kafka-ambassador/pkg/config"
	"github.com/anchorfree/kafka-ambassador/pkg/kafka"
	"github.com/anchorfree/kafka-ambassador/pkg/logger"
	"github.com/anchorfree/kafka-ambassador/pkg/servers"
	ckg "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/yaml.v2"
)

//it's needed to pass wal config.
//We need to refactor whole config chain to be parsed the same way - using yaml.Unmarshal
type Config struct {
	Producer kafka.Config `yaml:"producer"`
}

var (
	defaults = map[string]interface{}{
		"global.log.level":            "info",
		"global.log.encoding":         "json",
		"global.log.outputPaths":      ("stdout"),
		"global.log.errorOutputPaths": ("stderr"),
		"global.log.encoderConfig":    logger.NewEncoderConfig(),
		"server.http.listen":          ":19092",
		// Please take a look at:
		// https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
		// for more configuration parameters
		"kafka.compression.codec":                     "gzip",
		"kafka.batch.num.messages":                    100000,
		"kafka.socket.timeout.ms":                     10000, // mark connection as stalled
		"kafka.message.timeout.ms":                    60000, // try to deliver message with retries
		"kafka.max.in.flight.requests.per.connection": 20,
		"server.grpc.max.request.size":                4 * 1024 * 1024,
		"server.grpc.monitoring.histogram.enable":     true,
		"server.grpc.monitoring.enable":               true,
	}
)

func main() {
	var err error
	var configPathName string
	flag.StringVar(&configPathName, "config", "", "Configuration file to load")
	flag.Parse()

	// We need to shut down gracefully when the user hits Ctrl-C.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGUSR1, syscall.SIGTERM, syscall.SIGHUP)

	s := new(servers.T)
	c := &config.T{
		Filename:  configPathName,
		EnvPrefix: "ka",
	}
	s.Config, err = c.ReadConfig(defaults)
	if err != nil {
		panic(fmt.Sprintf("Could not read config file: %v", err))
	}
	// logs
	cfg := logger.NewLogConfig(s.Config.Sub("global.log"))
	logger, err := cfg.Build()
	if err != nil {
		panic(err)
	}
	s.Logger = logger.Sugar()
	// metrics
	s.Prometheus = prometheus.NewRegistry()

	// servers
	kafkaParams, err := kafka.Viper2Config(s.Config)
	if err != nil {
		return
	}
	//separate config read for wal. This is to be refactored
	data, err := ioutil.ReadFile(filepath.Clean(configPathName))
	if err != nil {
		panic("could not read config file")
	}
	var anotherConfig Config
	if err := yaml.Unmarshal(data, &anotherConfig); err != nil {
		s.Logger.Fatalf("error: %v", err)
	}
	producer := &kafka.T{}
	producer.Logger = s.Logger
	producer.Config = kafka.ProducerConfig(s.Config)
	producer.Config.Wal = anotherConfig.Producer.Wal
	err = producer.Init(&kafkaParams, s.Prometheus)
	defer producer.CloseWalDB()
	if err != nil {
		s.Logger.Fatal("Could not initialize producer")
	}

	s.Producer = producer
	s.Start()
	for {
		signal := <-sig
		switch signal {
		case syscall.SIGHUP:
			s.Logger.Info("Got SIGHUP: setting up new Kafka producer")
			kp, err := ckg.NewProducer(&kafkaParams)
			if err != nil {
				s.Logger.Errorf("ERROR. Could not create producer on SIGHUP due to: %v", err)
			} else {
				_ = s.Producer.AddActiveProducer(kp, &kafkaParams)
			}
		case syscall.SIGTERM, syscall.SIGINT:
			s.Stop()
			s.Producer.Shutdown()
			for {
				if !s.Producer.QueueIsEmpty() {
					s.Logger.Info("We still have messages in queue, waiting")
					time.Sleep(5 * time.Second)
				} else {
					s.Logger.Info("Queue is empty, shut down properly")
					s.Producer.GetProducer().Producer.Close()
					return
				}
			}
		}
	}
}
