package main

import (
	"flag"
	"sync"

	"github.com/anchorfree/kafka-ambassador/pkg/config"
	"github.com/anchorfree/kafka-ambassador/pkg/logger"
	"github.com/anchorfree/kafka-ambassador/pkg/servers"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	defaults map[string]interface{} = map[string]interface{}{
		"global.log.level":                            "info",
		"global.log.encoding":                         "json",
		"global.log.outputPaths":                      ("stdout"),
		"global.log.errorOutputPaths":                 ("stderr"),
		"global.log.encoderConfig":                    logger.NewEncoderConfig(),
		"server.http.listen":                          ":19092",
		"kafka.compression.codec":                     "gzip",
		"kafka.batch.num.messages":                    100000,
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

	s := new(servers.T)
	s.Wg = new(sync.WaitGroup)
	c := &config.T{
		Filename:  configPathName,
		EnvPrefix: "ka",
	}
	s.Config, err = c.ReadConfig(defaults)
	if err != nil {
		return
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
	kafkaParams, err := config.KafkaParams(s.Config)
	if err != nil {
		return
	}
	s.Producer.Logger = s.Logger
	s.Producer.Init(&kafkaParams, s.Prometheus)
	s.Start()
}
