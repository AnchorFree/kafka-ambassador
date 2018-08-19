package monitoring

import (
	"log"
	"net/http"

	"github.com/anchorfree/kafka-ambassador/pkg/server"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Server server.T

func (s *Server) Start(configPath string) {

	monitServer := &http.Server{
		Handler: promhttp.HandlerFor(s.Prometheus, promhttp.HandlerOpts{}),
		Addr:    s.Config.GetString(configPath + ".listen"),
	}

	// Start your http server for prometheus.
	go func() {
		if err := monitServer.ListenAndServe(); err != nil {
			log.Fatal("Unable to start a http server.")
		}
	}()

	s.Wg.Add(1)
}

func Stop() {

}
