package servers

import (
	"github.com/anchorfree/kafka-ambassador/pkg/server"
	"github.com/anchorfree/kafka-ambassador/pkg/servers/grpcserver"
	"github.com/anchorfree/kafka-ambassador/pkg/servers/httpserver"
	"github.com/anchorfree/kafka-ambassador/pkg/servers/monitoring"
)

const (
	grpcPath       = "server.grpc"
	httpPath       = "server.http"
	monitoringPath = "server.monitoring"
)

type T struct {
	server.T
	Servers []server.T
}

func (s *T) Start() {
	if s.Config.IsSet(monitoringPath + ".listen") {
		monitSrv := &monitoring.Server{
			Producer:   s.Producer,
			Config:     s.Config,
			Prometheus: s.Prometheus,
			Wg:         s.Wg,
		}
		monitSrv.Start(monitoringPath)
	}
	if s.Config.IsSet(httpPath + ".listen") {
		httpSrv := &httpserver.Server{
			Producer:   s.Producer,
			Config:     s.Config,
			Prometheus: s.Prometheus,
			Wg:         s.Wg,
		}
		httpSrv.Start(httpPath)
		// append(s.Servers, httpSrv)
	}

	if s.Config.IsSet(grpcPath + ".listen") {
		grpcSrv := &grpcserver.Server{
			Producer:   s.Producer,
			Config:     s.Config,
			Prometheus: s.Prometheus,
			Wg:         s.Wg,
		}
		grpcSrv.Start(grpcPath)
		// append(s.Servers, grpcSrv)
	}
	s.Wg.Wait()
}

func (s *T) Stop() {
	for i, _ := range s.Servers {
		s.Servers[i].Stop()
	}
}
