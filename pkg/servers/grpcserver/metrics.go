package grpcserver

import (
	"github.com/grpc-ecosystem/go-grpc-prometheus"
)

var (
	// Create some standard server metrics.
	grpcMetrics = grpc_prometheus.NewServerMetrics()
)

func (s *Server) RegisterMetrics() {
	// Register standard server metrics and customized metrics to registry.
	s.Prometheus.MustRegister(grpcMetrics)
}
