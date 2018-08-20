package grpcserver

import (
	"io"
	"net"

	"github.com/anchorfree/kafka-ambassador/pkg/server"
	pb "github.com/anchorfree/kafka-ambassador/pkg/servers/grpcserver/pb"
	"google.golang.org/grpc"
)

type Server server.T

func (s *Server) Start(configPath string) {
	var grpcSrv *grpc.Server
	c := s.Config.Sub(configPath)
	addr := c.GetString("listen")
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		s.Logger.Fatal(err.Error())
	}

	if s.Config.GetBool(configPath + ".monitoring.enable") {
		s.Logger.Info("Monitoring is enabled, applying GRPC interceptors")
		// Create a gRPC Server with gRPC interceptor.
		grpcSrv = grpc.NewServer(
			grpc.StreamInterceptor(grpcMetrics.StreamServerInterceptor()),
			grpc.UnaryInterceptor(grpcMetrics.UnaryServerInterceptor()),
			grpc.MaxMsgSize(s.Config.GetInt(configPath+".max.request.size")),
		)
		// Initialize all metrics.
		s.RegisterMetrics()
		grpcMetrics.InitializeMetrics(grpcSrv)
		// Histograms can be expensive on Prometheus servers.
		if s.Config.GetBool(configPath + ".monitoring.histogram.enable") {
			grpcMetrics.EnableHandlingTimeHistogram()
		}
	} else {
		s.Logger.Info("Monitoring is NOT enabled, enable it if you would like to see prometheus metrics")
		grpcSrv = grpc.NewServer(
			grpc.MaxMsgSize(s.Config.GetInt(configPath + ".max.request.size")),
		)

	}
	pb.RegisterKafkaAmbassadorServer(grpcSrv, s)

	s.Logger.Infof("Listening for GRPC requests on %s", addr)

	go func() {
		if err = grpcSrv.Serve(lis); err != nil {
			s.Logger.Fatalf("failed to serve: %v", err)
		}
	}()
	s.Wg.Add(1)
}

func (s *Server) Produce(stream pb.KafkaAmbassador_ProduceServer) error {
	var res *pb.ProdRs
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}

		s.Producer.Send(&req.Topic, req.Message)
		res = &pb.ProdRs{StreamOffset: req.StreamOffset}
		err = stream.Send(res)
		if err != nil {
			s.Logger.Errorf("Could not stream (GRPC) to the client: %s", err)
			return err
		}
	}
}

func (s *Server) Stop() {

}
