package main

import (
	"context"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"log"
	"net"
	"net/http"
	pb "therealbroker/api/proto"
	"therealbroker/internal/broker"
	broker2 "therealbroker/pkg/broker"
	"time"
)

type BrokerServer struct {
	pb.UnimplementedBrokerServer
	m broker2.Broker
}

var activeSubscriberCounter = prometheus.NewCounter(
	prometheus.CounterOpts{
		Name: "active_subscriber_count",
		Help: "No of active subscribers",
	},
)

var methodCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "Method_request_count_dalm_dalm",
		Help: "No of request counts",
	}, []string{"method", "status"})

var publishLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
	Name:    "publish_request_duration_seconds",
	Help:    "Histogram of response time for publish handler in seconds",
	Buckets: []float64{0.005, 0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.1, 0.125, 0.15, 0.175, 0.2, 0.25, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
})

var subscribeLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
	Name:    "subscribe_request_duration_seconds",
	Help:    "Histogram of response time for subscribe handler in seconds",
	Buckets: []float64{0, 25, 50, 100, 125, 150, 175, 200, 225, 250, 275, 300, 325, 350, 375, 400, 425, 450, 475, 500},
})

var fetchLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
	Name:    "fetch_request_duration_seconds",
	Help:    "Histogram of response time for fetch handler in seconds",
	Buckets: []float64{0, 25, 50, 100, 125, 150, 175, 200, 225, 250, 275, 300, 325, 350, 375, 400, 425, 450, 475, 500},
})

func (s *BrokerServer) Publish(ctx context.Context, publishRequest *pb.PublishRequest) (*pb.PublishResponse, error) {
	now := time.Now()
	msg := broker2.Message{Expiration: time.Second * time.Duration(publishRequest.GetExpirationSeconds()), Body: string(publishRequest.GetBody())}
	_, err := s.m.Publish(ctx, publishRequest.Subject, msg)
	if err != nil {
		methodCounter.With(prometheus.Labels{"method": "publish", "status": "failed"}).Inc()
		return &pb.PublishResponse{}, err
	}
	methodCounter.With(prometheus.Labels{"method": "publish", "status": "successful"}).Inc()
	publishResponse := &pb.PublishResponse{Id: int32(-1)}
	publishLatency.Observe(float64(time.Since(now).Milliseconds()))
	return publishResponse, nil
}

func (s *BrokerServer) Subscribe(subscribeRequest *pb.SubscribeRequest, stream pb.Broker_SubscribeServer) error {
	now := time.Now()
	out, err := s.m.Subscribe(context.Background(), subscribeRequest.GetSubject())
	if err != nil {
		methodCounter.With(prometheus.Labels{"method": "subscribe", "status": "failed"}).Inc()
		return err
	}
	activeSubscriberCounter.Inc()
	go func() {
		for {
			message := <-out
			msg := &pb.MessageResponse{Body: message.Body}
			if err := stream.Send(msg); err != nil {
				methodCounter.With(prometheus.Labels{"method": "subscribe", "status": "send_failed"}).Inc()
			}
		}
	}()
	subscribeLatency.Observe(float64(time.Since(now).Milliseconds()))
	methodCounter.With(prometheus.Labels{"method": "subscribe", "status": "failed"}).Inc()
	return nil
}

func (s *BrokerServer) Fetch(ctx context.Context, fetchRequest *pb.FetchRequest) (*pb.MessageResponse, error) {
	now := time.Now()
	msg, err := s.m.Fetch(ctx, fetchRequest.GetSubject(), int(fetchRequest.GetId()))
	if err != nil {
		methodCounter.With(prometheus.Labels{"method": "fetch", "status": "failed"}).Inc()
		return &pb.MessageResponse{}, err
	}
	methodCounter.With(prometheus.Labels{"method": "fetch", "status": "successful"}).Inc()
	messageResponse := &pb.MessageResponse{Body: msg.Body}
	fetchLatency.Observe(float64(time.Since(now).Milliseconds()))
	return messageResponse, nil
}

func main() {
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", 8080))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	prometheus.MustRegister(methodCounter)
	prometheus.MustRegister(activeSubscriberCounter)
	prometheus.MustRegister(publishLatency)
	prometheus.MustRegister(subscribeLatency)
	prometheus.MustRegister(fetchLatency)
	http.Handle("/metrics", promhttp.Handler())
	go http.ListenAndServe(":8090", nil)
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	s := &BrokerServer{m: broker.NewModule()}
	pb.RegisterBrokerServer(grpcServer, s)
	_ = grpcServer.Serve(lis)
}
