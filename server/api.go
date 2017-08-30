package server

import (
	"log"
	"net"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

const (
	port = ":50000"
)

// API is the API server
type API struct {
	server *grpc.Server
}

// NewAPI creates a new API server instance
func NewAPI() *API {
	return &API{
		server: grpc.NewServer(),
	}
}

// AddJob add a new job to the workflow
func (s *API) AddJob(ctx context.Context, j *Job) (*Job, error) {
	return j, nil
}

// Start starts the API server
func (s *API) Start() {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to lisen: %v", err)
	}
	RegisterJobServiceServer(s.server, s)
	if err := s.server.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

// Stop stops the API service
func (s *API) Stop() {
	s.server.GracefulStop()
}
