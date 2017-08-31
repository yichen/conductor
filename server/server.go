package server

import (
	"fmt"
)

// Config maintains the server configuration
type Config map[string]string

// Server represents a server instance
type Server struct {
	config  Config
	context *Context
}

// Context contains business logic context of the server
type Context struct {
	store *Store
	wal   *Wal
	api   *API
}

// NewServer creates a new server instance
func NewServer(cfg Config) *Server {
	store := NewStore(cfg["name"])
	if err := store.Open(); err != nil {
		fmt.Printf("Failed to open store. Error: %s", err.Error())
		return nil
	}

	wal := NewWal(cfg["name"], cfg["broker"], store)
	api := NewAPI()

	ctx := Context{
		store: store,
		wal:   wal,
		api:   api,
	}

	fmt.Printf("Store for %s", cfg["name"])

	return &Server{
		config:  cfg,
		context: &ctx,
	}
}

// Start starts the server
func (s *Server) Start() {
	fmt.Println("Starting server...")
	s.context.wal.Start()
	s.context.api.Start()
}

// Stop shuts down the server
func (s *Server) Stop() {
	fmt.Println("Stopping server...")

	if s.context.store != nil {
		s.context.store.Close()
	}

	if s.context.wal != nil {
		s.context.wal.Stop()
	}

	if s.context.api != nil {
		s.context.api.Stop()
	}
}
