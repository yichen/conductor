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
}

// NewServer creates a new server instance
func NewServer(cfg Config) *Server {
	store := NewStore(cfg["name"])
	if err := store.Open(); err != nil {
		fmt.Printf("Failed to open store. Error: %s", err.Error())
		return nil
	}

	wal := NewWal(cfg["name"], cfg["broker"], store)

	ctx := Context{
		store: store,
		wal:   wal,
	}

	fmt.Printf("Store for %s", cfg["name"])

	return &Server{
		config:  cfg,
		context: &ctx,
	}
}

// Start starts the server
func (s *Server) Start() {
	s.context.wal.Start()
}

// Stop shuts down the server
func (s *Server) Stop() {
	if s.context.store != nil {
		s.context.store.Close()
	}

	if s.context.wal != nil {
		s.context.wal.Stop()
	}
}
