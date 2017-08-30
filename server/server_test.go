package server

import (
	"fmt"
	"testing"
	"time"
)

func TestServerStartStop(t *testing.T) {
	t.Parallel()

	config := Config{
		"broker": "localhost:9092",
		"name":   fmt.Sprintf("TestServerStartStop-%d", time.Now().Unix()),
	}
	server := NewServer(config)
	if server == nil {
		t.FailNow()
	}

	server.Start()
	time.Sleep(3 * time.Second)
	server.Stop()
}
