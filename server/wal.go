package server

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Wal is the Write-Ahead-Log in RocksDB, persisted from Kafka
type Wal struct {
	name       string
	brokers    string
	store      *Store
	consumer   *kafka.Consumer
	sigchan    chan os.Signal
	shutdownWG sync.WaitGroup
}

// NewWal creates a new WAL instance
func NewWal(name string, brokers string, store *Store) *Wal {
	return &Wal{
		name:    name,
		brokers: brokers,
		store:   store,
		sigchan: make(chan os.Signal, 1),
	}
}

// Start starts the WAL service.
func (w *Wal) Start() {
	signal.Notify(w.sigchan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Printf("creating Kafka consumer for %s, broker: %s", w.name, w.brokers)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":               w.brokers,
		"group.id":                        w.name,
		"session.timeout.ms":              6000,
		"go.events.channel.enable":        true,
		"go.application.rebalance.enable": true,
		"default.topic.config":            kafka.ConfigMap{"auto.offset.reset": "earliest"},
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consume: %s\n", err)
		os.Exit(1)
	}

	w.consumer = c

	fmt.Printf("Created Consumer %v\n", c)

	err = c.SubscribeTopics([]string{w.name}, nil)

	w.shutdownWG.Add(1)
	go w.runWal()
}

func (w *Wal) runWal() {

	run := true

	for run == true {
		select {
		case sig := <-w.sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false

		case ev := <-w.consumer.Events():
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				fmt.Fprintf(os.Stderr, "%% %v\n", e)
				w.consumer.Assign(e.Partitions)

			case kafka.RevokedPartitions:
				fmt.Fprintf(os.Stderr, "%% %v\n", e)
				w.consumer.Unassign()

			case *kafka.Message:
				fmt.Printf("%% Message on %s:\n%s\n",
					e.TopicPartition, string(e.Value))

				// store the data in RocksDB, ordered by Kafka message timestamp
				key := fmt.Sprintf("%s:t:%d", e.Key, e.Timestamp.UnixNano())
				w.store.AppendWAL([]byte(key), e.Value)

			case kafka.Error:
				fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
				run = false
			}
		}
	}

	w.shutdownWG.Done()
}

// Stop stops the WAL service
func (w *Wal) Stop() {
	fmt.Println("stopping WAL service")
	w.sigchan <- syscall.SIGTERM
	w.shutdownWG.Wait()

	if w.consumer != nil {
		w.consumer.Close()
	}

	fmt.Println("WAL service stopped.")
}
