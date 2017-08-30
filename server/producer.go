package server

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/golang/protobuf/proto"
)

// Producer can send jobs to the engine
type Producer struct {
	name    string
	brokers string
	doneC   chan bool

	producer *kafka.Producer
}

func newProducer(name string, brokers string) *Producer {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": brokers})
	if err != nil {
		return nil
	}

	producer := &Producer{
		name:     name,
		brokers:  brokers,
		producer: p,
		doneC:    make(chan bool),
	}

	go func() {
		defer close(producer.doneC)

		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				m := ev
				if m.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
				} else {
					fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
						*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
				}
				return

			default:
				fmt.Printf("Ignored event: %s\n", ev)
			}
		}
	}()

	return producer
}

// Produce sends a job to the workflow engine
func (p *Producer) Produce(job *Job) error {

	data, err := proto.Marshal(job)
	if err != nil {
		return err
	}

	p.producer.ProduceChannel() <- &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.name, Partition: kafka.PartitionAny},
		Value:          data,
	}

	return nil
}

func (p *Producer) Close() {
	p.producer.Close()
}
