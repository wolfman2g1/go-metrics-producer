package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"

	"github.com/hatchify/atoms"
	"github.com/hatchify/errors"
	"github.com/hatchify/queue"
	"github.com/hatchify/scribe"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

const (
	// ErrEmptyHost is returned when the host argument is empty
	ErrEmptyHost = errors.Error("host cannot be empty")
	// ErrEmptyTopic is returned when the topic argument is empty
	ErrEmptyTopic = errors.Error("topic cannot be empty")
)

const (
	port uint16 = 9092
)

var (
	out = scribe.New("Kafka Tester")

	successCount atoms.Uint64
	errorCount   atoms.Uint64

	topicPartition kafka.TopicPartition
)

func main() {
	var (
		host         string
		topic        string
		requestCount uint64
		concurrency  uint64
		err          error
	)

	if host, topic, requestCount, concurrency, err = parseArgs(); err != nil {
		handleError(err)
	}

	out.Notificationf("Testing with host \"%s\" and topic of \"%s\"", host, topic)

	cfg := makeConfig(host)
	topicPartition = kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny}

	var p *kafka.Producer
	if p, err = kafka.NewProducer(&cfg); err != nil {
		handleError(err)
	}
	defer p.Close()

	go scanEvents(p)

	q := queue.New(int(concurrency), 256)

	var wg sync.WaitGroup
	wg.Add(int(requestCount))
	for i := uint64(0); i < requestCount; i++ {
		val := rand.Float64()
		str := strconv.FormatFloat(val, 'f', 4, 64)
		q.New(func() {
			defer wg.Done()
			if err := p.Produce(newMessage(str), nil); err != nil {
				out.Errorf("error producing message: %v", err)
				return
			}
		})
	}

	// Wait for all messages to be pushes to producer queue
	wg.Wait()

	// Wait for producer queue to clear
	p.Flush(15 * 1000)

	out.Successf("Testing complete:\nSuccess: %d\nErrors: %d\n", successCount.Load(), errorCount.Load())
}

func handleError(err error) {
	out.Error(err.Error())
	os.Exit(1)
}

func scanEvents(p *kafka.Producer) {
	// Scan for Producer events
	for evt := range p.Events() {
		// Handle event
		handleEvent(evt)
	}
}

func handleEvent(evt kafka.Event) {
	msg, ok := evt.(*kafka.Message)
	if !ok {
		return
	}

	if msg.TopicPartition.Error != nil {
		out.Errorf("Error encountered during delivery: %v", msg.TopicPartition.Error)
		errorCount.Add(1)
		return
	}

	successful := successCount.Add(1)
	if successful%10000 == 0 {
		out.Notificationf("Completed %d requests", successful)
	}
}

func makeConfig(host string) kafka.ConfigMap {
	endpoint := fmt.Sprintf("%s:%d", host, port)
	return kafka.ConfigMap{"bootstrap.servers": endpoint}
}

func parseArgs() (host, topic string, requestCount, concurrency uint64, err error) {
	flag.StringVar(&host, "host", "", "Host to test")
	flag.StringVar(&topic, "topic", "", "Topic to send in tests")
	flag.Uint64Var(&requestCount, "requestCount", 100000, "Amount of requests to send during test")
	flag.Uint64Var(&requestCount, "concurrency", 8, "The number of requests to send concurrently")
	flag.Parse()

	if len(host) == 0 {
		err = ErrEmptyHost
		return
	}

	if len(topic) == 0 {
		err = ErrEmptyTopic
		return
	}

	return
}

func newMessage(value string) *kafka.Message {
	msg := makeMessage(value)
	return &msg
}

func makeMessage(value string) (msg kafka.Message) {
	msg.Value = []byte(value)
	msg.TopicPartition = topicPartition
	return
}
