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
	// ErrEmptyURL is returned when the url argument is empty
	ErrEmptyURL = errors.Error("url cannot be empty")
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
		url          string
		topic        string
		requestCount uint64
		err          error
	)

	if url, topic, requestCount, err = parseArgs(); err != nil {
		handleError(err)
	}

	cfg := makeConfig(url)
	topicPartition = kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny}

	var p *kafka.Producer
	if p, err = kafka.NewProducer(&cfg); err != nil {
		handleError(err)
	}
	defer p.Close()

	go scanEvents(p)

	q := queue.New(8, 256)

	var wg sync.WaitGroup
	wg.Add(int(requestCount))
	for i := uint64(0); i < requestCount; i++ {
		val := rand.Float64()
		str := strconv.FormatFloat(val, 'f', 4, 64)
		q.New(func() {
			if err := p.Produce(newMessage(str), nil); err != nil {
				out.Errorf("error producing message: %v", err)
				return
			}

			wg.Done()
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

	successCount.Add(1)
}

func makeConfig(url string) kafka.ConfigMap {
	endpoint := fmt.Sprintf("%s:%d", url, port)
	return kafka.ConfigMap{"bootstrap.servers": endpoint}
}

func parseArgs() (url, topic string, requestCount uint64, err error) {
	if url = flag.Arg(0); len(url) == 0 {
		err = ErrEmptyURL
		return
	}

	if topic = flag.Arg(0); len(topic) == 0 {
		err = ErrEmptyTopic
		return
	}

	flag.Uint64Var(&requestCount, "requestCount", 100000, "Amount of requests to send during test")
	flag.Parse()
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
