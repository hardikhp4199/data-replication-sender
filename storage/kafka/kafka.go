package kafka

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-co-op/gocron"

	"datareplication_sender/config"
)

var servers string = config.GetString("Kafka.Host")
var autoCommit bool = config.GetBool("Kafka.AutoCommit")
var terminationSignalWatchInterval int = config.GetInt("Kafka.TerminationSignalWatchInterval_Second")

const registerProducerFailedRetry int = 1
const registerConsumerFailedRetry int = 1
const consumeFailedRetry int = 5
const systemErrorSignal string = "sys_error"
const consumePollTimeoutMS int = 100

var producer *kafka.Producer
var consumers = make(map[string](map[string]KafkaConsumer))

type KafkaConsumer struct {
	Consumer   *kafka.Consumer
	EOFReached bool
}

type KafkaResult struct {
	Topic    string
	Messages []Message
}

type Message struct {
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
}

var sigchan chan os.Signal = make(chan os.Signal, 1)

func init() {
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	s := gocron.NewScheduler(time.UTC)
	s.Every(terminationSignalWatchInterval).Seconds().Do(monitorSystemSignalsForGracefulShutdown)
	s.StartAsync()
}

func Produce(topic string, key string, message interface{}) (int64, error) {

	var keyBytes []byte
	if strings.TrimSpace(key) != "" {
		keyBytes = []byte(key)
	}

	var messageBytes []byte
	var err_message error = nil
	switch typeValue := message.(type) {
	case string:
		messageBytes = []byte(typeValue)
	case []byte:
		messageBytes = []byte(typeValue)
	default:
		messageBytes, err_message = json.Marshal(message)
	}

	if err_message != nil {
		return 0, err_message
	} else {
		err_register := registerProducer(0)

		if err_register == nil {
			delivery_chan := make(chan kafka.Event, 10000)
			err_produce := producer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Value:          messageBytes,
				Key:            keyBytes},
				delivery_chan,
			)

			if err_produce != nil {
				return 0, err_produce
			}

			e := <-delivery_chan
			m := e.(*kafka.Message)

			close(delivery_chan)

			if m.TopicPartition.Error != nil {
				return 0, m.TopicPartition.Error
			} else {
				return int64(m.TopicPartition.Offset), nil
			}
		} else {
			return 0, err_register
		}
	}
}

func registerProducer(retry int) error {

	if producer == nil {
		p, err := kafka.NewProducer(&kafka.ConfigMap{
			"bootstrap.servers": servers,
			"acks":              "all"})

		if err != nil && retry < registerProducerFailedRetry {
			return registerProducer(retry + 1)
		} else if err != nil {
			return err

		} else {
			producer = p
		}
	}
	return nil
}

func Consume(topic string, groupId string, batchSize int) (KafkaResult, error) {

	result := KafkaResult{
		Topic: topic,
	}
	var err_batch error = nil

	if strings.TrimSpace(topic) != "" && strings.TrimSpace(groupId) != "" {

		for i := 1; i <= batchSize; i++ {

			message, err_consume := registerConsumerAndConsume(topic, groupId, 0)
			if err_consume != nil {
				err_batch = err_consume
				break
			} else {

				if message != nil {
					result.Messages = append(result.Messages, *message)
				} else { // no error and no message, means there are no messages to consume
					break
				}
			}
		}
	} else {
		err_batch = errors.New("Kafka:Topic or Groupid is missing")
	}

	return result, err_batch
}

func registerConsumerAndConsume(topic string, groupId string, retry int) (*Message, error) {

	consumer, err_consumer := registerConsumer(topic, groupId, 0)

	if err_consumer != nil {
		return nil, err_consumer
	} else {
		if consumer.EOFReached {
			consumer, err_consumer = registerConsumerAgainAfterEOF(topic, groupId)
		}

		result, err_consume := consumeFromKafka(consumer, topic, groupId)

		if err_consume != nil && retry < consumeFailedRetry {
			return registerConsumerAndConsume(topic, groupId, retry+1)
		} else {
			if err_consume != nil {
				return result, err_consume
			} else {
				return result, nil
			}
		}
	}
}

func monitorSystemSignalsForGracefulShutdown() {
	select {
	case sig := <-sigchan:
		fmt.Printf("Kafka:Caught signal %v: terminating\n", sig)
		var topicAndGroups []string
		// close and clean all consumers
		if len(consumers) > 0 {
			for topic, groups := range consumers {
				if len(groups) > 0 {
					for group, _ := range groups {
						topicAndGroups = append(topicAndGroups, topic+"|"+group)
					}
				}
			}
		}

		if len(topicAndGroups) > 0 {
			for _, topicAndGroup := range topicAndGroups {
				topicAndGroupArray := strings.Split(topicAndGroup, "|")
				closeAndCleanTheConsumer(topicAndGroupArray[0], topicAndGroupArray[1])
			}
		}
		fmt.Println("Gracefully shuting down process")
		os.Exit(0)
	default:

	}
}

// Reference: https://github.com/confluentinc/confluent-kafka-go/blob/master/examples/consumer_example/consumer_example.go
// consumer.ReadMessage is not used because it is not giving kafka.PartitionEOF event
func consumeFromKafka(consumer KafkaConsumer, topic string, groupId string) (*Message, error) {

	var message *Message
	var err_consume error = nil
	run := true

	for run {
		ev := consumer.Consumer.Poll(consumePollTimeoutMS)
		if ev == nil {
			continue
		}

		// When ev (event) is not nil, stop the loop
		run = false

		switch e := ev.(type) {
		case *kafka.Message:
			//fmt.Printf("%% Message on %s:\n%s\n", e.TopicPartition, string(e.Value))
			message = &Message{
				Partition: e.TopicPartition.Partition,
				Offset:    int64(e.TopicPartition.Offset),
				Key:       e.Key,
				Value:     e.Value,
			}

		case kafka.PartitionEOF:
			// fmt.Printf("%% Reached %v\n", e)
			// fmt.Println("EOF Reached!")
			consumer.EOFReached = true
			consumers[topic][groupId] = consumer

		case kafka.Error:
			// Errors should generally be considered as informational, the client will try to automatically recover
			//fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
			// fmt.Println("consumer poll error")
			err_consume = e
		}
	}

	return message, err_consume
}

func registerConsumerAgainAfterEOF(topic string, groupId string) (KafkaConsumer, error) {

	closeAndCleanTheConsumer(topic, groupId)
	// Register consumer again
	return registerConsumer(topic, groupId, 0)
}

func closeAndCleanTheConsumer(topic string, groupId string) {
	// Close the consumer
	consumers[topic][groupId].Consumer.Close()

	// Remove consumer from global map
	delete(consumers[topic], groupId)
}

func registerConsumer(topic string, groupId string, retry int) (KafkaConsumer, error) {

	var err_consumer error = nil

	if len(consumers[topic]) == 0 || consumers[topic][groupId].Consumer == nil {

		consumer, err_newconsumer := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers":    servers,
			"group.id":             groupId,
			"auto.offset.reset":    "earliest",
			"enable.partition.eof": true,
			"enable.auto.commit":   autoCommit,
		})

		if err_newconsumer != nil {
			err_consumer = err_newconsumer
		}

		topics := []string{topic}

		err_sub := consumer.SubscribeTopics(topics, nil)
		if err_sub != nil {
			err_consumer = err_sub
		}

		if len(consumers[topic]) == 0 {
			consumers[topic] = make(map[string]KafkaConsumer)
		}

		con := KafkaConsumer{
			Consumer:   consumer,
			EOFReached: false,
		}

		consumers[topic][groupId] = con
	}

	if err_consumer != nil && retry < registerConsumerFailedRetry {
		return registerConsumer(topic, groupId, retry+1)
	} else {
		return consumers[topic][groupId], err_consumer
	}
}

func CommitOffset(topic string, groupId string) error {
	_, err := consumers[topic][groupId].Consumer.Commit()
	return err
}
