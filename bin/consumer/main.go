package main

import (
	"encoding/json"
	"fmt"
	"kafkademo/pkg/types"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
	flag "github.com/spf13/pflag"
)

var (
	appID     int
	topic     string
	offset    int
	partition int
	user      string
	password  string
)

func init() {
	flag.IntVar(&appID, "id", 2, "app id")
	flag.IntVar(&offset, "offset", 0, "kafka offset (start reading at this point)")
	flag.IntVar(&partition, "partition", 0, "kafka partition")
	flag.StringVar(&topic, "topic", "messages", "topic in kafka")
	flag.StringVar(&user, "user", "", "kafka sasl user")
	flag.StringVar(&password, "password", "", "kafka sasl password for sasl user")
	flag.Parse()
}

func connectConsumer(brokers []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_2_0
	config.Consumer.Return.Errors = true
	config.Net.SASL.Enable = true
	config.Net.SASL.User = user
	config.Net.SASL.Password = password

	return sarama.NewConsumer(brokers, config)
}

func main() {
	slog.Info("consumer", "id", appID, "topic", topic)

	brokers := []string{"localhost:29092"}
	worker, err := connectConsumer(brokers)
	if err != nil {
		slog.Error("connectConsumer", "err", err)
		os.Exit(1)
	}
	defer worker.Close()

	consumer, err := worker.ConsumePartition(topic, int32(partition), int64(offset))
	if err != nil {
		slog.Error("ConsumePartition", "err", err)
		os.Exit(1)
	}

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	doneCh := make(chan struct{})
	go func() {
		for {
			select {

			case msg := <-consumer.Messages():
				data := types.Message{}
				if err := json.Unmarshal(msg.Value, &data); err != nil {
					slog.Error("Unmarshal", "err", err, "value", msg.Value)
					continue
				}
				slog.Info("received",
					"message", data.Message,
					"offset", msg.Offset,
					"senderID", data.AppID,
				)

			case err := <-consumer.Errors():
				slog.Error("consumer", "Errors", err)

			case <-sigchan:
				fmt.Println("Interrupt signal detected")
				doneCh <- struct{}{}

			}
		}
	}()

	<-doneCh
}
