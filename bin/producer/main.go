package main

import (
	"encoding/json"
	"fmt"
	"kafkademo/pkg/types"
	"log/slog"
	"os"
	"time"

	"github.com/IBM/sarama"
	flag "github.com/spf13/pflag"
)

var (
	appID    int
	topic    string
	user     string
	password string
)

func init() {
	flag.IntVar(&appID, "id", 1, "app id")
	flag.StringVar(&topic, "topic", "messages", "topic in kafka")
	flag.StringVar(&user, "user", "", "kafka sasl user")
	flag.StringVar(&password, "password", "", "kafka sasl password for sasl user")
	flag.Parse()
}

func connectProducer(brokers []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_2_0
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Net.SASL.Enable = true
	config.Net.SASL.User = user
	config.Net.SASL.Password = password

	return sarama.NewSyncProducer(brokers, config)
}

func main() {
	slog.Info("consumer", "id", appID, "topic", topic)

	brokers := []string{"localhost:29092"}
	producer, err := connectProducer(brokers)
	if err != nil {
		slog.Error("connectProducer", "err", err)
		os.Exit(1)
	}
	defer producer.Close()

	i := 0
	for {
		i++
		data := types.Message{
			AppID:   appID,
			TS:      time.Now(),
			Message: fmt.Sprintf("message %d", i),
		}
		mData, _ := json.Marshal(data)

		msg := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(string(mData)),
		}

		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			slog.Error("SendMessage", "err", err, "data", data)
			continue
		}

		slog.Info("sent",
			"message", data.Message,
			"partition", partition,
			"offset", offset,
		)

		time.Sleep(5 * time.Second)
	}
}
