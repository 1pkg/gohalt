package gohalt

import (
	"context"
	"fmt"
	"sync"
	"time"

	uuid "github.com/satori/go.uuid"
	"github.com/segmentio/kafka-go"
	"github.com/streadway/amqp"
)

type Enqueuer interface {
	Enqueue(context.Context, []byte) error
}

type amqpp struct {
	memconnect Runnable
	connection *amqp.Connection
	channel    *amqp.Channel
	queue      string
	exchange   string
}

func NewEnqueuerAmqp(url string, queue string, cahce time.Duration) *amqpp {
	exchange := fmt.Sprintf("gohalt_exchange_%s", uuid.NewV4())
	enq := &amqpp{queue: queue, exchange: exchange}
	var lock sync.Mutex
	enq.memconnect = cached(cahce, func(ctx context.Context) error {
		lock.Lock()
		defer lock.Unlock()
		if err := enq.close(ctx); err != nil {
			return err
		}
		return enq.connect(ctx, url)
	})
	return enq
}

func (enq *amqpp) Enqueue(ctx context.Context, message []byte) error {
	if err := enq.memconnect(ctx); err != nil {
		return err
	}
	return enq.channel.Publish(
		enq.exchange,
		enq.queue,
		false,
		false,
		amqp.Publishing{
			DeliveryMode: 2,
			AppId:        "gohalt_enqueue",
			MessageId:    fmt.Sprintf("gohalt_enqueue_%s", uuid.NewV4()),
			Timestamp:    time.Now().UTC(),
			Body:         message,
		},
	)
}

func (enq *amqpp) close(context.Context) error {
	if enq.channel == nil || enq.connection == nil {
		return nil
	}
	if err := enq.channel.Close(); err != nil {
		return err
	}
	return enq.connection.Close()
}

func (enq *amqpp) connect(ctx context.Context, url string) error {
	connection, err := amqp.Dial(url)
	if err != nil {
		return err
	}
	channel, err := connection.Channel()
	if err := channel.ExchangeDeclare(enq.exchange, "direct", true, true, false, false, nil); err != nil {
		return err
	}
	if _, err := channel.QueueDeclare(enq.queue, true, false, false, false, nil); err != nil {
		return err
	}
	if err := channel.QueueBind(enq.queue, enq.queue, enq.exchange, false, nil); err != nil {
		return err
	}
	enq.connection = connection
	enq.channel = channel
	return nil
}

type kafkap struct {
	memconnect Runnable
	connection *kafka.Conn
}

func NewEnqueuerKafka(net string, url string, topic string, cache time.Duration) *kafkap {
	enq := &kafkap{}
	var lock sync.Mutex
	enq.memconnect = cached(cache, func(ctx context.Context) error {
		lock.Lock()
		defer lock.Unlock()
		if err := enq.close(ctx); err != nil {
			return err
		}
		return enq.connect(ctx, net, url, topic)
	})
	return enq
}

func (enq *kafkap) Enqueue(ctx context.Context, message []byte) error {
	if err := enq.memconnect(ctx); err != nil {
		return err
	}
	if _, err := enq.connection.WriteMessages(kafka.Message{
		Time:  time.Now().UTC(),
		Key:   []byte(fmt.Sprintf("gohalt_enqueue_%s", uuid.NewV4())),
		Value: message,
	}); err != nil {
		return err
	}
	return nil
}

func (enq *kafkap) close(ctx context.Context) error {
	if enq.connection == nil {
		return nil
	}
	return enq.connection.Close()
}

func (enq *kafkap) connect(ctx context.Context, net string, url string, topic string) error {
	connection, err := kafka.DialLeader(ctx, net, url, topic, 0)
	if err != nil {
		return err
	}
	enq.connection = connection
	return nil
}
