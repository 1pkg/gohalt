package gohalt

import (
	"context"
	"fmt"
	"sync"
	"time"

	uuid "github.com/satori/go.uuid"
	kafka "github.com/segmentio/kafka-go"
	"github.com/streadway/amqp"
)

type Enqueuer interface {
	Enqueue(context.Context, []byte) error
}

type enqrabbit struct {
	lock       sync.Mutex
	memconnect Runnable
	newrpub    func([]byte) Runnable
	connection *amqp.Connection
	channel    *amqp.Channel
}

func NewEnqueuerRabbit(url string, queue string, retries uint64) Enqueuer {
	exchange := fmt.Sprintf("gohalt_exchange_%s", uuid.NewV4())
	enq := &enqrabbit{}
	enq.memconnect = cached(0, func(ctx context.Context) error {
		return enq.connect(ctx, url, queue, exchange)
	})
	enq.newrpub = func(message []byte) Runnable {
		return retried(retries, func(ctx context.Context) error {
			if err := enq.channel.Publish(
				exchange,
				queue,
				false,
				false,
				amqp.Publishing{
					DeliveryMode: 2,
					AppId:        "gohalt_enqueue",
					MessageId:    fmt.Sprintf("gohalt_enqueue_%s", uuid.NewV4()),
					Timestamp:    time.Now().UTC(),
					Body:         message,
				},
			); err != nil {
				// on error refresh connection just in case
				_ = enq.close(ctx)
				_ = enq.connect(ctx, url, queue, exchange)
				return err
			}
			return nil
		})
	}
	return enq
}

func (enq *enqrabbit) Enqueue(ctx context.Context, message []byte) error {
	enq.lock.Lock()
	defer enq.lock.Unlock()
	if err := enq.memconnect(ctx); err != nil {
		return err
	}
	return enq.newrpub(message)(ctx)
}

func (enq *enqrabbit) close(context.Context) error {
	if enq.channel == nil || enq.connection == nil {
		return nil
	}
	if err := enq.channel.Close(); err != nil {
		return err
	}
	return enq.connection.Close()
}

func (enq *enqrabbit) connect(_ context.Context, url string, queue string, exchange string) error {
	connection, err := amqp.Dial(url)
	if err != nil {
		return err
	}
	channel, err := connection.Channel()
	if err != nil {
		return err
	}
	if err := channel.ExchangeDeclare(exchange, "direct", true, true, false, false, nil); err != nil {
		return err
	}
	if _, err := channel.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		return err
	}
	if err := channel.QueueBind(queue, queue, exchange, false, nil); err != nil {
		return err
	}
	enq.connection = connection
	enq.channel = channel
	return nil
}

type enqkafka struct {
	lock       sync.Mutex
	memconnect Runnable
	newrpub    func([]byte) Runnable
	connection *kafka.Conn
}

func NewEnqueuerKafka(net string, url string, topic string, retries uint64) Enqueuer {
	enq := &enqkafka{}
	enq.memconnect = cached(0, func(ctx context.Context) error {
		return enq.connect(ctx, net, url, topic)
	})
	enq.newrpub = func(message []byte) Runnable {
		return retried(retries, func(ctx context.Context) error {
			if _, err := enq.connection.WriteMessages(kafka.Message{
				Time:  time.Now().UTC(),
				Key:   []byte(fmt.Sprintf("gohalt_enqueue_%s", uuid.NewV4())),
				Value: message,
			}); err != nil {
				// on error refresh connection just in case
				_ = enq.close(ctx)
				_ = enq.connect(ctx, net, url, topic)
				return err
			}
			return nil
		})
	}
	return enq
}

func (enq *enqkafka) Enqueue(ctx context.Context, message []byte) error {
	enq.lock.Lock()
	defer enq.lock.Unlock()
	if err := enq.memconnect(ctx); err != nil {
		return err
	}
	return enq.newrpub(message)(ctx)
}

func (enq *enqkafka) close(context.Context) error {
	if enq.connection == nil {
		return nil
	}
	return enq.connection.Close()
}

func (enq *enqkafka) connect(ctx context.Context, net string, url string, topic string) error {
	connection, err := kafka.DialLeader(ctx, net, url, topic, 0)
	if err != nil {
		return err
	}
	enq.connection = connection
	return nil
}

type enqmock struct {
	err error
}

func (enq enqmock) Enqueue(context.Context, []byte) error {
	return enq.err
}
