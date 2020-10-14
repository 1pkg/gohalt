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

// Enqueuer defines abstract message enqueuing interface.
type Enqueuer interface {
	// Enqueue enqueues provided message or returns internal error if any happened.
	Enqueue(context.Context, []byte) error
}

// msgr defines inner type that creates new message publisher runnable.
type msgr func([]byte) Runnable

type enqrabbit struct {
	msgr       msgr
	connection *amqp.Connection
	channel    *amqp.Channel
}

// NewEnqueuerRabbit creates RabbitMQ enqueuer instance
// with cached connection and failure retries
// which enqueues provided message to the specified queue.
// New unique exchange `gohalt_exchange_{{uuid}}` is created for each new enqueuer,
// new unique message id `gohalt_enqueue_{{uuid}}` is created for each new message.
func NewEnqueuerRabbit(url string, queue string, retries uint64) Enqueuer {
	exchange := fmt.Sprintf("gohalt_exchange_%s", uuid.NewV4())
	enq := &enqrabbit{}
	memconnect, reset := cached(0, func(ctx context.Context) error {
		if err := enq.close(ctx); err != nil {
			return err
		}
		return enq.connect(ctx, url, queue, exchange)
	})
	var lock sync.Mutex
	enq.msgr = func(message []byte) Runnable {
		return retried(retries, func(ctx context.Context) error {
			lock.Lock()
			defer lock.Unlock()
			if err := memconnect(ctx); err != nil {
				return err
			}
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
				// on publish error refresh connection just in case
				_ = reset(ctx)
				return err
			}
			return nil
		})
	}
	return enq
}

func (enq *enqrabbit) Enqueue(ctx context.Context, message []byte) error {
	return enq.msgr(message)(ctx)
}

func (enq *enqrabbit) close(context.Context) error {
	if enq.connection == nil {
		return nil
	}
	if err := enq.channel.Close(); err != nil {
		return err
	}
	if err := enq.connection.Close(); err != nil {
		return err
	}
	enq.connection, enq.channel = nil, nil
	return nil
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
	msgr       msgr
	connection *kafka.Conn
}

// NewEnqueuerKafka creates Kafka enqueuer instance
// with cached connection and failure retries
// which enqueues provided message to the specified topic.
// New unique message key `gohalt_enqueue_{{uuid}}` is created for each new message.
func NewEnqueuerKafka(net string, url string, topic string, retries uint64) Enqueuer {
	enq := &enqkafka{}
	memconnect, reset := cached(0, func(ctx context.Context) error {
		if err := enq.close(ctx); err != nil {
			return err
		}
		return enq.connect(ctx, net, url, topic)
	})
	var lock sync.Mutex
	enq.msgr = func(message []byte) Runnable {
		return retried(retries, func(ctx context.Context) error {
			lock.Lock()
			defer lock.Unlock()
			if err := memconnect(ctx); err != nil {
				return err
			}
			if _, err := enq.connection.WriteMessages(kafka.Message{
				Time:  time.Now().UTC(),
				Key:   []byte(fmt.Sprintf("gohalt_enqueue_%s", uuid.NewV4())),
				Value: message,
			}); err != nil {
				// on write error refresh connection just in case
				_ = reset(ctx)
				return err
			}
			return nil
		})
	}
	return enq
}

func (enq *enqkafka) Enqueue(ctx context.Context, message []byte) error {
	return enq.msgr(message)(ctx)
}

func (enq *enqkafka) close(context.Context) error {
	if enq.connection == nil {
		return nil
	}
	if err := enq.connection.Close(); err != nil {
		return err
	}
	enq.connection = nil
	return nil
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
