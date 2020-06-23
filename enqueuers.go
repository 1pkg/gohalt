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
	Close(context.Context) error
}

type amqpp struct {
	conn *amqp.Connection
	ch   *amqp.Channel
	mut  sync.Mutex

	pool time.Duration
	url  string
	que  string
	exch string
}

func NewEnqueuerAmqp(ctx context.Context, url string, queue string, pool time.Duration) (*amqpp, error) {
	exchange := fmt.Sprintf("gohalt_exchange_%s", uuid.NewV4())
	enq := &amqpp{pool: pool, url: url, que: queue, exch: exchange}
	if err := enq.connect(ctx); err != nil {
		return nil, err
	}
	loop(ctx, pool, func(ctx context.Context) error {
		if err := enq.Close(ctx); err != nil {
			return err
		}
		enq.connect(ctx)
		return ctx.Err()
	})
	return enq, nil
}

func (enq *amqpp) Enqueue(ctx context.Context, message []byte) error {
	enq.mut.Lock()
	defer enq.mut.Unlock()
	return enq.ch.Publish(
		enq.exch,
		enq.que,
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

func (enq *amqpp) Close(context.Context) error {
	enq.mut.Lock()
	defer enq.mut.Unlock()
	if err := enq.ch.Close(); err != nil {
		return err
	}
	return enq.conn.Close()
}

func (enq *amqpp) connect(context.Context) error {
	conn, err := amqp.Dial(enq.url)
	if err != nil {
		return err
	}
	ch, err := conn.Channel()
	if err := ch.ExchangeDeclare(enq.exch, "direct", true, true, false, false, nil); err != nil {
		return err
	}
	if _, err := ch.QueueDeclare(enq.que, true, false, false, false, nil); err != nil {
		return err
	}
	if err := ch.QueueBind(enq.que, enq.que, enq.exch, false, nil); err != nil {
		return err
	}
	enq.mut.Lock()
	enq.conn = conn
	enq.ch = ch
	enq.mut.Unlock()
	return nil
}

type kafkap struct {
	conn *kafka.Conn
	mut  sync.Mutex

	pool  time.Duration
	net   string
	url   string
	topic string
}

func NewEnqueuerKafka(ctx context.Context, network string, url string, topic string, pool time.Duration) (*kafkap, error) {
	enq := &kafkap{net: network, url: url, topic: topic}
	if err := enq.connect(ctx); err != nil {
		return nil, err
	}
	loop(ctx, pool, func(ctx context.Context) error {
		if err := enq.Close(ctx); err != nil {
			return err
		}
		enq.connect(ctx)
		return ctx.Err()
	})
	return enq, nil
}

func (enq *kafkap) Enqueue(ctx context.Context, message []byte) error {
	enq.mut.Lock()
	defer enq.mut.Unlock()
	if _, err := enq.conn.WriteMessages(kafka.Message{
		Time:  time.Now().UTC(),
		Key:   []byte(fmt.Sprintf("gohalt_enqueue_%s", uuid.NewV4())),
		Value: message,
	}); err != nil {
		return err
	}
	return nil
}

func (enq *kafkap) Close(ctx context.Context) error {
	enq.mut.Lock()
	defer enq.mut.Unlock()
	return enq.conn.Close()
}

func (enq *kafkap) connect(ctx context.Context) error {
	conn, err := kafka.DialLeader(ctx, enq.net, enq.url, enq.topic, 0)
	if err != nil {
		return err
	}
	enq.mut.Lock()
	enq.conn = conn
	enq.mut.Unlock()
	return nil
}
