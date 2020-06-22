package gohalt

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/streadway/amqp"
)

var uuid uint64

type Enqueuer interface {
	Publish(context.Context, interface{}) error
	Close() error
}

type amqpp struct {
	conn *amqp.Connection
	ch   *amqp.Channel

	pool time.Duration
	url  string
	que  string
	exch string
}

func NewPublisherJsonAmqp(ctx context.Context, url string, queue string, pool time.Duration) (*amqpp, error) {
	uuid++
	exchange := fmt.Sprintf("gohalt_exchange_%d", uuid)
	enq := &amqpp{pool: pool, url: url, que: queue, exch: exchange}
	if err := enq.connect(); err != nil {
		return nil, err
	}
	loop(ctx, pool, func(ctx context.Context) error {
		if err := enq.Close(); err != nil {
			return err
		}
		enq.connect()
		return ctx.Err()
	})
	return enq, nil
}

func (enq *amqpp) Publish(ctx context.Context, data interface{}) error {
	body, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return enq.ch.Publish(
		enq.exch,
		enq.que,
		false,
		false,
		amqp.Publishing{
			ContentType:     "application/json",
			ContentEncoding: "",
			DeliveryMode:    2,
			Timestamp:       time.Now().UTC(),
			AppId:           "gohalt_enqueue",
			Body:            body,
		},
	)
}

func (enq *amqpp) Close() error {
	if err := enq.ch.Close(); err != nil {
		return err
	}
	return enq.conn.Close()
}

func (enq *amqpp) connect() error {
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
	return nil
}
