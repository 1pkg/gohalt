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

	url  string
	que  string
	exch string
}

func NewPublisherJsonAmqp(url string, queue string) (*amqpp, error) {
	uuid++
	exchange := fmt.Sprintf("gohalt_exchange_%d", uuid)
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err := ch.ExchangeDeclare(exchange, "direct", true, true, false, false, nil); err != nil {
		return nil, err
	}
	if _, err := ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		return nil, err
	}
	if err := ch.QueueBind(queue, queue, exchange, false, nil); err != nil {
		return nil, err
	}
	return &amqpp{
		conn: conn,
		ch:   ch,
		url:  url,
		que:  queue,
		exch: exchange,
	}, nil
}

func (p *amqpp) Publish(ctx context.Context, data interface{}) error {
	body, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return p.ch.Publish(
		p.exch,
		p.que,
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

func (p *amqpp) Close() error {
	if err := p.ch.Close(); err != nil {
		return err
	}
	return p.conn.Close()
}
