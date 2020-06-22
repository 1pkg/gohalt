package gohalt

import (
	"context"
	"fmt"

	"github.com/streadway/amqp"
)

var uuid uint64

type Publisher interface {
	Publish(context.Context, []byte) error
	Close() error
}

type amqpp struct {
	conn *amqp.Connection
	ch   *amqp.Channel

	url  string
	que  string
	exch string
}

func NewPublisherAmqp(url string, queue string) (*amqpp, error) {
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

func (p *amqpp) Publish(ctx context.Context, message []byte) error {
	return p.ch.Publish(
		p.exch,
		p.que,
		false,
		false,
		amqp.Publishing{Body: []byte(message)},
	)
}

func (p *amqpp) Close() error {
	if err := p.ch.Close(); err != nil {
		return err
	}
	return p.conn.Close()
}
