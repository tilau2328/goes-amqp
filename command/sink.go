package command

import (
	"encoding/json"
	"fmt"
	"github.com/streadway/amqp"
	"github.com/tilau2328/goes/core/command"
)

type Sink struct {
	q  *amqp.Queue
	ch *amqp.Channel
	cb func(interface{}, *amqp.Delivery) (interface{}, error)
}

func NewSink(
	q *amqp.Queue,
	ch *amqp.Channel,
	cb func(interface{}, *amqp.Delivery) (interface{}, error),
) *Sink {
	return &Sink{q, ch, cb}
}

func (s *Sink) Handle(c command.ICommand, response interface{}) (interface{}, error) {
	m, err := s.ch.Consume(
		s.q.Name, // queue
		"",       // consumer
		true,     // auto-ack
		false,    // exclusive
		false,    // no-local
		false,    // no-wait
		nil,      // args
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register a consumer")
	}
	var body []byte
	correlationId := c.Id().String()
	body, err = json.Marshal(c.Message())
	err = s.ch.Publish(
		"",          // exchange
		"rpc_queue", // routing key
		false,       // mandatory
		false,       // immediate
		amqp.Publishing{
			ContentType:   "application/json",
			CorrelationId: correlationId,
			ReplyTo:       s.q.Name,
			Body:          body,
		})
	for d := range m {
		if correlationId == d.CorrelationId {
			err = json.Unmarshal(d.Body, response)
			if err != nil {
				return nil, err
			}
			return s.cb(response, &d)
		}
	}
	return nil, nil
}
