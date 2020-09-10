package command

import (
	"encoding/json"
	"fmt"
	"github.com/streadway/amqp"
	"github.com/tilau2328/goes/core/command"
)

type Source struct {
	q        *amqp.Queue
	ch       *amqp.Channel
	bus      command.ICommandBus
	request  interface{}
	template func(interface{}, *amqp.Delivery) command.ICommand
}

func NewSource(
	q *amqp.Queue,
	ch *amqp.Channel,
	bus command.ICommandBus,
	request interface{},
	template func(interface{}, *amqp.Delivery) command.ICommand,
) *Source {
	return &Source{q, ch, bus, request, template}
}

func (s *Source) Handle(d *amqp.Delivery, request interface{}) {
	err := json.Unmarshal(d.Body, s.request)
	var result interface{}
	result, err = s.bus.Handle(s.template(request, d))
	if err != nil {
		return
	}
	var response []byte
	response, err = json.Marshal(result)
	if err != nil {
		return
	}
	err = s.ch.Publish(
		"",        // exchange
		d.ReplyTo, // routing key
		false,     // mandatory
		false,     // immediate
		amqp.Publishing{
			ContentType:   "application/json",
			CorrelationId: d.CorrelationId,
			Body:          response,
		})
	if err != nil {
		return
	}
	_ = d.Ack(false)
}

func (s *Source) Register() (<-chan amqp.Delivery, error) {
	err := s.ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		return nil, fmt.Errorf("failed to set QoS")
	}
	return s.ch.Consume(
		s.q.Name, // queue
		"",       // consumer
		false,    // auto-ack
		false,    // exclusive
		false,    // no-local
		false,    // no-wait
		nil,      // args
	)
}

func (s *Source) Listen() error {
	m, err := s.Register()
	if err != nil {
		return err
	}
	forever := make(chan bool)
	go func() {
		for d := range m {
			s.Handle(&d, s.template(d.Body, &d))
		}
	}()
	<-forever
	return nil
}
