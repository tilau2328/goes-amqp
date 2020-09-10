package event

import (
	"context"
	"github.com/tilau2328/goes/core/event"
)

type Sink struct {
	ctx context.Context
}

func (s *Sink) Publish(event event.IEvent) {

}
