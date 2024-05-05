package kgo

import (
	"context"
	"sync"

	"go.unistack.org/micro/v3/broker"
)

type event struct {
	ctx   context.Context
	topic string
	err   error
	sync.RWMutex
	msg *broker.Message
	ack bool
}

func (p *event) Context() context.Context {
	return p.ctx
}

func (p *event) Topic() string {
	return p.topic
}

func (p *event) Message() *broker.Message {
	return p.msg
}

func (p *event) Ack() error {
	p.ack = true
	return nil
}

func (p *event) Error() error {
	return p.err
}

func (p *event) SetError(err error) {
	p.err = err
}

var eventPool = sync.Pool{
	New: func() interface{} {
		return &event{msg: &broker.Message{}}
	},
}
