package acornq

import (
	"context"
	"time"
)

type Cleaner struct {
	broker     *Broker
	interval   time.Duration
	stopCh     chan struct{}
	errHandler ErrHandler
}

func NewCleaner(b *Broker, interval time.Duration, errHandler ErrHandler) *Cleaner {
	return &Cleaner{
		broker:     b,
		interval:   interval,
		errHandler: errHandler,
	}
}

func (c *Cleaner) Start() {
	for {
		select {
		case <-c.stopCh:
			return
		default:
			err := c.broker.CleanUpArchive(context.Background(), 500)
			if err != nil {
				c.errHandler(err)
			}
			time.Sleep(c.interval)
		}
	}
}

func (c *Cleaner) scanArchive() {
}
