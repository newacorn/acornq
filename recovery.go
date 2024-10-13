package acornq

import "time"

type recovery struct {
	queue         []string
	broker        *Broker
	stopCh        chan struct{}
	errHandler    ErrHandler
	checkInterval time.Duration
}

func newRecovery(stopCh chan struct{}, broker *Broker, queue []string, interval time.Duration, errHandler ErrHandler) *recovery {
	return &recovery{
		queue:         queue,
		broker:        broker,
		stopCh:        stopCh,
		errHandler:    errHandler,
		checkInterval: interval,
	}
}

func (r *recovery) Start() {
	ticker := time.NewTicker(r.checkInterval)
	for {
		select {
		case <-ticker.C:
			err := r.broker.RecoveryTasks(r.queue, 55*time.Second)
			if err != nil {
				r.errHandler(err)
			}
			// task in active set idle more than 30 seconds.
			// saddunion live and active set max score compare now.
			// put these tasks back to pending set.
		case <-r.stopCh:
			ticker.Stop()
			return
		}
	}
}
