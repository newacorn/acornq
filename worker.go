package acornq

import (
	"context"
	"math/rand"
	"time"
)

type Worker struct {
	s            *Server
	queues       []string
	broker       *Broker
	queuesStrict bool
	beatItemCh   chan *liveItem
	pollInterval time.Duration
}

func (w *Worker) exec() {
	for {
		if w.s.stop.Load() == 1 {
			return
		}
		ts, err := w.broker.PickTasks(context.Background(), w.queueNames(), 1)
		if err != nil {
			w.s.errHandler(err)
		}
		if w.s.stop.Load() == 1 || err != nil {
			// move ts from active set to pending set.
			if len(ts) > 0 {
				err = w.broker.Active2Pending(context.Background(), ts)
				if err != nil {
					w.s.errHandler(err)
				}
			}
			return
		}
		if len(ts) == 0 {
			time.Sleep(w.pollInterval)
		}

		for i, t := range ts {
			err = w.s.handler.Handle(t)
			if err != nil {
				w.handleConsumerError(t, err)
			} else {
				err = w.broker.Active2Archive(context.Background(), []*TaskInfo{t}, true)
				if err != nil {
					w.s.errHandler(err)
				}
			}
			if w.s.stop.Load() == 1 {
				// move left ts from active set to pending set.
				if len(ts[i+1:]) > 0 {
					err = w.broker.Active2Pending(context.Background(), ts[i+1:])
					if err != nil {
						w.s.errHandler(err)
					}
				}
				return
			}
		}
	}
}

func (w *Worker) handleConsumerError(t *TaskInfo, err error) {
	t.ErrorMsg = s2b(err.Error())
	er := w.broker.SetErrorMsg(context.Background(), t)
	if er != nil {
		w.s.errHandler(er)
	}
	if //goland:noinspection GoDirectComparisonOfErrors
	err == SkipRetry || t.Retried >= t.Retry {
		err = w.broker.Active2Archive(context.Background(), []*TaskInfo{t}, false)
		if err != nil {
			w.s.errHandler(err)
		}
		return
	}

	if w.s.isFailure(err) {
		t.Retried++
	}
	t.PendingAt = time.Now().Add(w.s.retryDelayFunc(t.Retried, err, t)).Unix()
	err = w.broker.RetryTasks(context.Background(), []*TaskInfo{t})
	if err != nil {
		w.s.errHandler(err)
	}
	return
}

func (w *Worker) queueNames() []string {
	if w.queuesStrict || len(w.queues) == 1 {
		return w.queues
	}
	rand.Shuffle(len(w.queues), func(i, j int) {
		w.queues[i], w.queues[j] = w.queues[j], w.queues[i]
	})
	return w.queues
}
