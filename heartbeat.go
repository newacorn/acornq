package acornq

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

type heartBeatWorker struct {
	// inherit from Server
	stopCh chan struct{}
	// inherit from Worker
	beatItemCh chan *liveItem
	// 20 seconds fixed
	liveDuration time.Duration
	// 10 seconds fixed
	batchDuration time.Duration
	beatContainer []*heartbeatBatch
	zombieLive    []*liveItem
	mu            sync.Mutex
	broker        *Broker
	stop          atomic.Bool
}

func newHeartBeatWorker(stopCh chan struct{}, beatItemCh chan *liveItem, broker *Broker) *heartBeatWorker {
	return &heartBeatWorker{
		stopCh:        stopCh,
		beatItemCh:    beatItemCh,
		broker:        broker,
		liveDuration:  25 * time.Second,
		batchDuration: 20 * time.Second,
	}
}

func (w *heartBeatWorker) Start() {
	start := nanoTime()
	batch := &heartbeatBatch{duration: w.liveDuration, w: w}
	for {
		select {
		case item := <-w.beatItemCh:
			if item.stop.Load() {
				// task terminated
				if item.batch != nil {
					item.batch.StopItem(item)
				}
				continue
			}
			// task is peeked and handling
			now := nanoTime()
			if now <= start+int64(w.batchDuration) {
				batch.addItem(item)
			} else {
				start = now
				if batch.len() > 0 {
					// this batch collect at least one item during w.batchDuration
					registerTickerItem(batch, w.liveDuration)
					w.AddBatch(batch)
					// create a new batch
					batch = &heartbeatBatch{duration: w.liveDuration, w: w}
				}
			}
		case <-w.stopCh:
			w.Stop()
			return
		}
	}
}

// Stop stops the heart beat worker, clear their taskKeys from live queue.
func (w *heartBeatWorker) Stop() {
	items := make([]*liveItem, 0, 10)
	for _, batch := range w.beatContainer {
		batch.stop.Store(true)
		start := true
		batch.mu.Lock()
		start = batch.start
		batch.mu.Unlock()
		if !start {
			continue
		}
		for _, item := range batch.items {
			items = append(items, item)
		}
	}
	clear(w.beatContainer)
	if len(items) == 0 {
		return
	}
	_ = w.broker.DeleteLiveTasks(context.Background(), items)

}

func (w *heartBeatWorker) Clean() (duration time.Duration, stop bool) {
	if w.stop.Load() {
		stop = true
		return
	}
	var zombieLive []*liveItem
	w.mu.Lock()
	if len(w.zombieLive) > 0 {
		zombieLive = make([]*liveItem, len(w.zombieLive))
		copy(zombieLive, w.zombieLive)
		clear(w.zombieLive)
		w.zombieLive = w.zombieLive[:0]
	}
	w.mu.Unlock()
	if len(zombieLive) > 0 {
		_ = w.broker.DeleteLiveTasks(context.Background(), w.zombieLive)
	}
	duration = w.liveDuration
	return
}

func (w *heartBeatWorker) AddBatch(batch *heartbeatBatch) {
	w.beatContainer = append(w.beatContainer, batch)
}

func (w *heartBeatWorker) zombieLiveAdd(item *liveItem) {
	w.mu.Lock()
	w.zombieLive = append(w.zombieLive, item)
	w.mu.Unlock()
}

type heartbeatBatch struct {
	items    []*liveItem
	duration time.Duration
	mu       sync.Mutex
	w        *heartBeatWorker
	stop     atomic.Bool
	start    bool
}

func (h *heartbeatBatch) Clean() (duration time.Duration, stop bool) {
	if h.stop.Load() {
		stop = true
		return
	}
	h.mu.Lock()
	if len(h.items) == 0 {
		stop = true
		h.mu.Unlock()
		return
	}
	items := h.items
	duration = h.duration
	start := h.start
	if !start {
		h.start = true
	}
	h.mu.Unlock()
	_ = h.w.broker.LiveTasksChange(context.Background(), items, start)
	return
}

func (h *heartbeatBatch) StopItem(item *liveItem) {
	h.mu.Lock()
	if h.start {
		h.w.zombieLiveAdd(item)
	}
	h.items = slices.DeleteFunc(h.items, func(item1 *liveItem) bool {
		return item1.taskID == item.taskID
	})
	h.mu.Unlock()
}

func (h *heartbeatBatch) Register() {
	items := h.items[:0]
	for _, item := range h.items {
		if !item.stop.Load() {
			items = append(items, item)
		}
	}
	if len(items) == 0 {
		return
	}
	h.items = items
	registerTickerItem(h, h.duration)
}

func (h *heartbeatBatch) len() int {
	return len(h.items)
}

func (h *heartbeatBatch) addItem(item *liveItem) {
	item.batch = h
	h.items = append(h.items, item)
}

type liveItem struct {
	taskID string
	queue  string
	batch  *heartbeatBatch
	stop   atomic.Bool
}
