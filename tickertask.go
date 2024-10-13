package acornq

import (
	"math"
	"sync"
	"sync/atomic"
	"time"
)

var maxWhen = time.Hour * 365 * 24 * 10
var defaultTickerWorker = TickerWorker{
	stopChan:    make(chan struct{}, 1),
	cleanCh:     make(chan struct{}),
	t:           time.NewTimer(maxWhen),
	minDuration: time.Millisecond * 500,
}

func registerTickerItem(client TickerTasker, duration time.Duration) {
	defaultTickerWorker.Register(client, duration)
}
func stopTickerWorker(block bool) {
	defaultTickerWorker.Stop(block)
}

type TickerWorker struct {
	items      []*tickerTaskWrapper
	mu         sync.Mutex
	zeroWhen   atomic.Int64
	cleanStart atomic.Uint32
	numItems   atomic.Uint32
	stop       atomic.Bool
	stopChan   chan struct{}
	t          *time.Timer
	// The minimum runtime interval for the same TickerTasker
	minDuration time.Duration
	cleanCh     chan struct{}
}

type TickerTasker interface {
	Clean() (duration time.Duration, stop bool)
}

type tickerTaskWrapper struct {
	client TickerTasker
	when   int64
}

func (cc *TickerWorker) Stop(block bool) {
	if cc.stop.Load() {
		return
	}
	cc.mu.Lock()
	cc.stop.Store(true)
	cc.stopChan <- struct{}{}
	cc.mu.Unlock()
	if block {
		<-cc.cleanCh
	}
}

func (cc *TickerWorker) Register(client TickerTasker, duration time.Duration) {
	if client == nil {
		return
	}
	var item *tickerTaskWrapper
	when := nanoTime() + int64(duration)
	cc.mu.Lock()
	l := len(cc.items)
	if cap(cc.items) > l {
		cc.items = cc.items[:l+1]
		item = cc.items[l]
		if item == nil {
			item = &tickerTaskWrapper{client: client, when: when}
			cc.items[l] = item
		} else {
			item.client = client
			item.when = when
		}
	}
	if item == nil {
		item = &tickerTaskWrapper{client: client, when: when}
		cc.items = append(cc.items, item)
	}
	doAdd(cc, item)
	if cc.cleanStart.Load() == 0 {
		go func() {
			cc.startClen()
		}()
		cc.cleanStart.Store(1)
	}
	cc.mu.Unlock()
}

func (cc *TickerWorker) startClen() {
	for {
		select {
		case <-cc.t.C:
			cc.mu.Lock()
			now := nanoTime()
			pollUntil, _ := checkItems(cc, now)
			if pollUntil > 0 {
				du := pollUntil - now
				if du <= 0 {
					panic("never happened")
				}
				cc.t.Reset(time.Duration(du))
			}
			cc.mu.Unlock()
			if cc.stop.Load() {
				close(cc.cleanCh)
				return
			}
		case <-cc.stopChan:
			close(cc.cleanCh)
			return
		}
	}
}

func badHeap() {
	panic("client data corruption")
}

func siftUp(items []*tickerTaskWrapper, i int) int {
	if i >= len(items) {
		badHeap()
	}
	when := items[i].when
	if when <= 0 {
		badHeap()
	}
	tmp := items[i]
	for i > 0 {
		p := (i - 1) / 4 // parent
		if when >= items[p].when {
			break
		}
		items[i] = items[p]
		i = p
	}
	if tmp != items[i] {
		items[i] = tmp
	}
	return i
}

func siftDown(items []*tickerTaskWrapper, i int) {
	n := len(items)
	if i >= n {
		badHeap()
	}
	when := items[i].when
	if when <= 0 {
		badHeap()
	}
	tmp := items[i]
	for {
		c := i*4 + 1 // left child
		c3 := c + 2  // mid child
		if c >= n {
			break
		}
		w := items[c].when
		if c+1 < n && items[c+1].when < w {
			w = items[c+1].when
			c++
		}
		if c3 < n {
			w3 := items[c3].when
			if c3+1 < n && items[c3+1].when < w3 {
				w3 = items[c3+1].when
				c3++
			}
			if w3 < w {
				w = w3
				c = c3
			}
		}
		if w >= when {
			break
		}
		items[i] = items[c]
		i = c
	}
	if tmp != items[i] {
		items[i] = tmp
	}
}

func runOne(cc *TickerWorker, item *tickerTaskWrapper, now int64) {
	duration, stop := item.client.Clean()
	if duration < cc.minDuration {
		duration = cc.minDuration
	}
	item.when = int64(duration) + nanoTime()
	if stop {
		doDel0(cc)
		return
	}
	siftDown(cc.items, 0)
}

func run(cc *TickerWorker, now int64) int64 {
	t := cc.items[0]
	if t.when > now {
		return t.when
	}
	runOne(cc, t, now)
	return 0
}

func doDel0(cc *TickerWorker) {
	last := len(cc.items) - 1
	if last > 0 {
		cc.items[0], cc.items[last] = cc.items[last], cc.items[0]
	}
	cc.items[last].client = nil
	cc.items = cc.items[:last]
	if last > 0 {
		siftDown(cc.items, 0)
	}
	update0When(cc)
	cc.numItems.Add(math.MaxUint32)
}

func update0When(pp *TickerWorker) {
	if len(pp.items) == 0 {
		pp.zeroWhen.Store(0)
	} else {
		if pp.zeroWhen.Load() != pp.items[0].when {
			du := pp.items[0].when - nanoTime()
			pp.zeroWhen.Store(pp.items[0].when)
			if du < 0 {
				du = 0
			}
			pp.t.Reset(time.Duration(du))
		}
	}
}

func checkItems(cc *TickerWorker, now int64) (pollUntil int64, ran bool) {
	next := cc.zeroWhen.Load()
	if now < next {
		return next, false
	}
	if len(cc.items) > 0 {
		for len(cc.items) > 0 {
			if tw := run(cc, now); tw != 0 {
				if tw > 0 {
					pollUntil = tw
				}
				break
			}
			ran = true
		}
	}
	return
}

func doAdd(cc *TickerWorker, c *tickerTaskWrapper) {
	cc.numItems.Add(1)
	siftUp(cc.items, len(cc.items)-1)
	if c == cc.items[0] {
		update0When(cc)
	}
	return
}
