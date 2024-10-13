package acornq

import (
	"context"
	"errors"
	"golang.org/x/sys/unix"
	"log"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

var (
	defaultCleanerInterval   = time.Minute
	defaultRecoverInterval   = time.Minute
	defaultTaskPeekInterval  = time.Second
	defaultWorkerConcurrency = 10
	defaultQueues            = map[string]int{"default": 0}
)
var ErrEmptyHandler = errors.New("task handler is empty")
var ErrNilServerConfig = errors.New("server config is nil")
var SkipRetry = errors.New("skip retry for the task")
var ErrNilBroker = errors.New("broker is nil")

type ErrHandler func(err error)
type RetryDelayFunc func(n int, e error, t *TaskInfo) time.Duration
type Server struct {
	// task handler
	handler    TaskHandler
	errHandler ErrHandler
	// max worker count
	concurrency int
	// queue name to priority
	queues         map[string]int
	retryDelayFunc RetryDelayFunc
	// queue arrange fixed
	queuesStrict bool
	broker       *Broker
	// notify component exit
	stop atomic.Int32
	// notify component exit
	stopCh chan struct{}
	// notify self exit
	shutDown         chan struct{}
	mu               sync.Mutex
	wg               sync.WaitGroup
	ws               []*Worker
	keysInfos        []*KeyInfo
	isFailure        func(err error) bool
	r                *recovery
	h                *heartBeatWorker
	c                *Cleaner
	taskPeekInterval time.Duration
	recoverInterval  time.Duration
	cleanerInterval  time.Duration
}
type Config struct {
	// task handler
	Handler TaskHandler
	// max worker count
	Concurrency int
	// queue name to priority
	Queues map[string]int
	// queue arrange fixed
	QueuesStrict     bool
	RetryDelayFunc   RetryDelayFunc
	IsFailure        func(err error) bool
	TaskPeekInterval time.Duration
	CleanerInterval  time.Duration
	RecoveryInterval time.Duration
	ErrHandler       ErrHandler
	Broker           *Broker
}

func NewServer(cfg *Config) (s *Server, err error) {
	err = patchConfig(cfg)
	if err != nil {
		return
	}
	stopCh := make(chan struct{})
	s = &Server{
		handler:          cfg.Handler,
		concurrency:      cfg.Concurrency,
		queues:           cfg.Queues,
		queuesStrict:     cfg.QueuesStrict,
		retryDelayFunc:   cfg.RetryDelayFunc,
		isFailure:        cfg.IsFailure,
		stopCh:           stopCh,
		shutDown:         make(chan struct{}),
		errHandler:       cfg.ErrHandler,
		broker:           cfg.Broker,
		cleanerInterval:  cfg.CleanerInterval,
		recoverInterval:  cfg.RecoveryInterval,
		taskPeekInterval: cfg.TaskPeekInterval,
	}
	s.createKeyInfos()
	s.broker.keyInfos = s.keysInfos
	s.r = newRecovery(stopCh, s.broker, s.queueNames(true), s.recoverInterval, s.errHandler)
	s.h = newHeartBeatWorker(stopCh, nil, s.broker)
	s.c = NewCleaner(s.broker, s.cleanerInterval, s.errHandler)
	return
}

func patchConfig(cfg *Config) (err error) {
	if cfg == nil {
		return ErrNilServerConfig
	}
	if cfg.Handler == nil {
		return ErrEmptyHandler
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = defaultWorkerConcurrency
	}
	if cfg.Queues == nil {
		cfg.Queues = defaultQueues
	}
	if cfg.RetryDelayFunc == nil {
		cfg.RetryDelayFunc = defaultRetryDelayFunc
	}
	if cfg.IsFailure == nil {
		cfg.IsFailure = defaultIsFailureFunc
	}
	if cfg.TaskPeekInterval == 0 {
		cfg.TaskPeekInterval = defaultTaskPeekInterval
	}
	if cfg.CleanerInterval == 0 {
		cfg.CleanerInterval = defaultCleanerInterval
	}
	if cfg.RecoveryInterval == 0 {
		cfg.RecoveryInterval = defaultRecoverInterval
	}
	if cfg.ErrHandler == nil {
		cfg.ErrHandler = func(err error) {
			log.Println(err)
		}
	}
	if cfg.Broker == nil {
		err = ErrNilBroker
	}
	return
}

func (s *Server) Start() {
	s.wg.Add(s.concurrency)
	s.ws = make([]*Worker, s.concurrency)
	beatItemCh := make(chan *liveItem)
	for i := 0; i < s.concurrency; i++ {
		w := &Worker{
			s:            s,
			queues:       s.queueNames(false),
			broker:       s.broker,
			queuesStrict: s.queuesStrict,
			beatItemCh:   beatItemCh,
			pollInterval: s.taskPeekInterval,
		}
		s.ws[i] = w
		go func() {
			defer func() {
				s.wg.Done()
			}()
			w.exec()
		}()
	}
	// recovery worker
	s.wg.Add(1)
	go func() {
		defer func() {
			s.wg.Done()
		}()
		s.r.Start()
	}()
	// cleaner worker
	s.wg.Add(1)
	go func() {
		defer func() {
			s.wg.Done()
		}()
		s.c.Start()
	}()
	// live check worker
	s.h.beatItemCh = beatItemCh
	go func() {
		defer func() {
			s.wg.Done()
		}()
		s.h.Start()
	}()
	outputInfo(s)
	s.waitForSignals()
	<-s.shutDown
}

func (s *Server) ShutDown(ctx context.Context) {
	s.mu.Lock()
	if s.stop.Load() == 0 {
		close(s.stopCh)
		s.stop.Store(1)
	}
	s.mu.Unlock()
	if ctx == context.Background() {
		s.wg.Wait()
		close(s.shutDown)
		return
	}
	<-ctx.Done()
	close(s.shutDown)
	return
}

func (s *Server) queueNames(customStrict bool) (queues []string) {
	if s.queuesStrict || customStrict || len(s.queues) == 1 {
		queues = make([]string, len(s.queues))
		i := 0
		for k := range s.queues {
			queues[i] = k
			i++
		}
		sort.Slice(queues, func(i, j int) bool {
			return s.queues[queues[i]] > s.queues[queues[j]]
		})
		return
	}
	for k, v := range s.queues {
		for i := 0; i < v; i++ {
			queues = append(queues, k)
		}
	}
	return
}

func (s *Server) createKeyInfos() {
	s.keysInfos = make([]*KeyInfo, len(s.queues))
	i := 0
	for k, _ := range s.queues {
		s.keysInfos[i] = NewKeyInfo(k)
		i++
	}
}

func (s *Server) KeyInfo(queue string) (keyInfo *KeyInfo) {
	for i := 0; i < len(s.keysInfos); i++ {
		if s.keysInfos[i].queue == queue {
			return s.keysInfos[i]
		}
	}
	return
}

// waitForSignals waits for signals and handles them.
// It handles SIGTERM, SIGINT, and SIGTSTP.
// SIGTERM and SIGINT will signal the process to exit.
// SIGTSTP will signal the process to stop processing new tasks.
func (s *Server) waitForSignals() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, unix.SIGTERM)
	<-sigs
	s.ShutDown(context.Background())
}

// defaultRetryDelayFunc is the default RetryDelayFunc used if one is not specified in Config.
// It uses exponential back-off strategy to calculate the retry delay.
func defaultRetryDelayFunc(n int, _ error, _ *TaskInfo) time.Duration {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	// Formula taken from https://github.com/mperham/sidekiq.
	s := int(math.Pow(float64(n), 4)) + 15 + (r.Intn(30) * (n + 1))
	return time.Duration(s) * time.Second
}

func defaultIsFailureFunc(err error) bool { return err != nil }

func defaultErrHandler(err error) {
	LogStack("acornq", err.Error(), 2, os.Stderr, true)
}
