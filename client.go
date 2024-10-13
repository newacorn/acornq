package acornq

import (
	"context"
	"errors"
	"github.com/redis/rueidis"
	"github.com/zeebo/xxh3"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Client struct {
	broker *Broker
	mu     sync.Mutex
}

func NewClient(redisCli rueidis.Client) *Client {
	return &Client{
		broker: &Broker{redisCli: redisCli},
	}
}

const (
	// Default max Retry count used if nothing is specified.
	defaultMaxRetry = 25
	// Default Timeout used if both Timeout and deadline are not specified.
	defaultTimeout   = 30 * time.Minute
	defaultQueueName = "default"
)

var (
	ErrNilTask       = errors.New("task is nil")
	ErrEmptyTaskType = errors.New("task type is empty")
)

func (c *Client) EnqueueContext(ctx context.Context, task Tasker, opts ...Optioner) (err error) {
	if task == nil {
		err = ErrNilTask
		return
	}
	if strings.TrimSpace(task.TypeIdentifier()) == "" {
		err = ErrEmptyTaskType
		return
	}
	now := time.Now()
	o := option{
		retry:     defaultMaxRetry,
		queue:     defaultQueueName,
		deadline:  noDeadline,
		processAt: now,
	}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		err = opt.Set(&o)
		if err != nil {
			return
		}
	}
	var uniqueKey string
	if o.uniqueTTL > 0 {
		uniqueKey = createUniqueKey(task.TypeIdentifier(), task.Payload())
	}
	if len(o.taskID) == 0 {
		u := uuidBytes()
		o.taskID = b2s(u[:])
	}
	taskInfo := TaskInfo{
		ID:        StringBytes(o.taskID),
		Type:      StringBytes(task.TypeIdentifier()),
		Payload:   StringBytes(task.Payload()),
		Queue:     StringBytes(o.queue),
		UniqueKey: StringBytes(uniqueKey),
		Timeout:   int(o.timeout.Seconds()),
		StartAt:   o.processAt.Unix(),
		Retention: int(o.retention.Seconds()),
		Retry:     o.retry,
	}
	if o.deadline == noDeadline {
		taskInfo.Deadline = 0
	}
	if o.processAt.After(now) {
		taskInfo.State = Scheduled
	} else {
		o.processAt = now
		taskInfo.State = Pending
	}
	c.AddQueue(b2s(taskInfo.Queue))
	err = c.enqueueTask(ctx, &taskInfo)
	return
}

type option struct {
	retry     int
	queue     string
	taskID    string
	timeout   time.Duration
	deadline  time.Time
	uniqueTTL time.Duration
	processAt time.Time
	retention time.Duration
}

// ValidateQueueName validates a given qname to be used as a queue name.
// Returns nil if valid, otherwise returns non-nil error.
func validateQueueName(qname string) error {
	if len(strings.TrimSpace(qname)) == 0 {
		return errors.New("queue name must contain one or more characters")
	}
	return nil
}
func validTaskId(id string) (err error) {
	if strings.TrimSpace(id) == "" {
		err = errors.New("task ID cannot be empty")
	}
	return
}
func validUniqueTTL(d time.Duration) (err error) {
	if d < 1*time.Second {
		err = errors.New("unique TTL cannot be less than 1s")
	}
	return
}

func createUniqueKey(tasktype string, payload []byte) string {
	h := xxh3.New()
	_, _ = h.WriteString(tasktype)
	_, _ = h.Write(payload)
	return strconv.FormatUint(h.Sum64(), 10)
}

func (c *Client) enqueueTask(ctx context.Context, task *TaskInfo) (err error) {
	err = c.broker.EnqueueTasks(ctx, []*TaskInfo{task})
	return
}

func (c *Client) AddQueue(queue string) {
	c.mu.Lock()
	idx := slices.IndexFunc(c.broker.keyInfos, func(keyInfo *KeyInfo) bool { return keyInfo.queue == queue })
	if idx == -1 {
		c.broker.keyInfos = append(c.broker.keyInfos, NewKeyInfo(queue))
	}
	c.mu.Unlock()
}
