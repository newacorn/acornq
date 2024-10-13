package acornq

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// TaskState denotes the state of a task.
type TaskState int

// Active
// Pending
// Scheduled
// Archived | Completed
// Archived | Failed
const (
	Active TaskState = 1 << iota
	Scheduled
	Pending
	Failed
	Retried
	Successful
	Archived
)

type StringBytes []byte

func (s *StringBytes) UnmarshalJSON(b []byte) error {
	if len(b) > 2 {
		*s = append(*s, b[1:len(b)-1]...)
	}
	return nil
}

func (s *StringBytes) MarshalJSON() ([]byte, error) {
	l := len(*s)
	b := make([]byte, l+2)
	b[0] = '"'
	b[l+1] = '"'
	copy(b[1:l+1], *s)
	b[l+1] = '"'
	return b, nil
}

type Task struct {
	typeName string
	payload  []byte
	opts     []Optioner
}

func NewTask(typeName string, payload []byte, opts ...Optioner) Task {
	return Task{
		typeName: typeName,
		payload:  payload,
		opts:     opts,
	}
}

func (t Task) TypeIdentifier() string {
	return t.typeName
}
func (t Task) Payload() []byte {
	return t.payload
}

type Tasker interface {
	TypeIdentifier() string
	Payload() []byte
}

type TaskInfo struct {
	// task unique id
	ID StringBytes `json:"id"`
	// task type pick handler by this field.
	Type StringBytes `json:"type"`
	// task payload
	Payload StringBytes `json:"payload"`
	// queue name such as : default
	Queue StringBytes `json:"queue"`
	// unique key
	UniqueKey StringBytes `json:"unique_key,omitempty"`
	// cache task handle error message
	ErrorMsg StringBytes `json:"error_msg,omitempty"`
	// task state
	State TaskState `json:"state,omitempty"`
	// total retry count, first execution does not take into account
	Retry int `json:"retry,omitempty"`
	// had been retried count
	Retried int `json:"retried,omitempty"`
	// task execution timeout
	Timeout int `json:"timeout,omitempty"`
	// task deadline
	Deadline int64 `json:"deadline,omitempty"`
	// successful task keep until after success
	Retention int `json:"retention,omitempty"`
	// task first enter pending queue
	StartAt int64 `json:"start_at,omitempty"`
	// last failed at
	LastFailedAt int64 `json:"last_failed_at,omitempty"`
	// every enter pending time
	PendingAt int64 `json:"pending_at,omitempty"`
	// successful at or last failed at
	CompletedAt int64 `json:"completed_at,omitempty"`
}

func MarshalTask(t *TaskInfo) ([]byte, error) {
	return json.Marshal(t)
}

func unmarshalTask(b []byte) (*TaskInfo, error) {
	var t TaskInfo
	if err := json.Unmarshal(b, &t); err != nil {
		return nil, err
	}
	return &t, nil
}

func (ti *TaskInfo) Scheduled(now int64) bool {
	return ti.StartAt > now
}

type OptionType int

const (
	MaxRetryOpt OptionType = iota
	QueueOpt
	TimeoutOpt
	DeadlineOpt
	UniqueOpt
	ProcessAtOpt
	ProcessInOpt
	TaskIDOpt
	RetentionOpt
)

// Optioner specifies the task processing behavior.
type Optioner interface {
	// String returns a string representation of the Option.
	String() string

	// Type describes the type of the Option.
	Type() OptionType

	// Value returns a value used to create this Option.
	Value() interface{}
	Set(o *option) (err error)
}

// Internal Option representations.
type (
	retryOption     int
	queueOption     string
	taskIDOption    string
	timeoutOption   time.Duration
	deadlineOption  time.Time
	uniqueOption    time.Duration
	processAtOption time.Time
	processInOption time.Duration
	retentionOption time.Duration
)

// MaxRetry returns an Option to specify the max number of times
// the task will be retried.
//
// Negative retry count is treated as zero retry.
func MaxRetry(n int) Optioner {
	if n < 0 {
		n = 0
	}
	return retryOption(n)
}

func (n retryOption) String() string     { return fmt.Sprintf("MaxRetry(%d)", int(n)) }
func (n retryOption) Type() OptionType   { return MaxRetryOpt }
func (n retryOption) Value() interface{} { return int(n) }
func (n retryOption) Set(o *option) (err error) {
	o.retry = int(n)
	return
}

// Queue returns an Option to specify the queue to enqueue the task into.
func Queue(name string) Optioner {
	return queueOption(name)
}

func (name queueOption) String() string     { return fmt.Sprintf("Queue(%q)", string(name)) }
func (name queueOption) Type() OptionType   { return QueueOpt }
func (name queueOption) Value() interface{} { return string(name) }
func (name queueOption) Set(o *option) (err error) {
	err = validateQueueName(string(name))
	o.queue = string(name)
	return
}

// TaskID returns an Option to specify the task ID.
func TaskID(id string) Optioner {
	return taskIDOption(id)
}

func (id taskIDOption) String() string     { return fmt.Sprintf("TaskID(%q)", string(id)) }
func (id taskIDOption) Type() OptionType   { return TaskIDOpt }
func (id taskIDOption) Value() interface{} { return string(id) }
func (id taskIDOption) Set(o *option) (err error) {
	err = validTaskId(string(id))
	o.taskID = string(id)
	return
}

// Timeout returns an Option to specify how long a task may run.
// If the timeout elapses before the Handler returns, then the task
// will be retried.
//
// Zero duration means no limit.
//
// If there's a conflicting Deadline Option, whichever comes earliest
// will be used.
func Timeout(d time.Duration) Optioner {
	return timeoutOption(d)
}

func (d timeoutOption) String() string     { return fmt.Sprintf("Timeout(%v)", time.Duration(d)) }
func (d timeoutOption) Type() OptionType   { return TimeoutOpt }
func (d timeoutOption) Value() interface{} { return time.Duration(d) }

func (d timeoutOption) Set(o *option) (err error) {
	o.timeout = time.Duration(d)
	return
}

// Deadline returns an Option to specify the deadline for the given task.
// If it reaches the deadline before the Handler returns, then the task
// will be retried.
//
// If there's a conflicting Timeout Option, whichever comes earliest
// will be used.
func Deadline(t time.Time) Optioner {
	return deadlineOption(t)
}

func (t deadlineOption) String() string {
	return fmt.Sprintf("Deadline(%v)", time.Time(t).Format(time.UnixDate))
}
func (t deadlineOption) Type() OptionType   { return DeadlineOpt }
func (t deadlineOption) Value() interface{} { return time.Time(t) }

func (t deadlineOption) Set(o *option) (err error) {
	o.deadline = time.Time(t)
	return
}

// Unique returns an Option to enqueue a task only if the given task is unique.
// TaskInfo enqueued with this Option is guaranteed to be unique within the given ttl.
// Once the task gets processed successfully or once the TTL has expired,
// another task with the same uniqueness may be enqueued.
// ErrDuplicateTask error is returned when enqueueing a duplicate task.
// TTL duration must be greater than or equal to 1 second.
//
// Uniqueness of a task is based on the following properties:
//   - TaskInfo Type
//   - TaskInfo Payload
//   - Queue Name
func Unique(ttl time.Duration) Optioner {
	return uniqueOption(ttl)
}

func (ttl uniqueOption) String() string     { return fmt.Sprintf("Unique(%v)", time.Duration(ttl)) }
func (ttl uniqueOption) Type() OptionType   { return UniqueOpt }
func (ttl uniqueOption) Value() interface{} { return time.Duration(ttl) }

func (ttl uniqueOption) Set(o *option) (err error) {
	err = validUniqueTTL(time.Duration(ttl))
	o.uniqueTTL = time.Duration(ttl)
	return
}

// ProcessAt returns an Option to specify when to process the given task.
//
// If there's a conflicting ProcessIn Option, the last Option passed to Enqueue overrides the others.
func ProcessAt(t time.Time) Optioner {
	return processAtOption(t)
}

func (t processAtOption) String() string {
	return fmt.Sprintf("StartAt(%v)", time.Time(t).Format(time.UnixDate))
}
func (t processAtOption) Type() OptionType   { return ProcessAtOpt }
func (t processAtOption) Value() interface{} { return time.Time(t) }
func (t processAtOption) Set(o *option) (err error) {
	o.processAt = time.Time(t)
	return
}

// ProcessIn returns an Option to specify when to process the given task relative to the current time.
//
// If there's a conflicting ProcessAt Option, the last Option passed to Enqueue overrides the others.
func ProcessIn(d time.Duration) Optioner {
	return processInOption(d)
}

func (d processInOption) String() string     { return fmt.Sprintf("ProcessIn(%v)", time.Duration(d)) }
func (d processInOption) Type() OptionType   { return ProcessInOpt }
func (d processInOption) Value() interface{} { return time.Duration(d) }
func (d processInOption) Set(o *option) (err error) {
	o.processAt = time.Now().Add(time.Duration(d))
	return
}

// Retention returns an Option to specify the duration of retention period for the task.
// If this Option is provided, the task will be stored as a completed task after successful processing.
// A completed task will be deleted after the specified duration elapses.
func Retention(d time.Duration) Optioner {
	return retentionOption(d)
}

func (ttl retentionOption) String() string     { return fmt.Sprintf("Retention(%v)", time.Duration(ttl)) }
func (ttl retentionOption) Type() OptionType   { return RetentionOpt }
func (ttl retentionOption) Value() interface{} { return time.Duration(ttl) }

func (ttl retentionOption) Set(o *option) (err error) {
	o.retention = time.Duration(ttl)
	return
}

// ErrDuplicateTask indicates that the given task could not be enqueued since it's a duplicate of another task.
//
// ErrDuplicateTask error only applies to tasks enqueued with a Unique Option.
var ErrDuplicateTask = errors.New("task already exists")

// ErrTaskIDConflict indicates that the given task could not be enqueued since its task ID already exists.
//
// ErrTaskIDConflict error only applies to tasks enqueued with a TaskID Option.
var ErrTaskIDConflict = errors.New("task ID conflicts with another task")
var noDeadline = time.Unix(0, 0)
