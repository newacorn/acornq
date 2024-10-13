package acornq

import "time"

type KeyInfo struct {
	queue          string
	taskKeyPrefix  string
	queueKeyPrefix string
	//
	pendingKey   string
	activeKey    string
	scheduledKey string
	retryKey     string
	//
	liveKey  string
	toDelKey string
	//
	successfulKey string
	failedKey     string
	//
	failedTotalKey        string
	processedTotalKey     string
	failedDayKeyPrefix    string
	processedDayKeyPrefix string
}

// task: acornq:{default}:t:{taskID}
//
// scheduled queue(sorted set): acornq:{default}:scheduled
// pending queue(set): acornq:{default}:pending
// active queue(set): acornq:{default}:active
// retry queue(sorted set): acornq:{default}:retry
//
// failed queue(sorted set): acornq:{default}:failed
// successful queue(sorted set): acornq:{default}:success
//
// failed total(int): acornq:{default}:failed
// processed total(int): acornq:{default}:processed
// failed day(int): acornq:{default}:failed:{day}
// processed day(int): acornq:{default}:processed:{day}

func NewKeyInfo(queue string) *KeyInfo {
	n := KeyInfo{queue: queue}
	//
	n.queueKeyPrefix = n.QueueKeyPrefix()
	n.taskKeyPrefix = n.queueKeyPrefix + "t:"
	//
	n.scheduledKey = n.queueKeyPrefix + "scheduled"
	n.pendingKey = n.queueKeyPrefix + "pending"
	n.activeKey = n.queueKeyPrefix + "active"
	n.retryKey = n.queueKeyPrefix + "retry"
	//
	n.liveKey = n.queueKeyPrefix + "live"
	n.toDelKey = n.queueKeyPrefix + "todel"
	//
	n.failedKey = n.queueKeyPrefix + "failed"
	n.successfulKey = n.queueKeyPrefix + "success"
	//
	n.processedTotalKey = n.queueKeyPrefix + "processed"
	n.failedTotalKey = n.queueKeyPrefix + "failed"
	n.processedDayKeyPrefix = n.queueKeyPrefix + "processed:"
	n.failedDayKeyPrefix = n.queueKeyPrefix + "failed:"
	//
	return &n
}

func (n *KeyInfo) QueueKeyPrefix() string {
	return "acornq:{" + n.queue + "}:"
}
func (n *KeyInfo) TaskKeyPrefix() string {
	return n.taskKeyPrefix
}
func (n *KeyInfo) TaskKey(id string) string {
	return n.taskKeyPrefix + id
}

func (n *KeyInfo) ScheduledKey() string {
	return n.scheduledKey
}
func (n *KeyInfo) PendingKey() string {
	return n.pendingKey
}
func (n *KeyInfo) ToDeleteKey() string {
	return n.toDelKey
}
func (n *KeyInfo) ActiveKey() string {
	return n.activeKey
}
func (n *KeyInfo) RetryKey() string {
	return n.retryKey
}

func (n *KeyInfo) LiveKey() string {
	return n.liveKey
}

func (n *KeyInfo) FailedKey() string {
	return n.failedKey
}
func (n *KeyInfo) SuccessfulKey() string {
	return n.successfulKey
}

func (n *KeyInfo) FailedTotalKey() string {
	return n.failedTotalKey
}
func (n *KeyInfo) ProcessedTotalKey() string {
	return n.processedTotalKey
}
func (n *KeyInfo) FailedDayKey(t time.Time) string {
	return n.failedDayKeyPrefix + t.UTC().Format("2006-01-02")
}
