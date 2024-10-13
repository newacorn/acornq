package acornq

import (
	"context"
	"github.com/redis/rueidis"
	"strconv"
	"time"
)

type Broker struct {
	keyInfos []*KeyInfo
	redisCli rueidis.Client
}

// PickTasks from pending set.
// 1. loop all queues.
// 2. move task from scheduled list to pending list.
// 3. move task from retry list to pending list.
// 4. move task from pending list to active list and return them.
func (b *Broker) PickTasks(ctx context.Context, queues []string, count int) (ts []*TaskInfo, err error) {
	var ts1 []*TaskInfo
	for _, queue := range queues {
		ts1, err = b.pickTasks(ctx, b.keyInfo(queue), count)
		// network error, break loop and return error
		if err != nil {
			return
		}
		if len(ts1) == 0 {
			continue
		}
		ts = append(ts, ts1...)
		count = count - len(ts1)
		if count == 0 {
			return
		}
	}
	return
}

// only return network error, other err convert to nil.
func (b *Broker) pickTasks(ctx context.Context, keyInfo *KeyInfo, count int) (ts []*TaskInfo, err error) {
	keys := []string{keyInfo.PendingKey(), keyInfo.ActiveKey(), keyInfo.ScheduledKey(), keyInfo.RetryKey()}
	resp := pickTasksLs.Exec(ctx, b.redisCli, keys, []string{strconv.Itoa(count), strconv.Itoa(int(Pending)), strconv.Itoa(int(Active))})
	arr, err := resp.ToArray()
	if len(arr) == 0 {
		return
	}
	err = nil
	ts = make([]*TaskInfo, 0, len(arr))
	for _, v := range arr {
		str, err1 := v.ToString()
		if err1 != nil {
			continue
		}
		t, err1 := unmarshalTask(s2b(str))
		if err1 != nil {
			continue
		}
		ts = append(ts, t)
	}
	return
}

// RecoveryTasks move zombies tasks from active list to pending list.
// clear zombie tasks from live list.
//
// 1. idle 30 seconds
//
// 2. task in active list not int live sorted set for 30 seconds.
// 3. task in live sorted set and in active list max time compare now for 30 seconds.
// 4. move 2 and 3 tasks to pending list.
//
// 5. delete tasks only in live sorted set but not in active list.
func (b *Broker) RecoveryTasks(queues []string, idleTimeout time.Duration) (err error) {
	idleTimeoutStr := strconv.Itoa(int(idleTimeout.Seconds()))
	for _, queue := range queues {
		keyInfo := b.keyInfo(queue)
		err = b.recoveryTasks(context.Background(), keyInfo, idleTimeoutStr)
		if err != nil {
			return
		}
	}
	return
}

func (b *Broker) recoveryTasks(ctx context.Context, keyInfo *KeyInfo, idleTimeout string) (err error) {
	err = recoveryLs.Exec(ctx, b.redisCli, []string{keyInfo.ActiveKey(), keyInfo.LiveKey(), keyInfo.PendingKey()}, []string{idleTimeout, strconv.Itoa(int(Pending))}).Error()
	//goland:noinspection GoDirectComparisonOfErrors
	if err == rueidis.Nil {
		err = nil
	}
	return
}

// LiveTasksChange add or update scores in live sorted set.
func (b *Broker) LiveTasksChange(ctx context.Context, items []*liveItem, update bool) (err error) {
	if len(items) > 1 {
		m := map[string][]*liveItem{}
		for _, item := range items {
			m[item.queue] = append(m[item.queue], item)
		}
		for queue, items2 := range m {
			keyInfo := b.keyInfo(queue)
			err = b.liveTasksChange(ctx, keyInfo, items2, update)
			if err != nil {
				return
			}
		}
		return
	}
	item := items[0]
	keyInfo := b.keyInfo(item.queue)
	if update {
		err = b.redisCli.Do(ctx, b.redisCli.B().Zadd().Key(keyInfo.LiveKey()).Xx().ScoreMember().ScoreMember(float64(time.Now().Unix()), keyInfo.TaskKey(item.taskID)).Build()).Error()
	} else {
		err = b.redisCli.Do(ctx, b.redisCli.B().Zadd().Key(keyInfo.LiveKey()).Nx().ScoreMember().ScoreMember(float64(time.Now().Unix()), keyInfo.TaskKey(item.taskID)).Build()).Error()
	}
	if //goland:noinspection GoDirectComparisonOfErrors
	err == rueidis.Nil {
		err = nil
	}
	return
}

func (b *Broker) liveTasksChange(ctx context.Context, keyInfo *KeyInfo, items []*liveItem, update bool) (err error) {
	nowStr := strconv.FormatInt(time.Now().Unix(), 10)
	args := make([]string, 0, len(items)*2+1)
	if update {
		args[0] = "XX"
	} else {
		args[0] = "NX"
	}
	args1 := args[1 : len(items)+1]
	j := 0
	for _, item := range items {
		args1[j] = nowStr
		args1[j+1] = keyInfo.TaskKey(item.taskID)
		j += 2
	}
	err = b.redisCli.Do(ctx, b.redisCli.B().Arbitrary("ZADD").Keys(keyInfo.LiveKey()).Args(args...).Build()).Error()
	if //goland:noinspection GoDirectComparisonOfErrors
	err == rueidis.Nil {
		err = nil
	}
	return
}

// DeleteLiveTasks delete tasks from live sorted set.
func (b *Broker) DeleteLiveTasks(ctx context.Context, items []*liveItem) (err error) {
	if len(items) > 1 {
		m := map[string][]*liveItem{}
		for _, item := range items {
			m[item.queue] = append(m[item.queue], item)
		}
		for queue, items2 := range m {
			keyInfo := b.keyInfo(queue)
			err = b.deleteLiveTasks(ctx, keyInfo, items2)
			if err != nil {
				return
			}
		}
		return
	}
	item := items[0]
	keyInfo := b.keyInfo(item.queue)
	err = b.redisCli.Do(ctx, b.redisCli.B().Zrem().Key(keyInfo.LiveKey()).Member(keyInfo.TaskKey(item.taskID)).Build()).Error()
	if //goland:noinspection GoDirectComparisonOfErrors
	err == rueidis.Nil {
		err = nil
	}
	return
}

func (b *Broker) deleteLiveTasks(ctx context.Context, keyInfo *KeyInfo, items []*liveItem) (err error) {
	members := make([]string, len(items))
	for i, item := range items {
		members[i] = keyInfo.TaskKey(item.taskID)
	}
	err = b.redisCli.Do(ctx, b.redisCli.B().Zrem().Key(keyInfo.LiveKey()).Member(members...).Build()).Error()
	if //goland:noinspection GoDirectComparisonOfErrors
	err == rueidis.Nil {
		err = nil
	}
	return
}

// EnqueueTasks add tasks to pending list or scheduled sorted set.
func (b *Broker) EnqueueTasks(ctx context.Context, ts []*TaskInfo) (err error) {
	now := time.Now().Unix()
	if len(ts) > 1 {
		pending := map[string][]*TaskInfo{}
		scheduled := map[string][]*TaskInfo{}
		for _, t := range ts {
			if t.Scheduled(now) {
				scheduled[b2s(t.Queue)] = append(scheduled[b2s(t.Queue)], t)
				continue
			}
			pending[b2s(t.Queue)] = append(pending[b2s(t.Queue)], t)
		}
		if len(pending) > 0 {
			err = b.enqueueTasks(ctx, pending, false)
			if err != nil {
				return
			}
		}
		if len(scheduled) > 0 {
			err = b.enqueueTasks(ctx, scheduled, true)
		}
		return
	}
	t := ts[0]
	b1, err := MarshalTask(t)
	if err != nil {
		return
	}
	keyInfo := b.keyInfo(b2s(t.Queue))
	if t.Scheduled(now) {
		return enqueueScheduledLs.Exec(ctx, b.redisCli, []string{keyInfo.ScheduledKey(), keyInfo.TaskKey(b2s(t.ID))}, []string{b2s(b1)}).Error()
	}
	return enqueuePendingLs.Exec(ctx, b.redisCli, []string{keyInfo.PendingKey(), keyInfo.TaskKey(b2s(t.ID))}, []string{b2s(b1)}).Error()
}

func (b *Broker) enqueueTasks(ctx context.Context, queue2ts map[string][]*TaskInfo, scheduled bool) (err error) {
	ls := enqueuePendingLs
	if scheduled {
		ls = enqueueScheduledLs
	}
	for queue, tasks := range queue2ts {
		keyInfo := b.keyInfo(queue)
		keys, argv := make([]string, len(tasks)+1), make([]string, len(tasks))
		if scheduled {
			keys[0] = b.keyInfo(queue).ScheduledKey()
		} else {
			keys[0] = b.keyInfo(queue).PendingKey()
		}
		keys2 := keys[1:]
		for i, t := range tasks {
			keys2[i] = keyInfo.TaskKey(b2s(t.ID))
			b1, err1 := MarshalTask(t)
			if err1 != nil {
				err = err1
				return
			}
			argv[i] = b2s(b1)
		}
		err = ls.Exec(ctx, b.redisCli, keys, argv).Error()
	}
	return
}

// RetryTasks remove ts from active list and add tasks to retry sorted set conditional.
func (b *Broker) RetryTasks(ctx context.Context, ts []*TaskInfo) (err error) {
	if len(ts) > 1 {
		m := map[string][]*TaskInfo{}
		for _, t := range ts {
			m[b2s(t.Queue)] = append(m[b2s(t.Queue)], t)
		}
		for queue, tasks := range m {
			err = b.retryTasks(ctx, b.keyInfo(queue), tasks)
			if err != nil {
				return
			}
		}
		return
	}
	t := ts[0]
	keyInfo := b.keyInfo(b2s(t.Queue))
	err = b.retryTasks(ctx, keyInfo, []*TaskInfo{t})
	return
}

/*
	func (b *Broker) retryTasks(ctx context.Context, keyInfo *KeyInfo, ts []*TaskInfo, isFailure bool) (err error) {
		keys := make([]string, len(ts)+2)
		args := make([]string, len(ts)+2)
		keys[0] = keyInfo.RetryKey()
		keys[1] = keyInfo.ActiveKey()
		args[0] = strconv.Itoa(int(Retried))
		keys2 := keys[2:]
		args2 := args[1:]
		for i, t := range ts {
			keys2[i] = keyInfo.TaskKey(b2s(t.ID))
			args2[i] = strconv.FormatUint(uint64(t.PendingAt), 10)
		}
		err = retryTasksLs.Exec(ctx, b.redisCli, keys, args).Error()
		if //goland:noinspection GoDirectComparisonOfErrors
		err == rueidis.Nil {
			err = nil
		}
		return
	}
*/
func (b *Broker) retryTasks(ctx context.Context, keyInfo *KeyInfo, ts []*TaskInfo) (err error) {
	keys := make([]string, len(ts)+2)
	args := make([]string, len(ts)*2+1)
	keys[0] = keyInfo.RetryKey()
	keys[1] = keyInfo.ActiveKey()
	args[0] = strconv.Itoa(int(Retried))
	keys2 := keys[2:]
	args2 := args[1:]
	j := 0
	for i, t := range ts {
		keys2[i] = keyInfo.TaskKey(b2s(t.ID))
		args2[j] = strconv.FormatUint(uint64(t.PendingAt), 10)
		args2[j+1] = strconv.Itoa(t.Retried)
		j += 2
	}
	err = retryTasksLs.Exec(ctx, b.redisCli, keys, args).Error()
	if //goland:noinspection GoDirectComparisonOfErrors
	err == rueidis.Nil {
		err = nil
	}
	return
}

func (b *Broker) keyInfo(queue string) (keyInfo *KeyInfo) {
	for i := 0; i < len(b.keyInfos); i++ {
		if b.keyInfos[i].queue == queue {
			return b.keyInfos[i]
		}
	}
	return
}

func (b *Broker) Active2Pending(ctx context.Context, ts []*TaskInfo) (err error) {
	if len(ts) > 1 {
		m := map[string][]*TaskInfo{}
		for _, t := range ts {
			m[b2s(t.Queue)] = append(m[b2s(t.Queue)], t)
		}
		for queue, tasks := range m {
			err = b.active2pending(ctx, b.keyInfo(queue), tasks)
			if err != nil {
				return
			}
		}
		return
	}
	t := ts[0]
	keyInfo := b.keyInfo(b2s(t.Queue))
	return b.active2pending(ctx, keyInfo, []*TaskInfo{t})
}

func (b *Broker) active2pending(ctx context.Context, keyInfo *KeyInfo, ts []*TaskInfo) (err error) {
	keys := make([]string, len(ts)+2)
	keys[0] = keyInfo.PendingKey()
	keys[1] = keyInfo.ActiveKey()
	keys2 := keys[2:]
	for i, t := range ts {
		keys2[i] = keyInfo.TaskKey(b2s(t.ID))
	}
	err = active2pendingLs.Exec(ctx, b.redisCli, keys, nil).Error()
	if //goland:noinspection GoDirectComparisonOfErrors
	err == rueidis.Nil {
		err = nil
	}
	return
}

func (b *Broker) Active2Archive(ctx context.Context, ts []*TaskInfo, successful bool) (err error) {
	if len(ts) > 1 {
		m := map[string][]*TaskInfo{}
		for _, t := range ts {
			m[b2s(t.Queue)] = append(m[b2s(t.Queue)], t)
		}
		for queue, tasks := range m {
			err = b.active2Archive(ctx, b.keyInfo(queue), tasks, successful)
			if err != nil {
				return
			}
		}
		return
	}
	t := ts[0]
	keyInfo := b.keyInfo(b2s(t.Queue))
	return b.active2Archive(ctx, keyInfo, []*TaskInfo{t}, successful)
}

func (b *Broker) active2Archive(ctx context.Context, keyInfo *KeyInfo, ts []*TaskInfo, successful bool) (err error) {
	keys := make([]string, len(ts)+3)
	args := make([]string, len(ts)+1)
	if successful {
		keys[0] = keyInfo.SuccessfulKey()
	} else {
		keys[0] = keyInfo.FailedKey()
	}
	keys[1] = keyInfo.ActiveKey()
	keys[2] = keyInfo.ToDeleteKey()
	state := Archived
	if successful {
		state |= Successful
	} else {
		state |= Failed
	}
	args[0] = strconv.Itoa(int(state))
	keys2 := keys[3:]
	args2 := args[1:]
	for i, t := range ts {
		keys2[i] = keyInfo.TaskKey(b2s(t.ID))
		args2[i] = strconv.Itoa(t.Retention)
	}
	err = active2ArchiveLs.Exec(ctx, b.redisCli, keys, args).Error()
	if //goland:noinspection GoDirectComparisonOfErrors
	err == rueidis.Nil {
		err = nil
	}
	return
}

func (b *Broker) SetErrorMsg(ctx context.Context, t *TaskInfo) (err error) {
	keyInfo := b.keyInfo(b2s(t.Queue))
	err = b.redisCli.Do(ctx, b.redisCli.B().JsonSet().Key(keyInfo.TaskKey(b2s(t.ID))).Path("$.error_msg").Value(`"`+b2s(t.ErrorMsg)+`"`).Build()).Error()
	return
}

func (b *Broker) CleanUpArchive(ctx context.Context, batchLen int) (err error) {
	// clean zombie keys in archive list(successful and failed)
	keys := make([]string, len(b.keyInfos)*3)
	for i := 0; i < len(b.keyInfos); i++ {
		j := i * 3
		keys[j] = b.keyInfos[i].ToDeleteKey()
		keys[j+1] = b.keyInfos[i].SuccessfulKey()
		keys[j+2] = b.keyInfos[i].FailedKey()
	}
	var nextStartPos int
	for {
		resp := cleanerLs.Exec(ctx, b.redisCli, keys, []string{
			strconv.Itoa(nextStartPos), strconv.Itoa(nextStartPos + batchLen - 1),
		})
		if resp.Error() != nil {
			return resp.Error()
		}
		v, er := resp.ToInt64()
		if er != nil {
			err = er
			return
		}
		if v == 0 {
			return
		}
		nextStartPos = int(v)
	}
}
