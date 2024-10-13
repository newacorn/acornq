package acornq

import (
	"context"
	"errors"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/assert"
	"log"
	"testing"
	"time"
)

func TestMarshalTask(t *testing.T) {
	t1 := &TaskInfo{
		ID:        StringBytes("id"),
		Type:      StringBytes("type"),
		Payload:   StringBytes("payload"),
		Queue:     StringBytes("queue"),
		UniqueKey: StringBytes("key"),
	}
	b, err := MarshalTask(t1)
	assert.Nil(t, err)
	log.Println(string(b))
	t2, _ := unmarshalTask(b)
	log.Println(t2)
	_ = t1
}

func TestTemp(t *testing.T) {
	//log.Println(time.Time{}.Unix())
	t.Log(noDeadline.Unix())
}

func TestClient_EnqueueContext(t *testing.T) {
	redisCli := client()
	for i := 0; i < 10; i++ {
		var o Optioner
		var payload = []byte("payload")
		if i%2 == 0 {
			o = processInOption(time.Second * 60)
			payload = []byte("scheduled payload")
		}
		cli := NewClient(redisCli)
		err := cli.EnqueueContext(context.Background(), NewTask("task", payload), o, Retention(time.Second*120))
		assert.Nil(t, err)
	}

}

func TestServer(t *testing.T) {
	rueidis.RedisResult{}.ToMap()
	log.SetFlags(log.Lshortfile)
	broker := &Broker{
		redisCli: client(),
	}
	s, err := NewServer(&Config{
		Handler: TaskHandlerFunc(func(task *TaskInfo) error {
			log.Println(string(task.Payload))
			return nil
		}),
		Queues:           map[string]int{"default": 1},
		Broker:           broker,
		TaskPeekInterval: time.Second * 2,
	})
	assert.Nil(t, err)
	s.Start()
}

func TestWorker(t *testing.T) {
	log.SetFlags(log.Lshortfile)
	redisCli := client()
	broker := &Broker{
		redisCli: redisCli,
		keyInfos: []*KeyInfo{
			NewKeyInfo("default")},
	}
	s := &Server{
		errHandler: func(err error) {
			log.Println(err)
		},
		handler: TaskHandlerFunc(func(t *TaskInfo) (err error) {
			log.Println(string(t.Payload))
			return errors.New("tmp error")
		}),
		isFailure:      defaultIsFailureFunc,
		retryDelayFunc: defaultRetryDelayFunc,
	}
	w := Worker{
		queues:       []string{"default"},
		broker:       broker,
		s:            s,
		pollInterval: time.Second * 2,
	}
	w.exec()
}

func client() (cli rueidis.Client) {
	cli, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{"localhost:6380"}, DisableCache: true,
		ForceSingleClient: true,
		MaxFlushDelay:     20 * time.Microsecond,
	})
	if err != nil {
		log.Fatal(err)
	}
	return
}

func TestLog(t *testing.T) {
	defaultErrHandler(errors.New("tmp error"))
}
