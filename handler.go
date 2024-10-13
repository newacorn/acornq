package acornq

type TaskHandler interface {
	Handle(task *TaskInfo) error
}

type TaskHandlerFunc func(task *TaskInfo) error

func (fn TaskHandlerFunc) Handle(task *TaskInfo) error {
	return fn(task)
}
