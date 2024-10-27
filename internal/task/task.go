package task

import (
	"sync"
	"sync/atomic"
	"time"
)

type TaskFunc = func(cancelTask *atomic.Bool)

type TaskHandle struct {
	cancel       atomic.Bool
	taskCanceled sync.WaitGroup
}

func Start(taskFunc TaskFunc) *TaskHandle {
	taskHandle := &TaskHandle{
		cancel:       atomic.Bool{},
		taskCanceled: sync.WaitGroup{},
	}
	taskHandle.taskCanceled.Add(1)
	go func() {
		taskFunc(&taskHandle.cancel)
		taskHandle.taskCanceled.Done()
	}()
	return taskHandle
}

func (th *TaskHandle) IsCancelled() bool {
	return th.cancel.Load()
}

func (th *TaskHandle) Cancel() {
	th.cancel.Store(true)
}

func (th *TaskHandle) Join() {
	th.taskCanceled.Wait()
}

func (th *TaskHandle) JoinWithTimeout(timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		th.taskCanceled.Wait()
	}()
	select {
	case <-c:
		return false
	case <-time.After(timeout):
		return true
	}
}
