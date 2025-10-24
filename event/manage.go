package event

import (
	"fmt"
	"sync"
)

var EventAdmin EventManage

type EventManage struct {
	mu       sync.Mutex
	Observer map[Type][]ObserverFunc
}

func (e *EventManage) Init() {
	e.Observer = map[Type][]ObserverFunc{}
}

func (e *EventManage) Register(et Type, observer ObserverFunc) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.Observer[et]; !ok {
		e.Observer[et] = []ObserverFunc{}
	}
	e.Observer[et] = append(e.Observer[et], observer)
}

func (e *EventManage) Upload(event Event) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if observer, ok := e.Observer[event.Type]; ok {
		for _, fn := range observer {
			go fn(event)
		}
	}
}

const Error = "error"

const (
	PaincLevel   = "panic"
	ErrorLevel   = "error"
	WarningLevel = "warning"
)

func ErrorEvent(t Type, err error) Event {
	return Event{
		Type:  t,
		Value: errorValue(err),
	}
}

func errorValue(err error) map[string]any {
	return map[string]any{
		Error: err.Error(),
	}
}

func ValueError(data map[string]any) error {
	if data == nil {
		return nil
	}
	if err, ok := data[Error]; ok {
		return fmt.Errorf("%v", err)
	}
	return nil
}
