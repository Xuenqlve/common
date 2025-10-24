package event

type Type int

const (
	SyncExceptionsPanicExit = 1000
)

type Event struct {
	Type  Type
	Key   string
	Value map[string]any
}

type ObserverFunc func(e Event)

type Subject interface {
	Register(observer ObserverFunc)
	Upload(event Event)
}
