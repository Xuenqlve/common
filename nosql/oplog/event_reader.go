package oplog

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xuenqlve/common/errors"
	"github.com/xuenqlve/common/event"
	"github.com/xuenqlve/common/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

var (
	//CollectionCappedError = errors.New("collection capped error")
	TimeoutError = errors.New("read next log timeout, It shouldn't be happen")
)

type EventReader struct {
	ctx        context.Context
	cancelFunc context.CancelFunc
	client     *mongo.ChangeStream

	cfg                  ReaderConfig
	startAtOperationTime interface{}

	fetcherExist bool
	fetcherLock  sync.Mutex
	oplogChan    chan []byte
	errChan      chan error

	closed atomic.Bool
}

func NewEventReader(ctx context.Context, cfg ReaderConfig) *EventReader {
	ctx, cancelFunc := context.WithCancel(ctx)
	r := &EventReader{
		ctx:                  ctx,
		cancelFunc:           cancelFunc,
		cfg:                  cfg,
		startAtOperationTime: nil,
		oplogChan:            make(chan []byte),
		errChan:              make(chan error),
	}
	r.closed.Store(false)
	return r
}

const (
	InitCheckpoint = int64(1)
	DurationTime   = 6000
)

func (r *EventReader) SetQueryTimestampOnEmpty(ts Position) {
	if r.startAtOperationTime == nil {
		if ts.Timestamp != InitCheckpoint {
			r.startAtOperationTime = ts.Timestamp
		} else {
			r.startAtOperationTime = ts.Token
		}
	}
}

func (r *EventReader) UpdateQueryTimestamp(ts int64) {
	r.startAtOperationTime = ts
}

func (r *EventReader) Start() {
	if r.fetcherExist {
		return
	}

	r.fetcherLock.Lock()
	if !r.fetcherExist { // double check
		r.fetcherExist = true
		go r.run()
	}
	r.fetcherLock.Unlock()
	return
}

func (r *EventReader) ensureNetwork() (err error) {
	if r.client != nil {
		return nil
	}
	connect, err := r.cfg.connect()
	if err != nil {
		return err
	}
	r.client, err = MongoDBStreamConn(r.ctx, connect, DefaultReaderFetchBatchSize, r.startAtOperationTime)
	if err != nil {
		return err
	}
	return nil
}

func (r *EventReader) run() {
	defer func() {
		r.fetcherLock.Lock()
		r.fetcherExist = false
		r.fetcherLock.Unlock()

		// 捕获向已关闭通道写入时产生的panic
		if p := recover(); p != nil {
			// 检查是否是因为通道已关闭导致的panic
			if r.closed.Load() {
				// 正常关闭情况下的panic，忽略
				return
			}
			// 其他类型的panic，记录日志
			event.EventAdmin.Upload(event.ErrorEvent(event.SyncExceptionsPanicExit, fmt.Errorf("EventReader run panic: %v", p)))
			return
		}
	}()

	for {
		// 检查上下文是否已取消
		select {
		case <-r.ctx.Done():
			return
		default:
		}

		if err := r.ensureNetwork(); err != nil {
			// 发送错误信息，如果通道已关闭会触发panic并被defer捕获
			r.errChan <- err
			continue
		}

		data, ok := r.getNext()
		if !ok {
			if err := r.client.Err(); err != nil {
				log.Errorf("stream reader hit the end:%v", err)
			}
			if err := r.client.Close(r.ctx); err != nil {
				log.Errorf("stream reader close err:%v", err)
			}
			r.client = nil

			time.Sleep(1 * time.Second)
			continue
		}

		// 发送数据，如果通道已关闭会触发panic并被defer捕获
		r.oplogChan <- data
	}
}

func (r *EventReader) getNext() ([]byte, bool) {
	if ok := r.client.Next(r.ctx); !ok {
		return nil, false
	}
	return r.client.Current, true
}

func (r *EventReader) Next() ([]byte, error) {
	// 检查是否已关闭
	if r.closed.Load() {
		return nil, errors.New("event reader already closed")
	}

	select {
	case ret := <-r.oplogChan:
		return ret, nil
	case err := <-r.errChan:
		return nil, err
	case <-r.ctx.Done():
		return nil, errors.New("event reader context canceled")
	}
}

func (r *EventReader) TryNext() (bool, []byte) {
	if ok := r.client.TryNext(r.ctx); !ok {
		return false, nil
	}
	return true, r.client.Current
}

func (r *EventReader) ResumeToken() (bson.Raw, error) {
	out := r.client.ResumeToken()
	if len(out) == 0 {
		return nil, errors.New("empty resume token")
	}
	return out, nil
}

func (r *EventReader) FetchNewestTimestamp() (bson.Raw, error) {
	if err := r.ensureNetwork(); err != nil {
		return nil, err
	}
	r.TryNext()
	return r.ResumeToken()
}

func (r *EventReader) Close() {
	// 如果已经关闭，直接返回
	if r.closed.Swap(true) {
		return
	}

	// 先取消上下文，停止 run 方法中的循环
	r.cancelFunc()

	// 关闭 MongoDB 连接
	if r.client != nil {
		if err := r.client.Close(r.ctx); err != nil {
			log.Errorf("EventReader close err:%v", err)
		}
		r.client = nil
	}

	// 关闭通道，这将触发向这些通道写入时的panic，并被run方法中的defer捕获
	close(r.oplogChan)
	close(r.errChan)

	// 重置fetcher状态
	r.fetcherLock.Lock()
	r.fetcherExist = false
	r.fetcherLock.Unlock()

	return
}

func WaitReader() {
	WaitInMs(DurationTime)
}

func WaitInMs(n int64) {
	time.Sleep(time.Millisecond * time.Duration(n))
}

func TimeStampToInt64(ts primitive.Timestamp) int64 {
	return int64(ts.T)<<32 + int64(ts.I)
}

func Int64ToTimestamp(t int64) primitive.Timestamp {
	return primitive.Timestamp{T: uint32(uint64(t) >> 32), I: uint32(t)}
}
