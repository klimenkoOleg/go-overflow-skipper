package main

import (
	"context"
	"time"
)

type throttle[T any] struct {
	//Send(ctx context.Context, interface{}) error
	memoryMessageBufferCapacity int64
	skippedMsgCount             uint64
	queue                       chan T
	logMessagePerDuration       time.Duration // logMessagePerDuration
	messageHandler              func(msg T) error
	skipMessageCallback         func(skippedMsgCount uint64) error
}

func NewThrottle[T any](bufferCapacity int, messageToDuration time.Duration) *throttle[T] {
	return &throttle[T]{
		queue:                 make(chan T, bufferCapacity), // up to 100 pending messages
		logMessagePerDuration: messageToDuration,            // time.Second,
	}
}

func (t *throttle[T]) Run(ctx context.Context) {
	for {
		switch {
		case <- ctx.Done():
			return
		case msg := <-t.queue:
			t.messageHandler(msg)
		}
	}
}

func (t *throttle[T]) Send(msg T) error {
	select {
	case t.queue <- msg:
		// increment cnt.
		//if state == HALF_CLOSED:
		//drop every 10nth message
	default: // means s.queue is blocked, overflown. So we're skipping publishing by dropping messages.
		now := time.Now()
		t.skippedMsgCount++
		// Reduce the number of logs to 1 msg/sec if client buffer is full
		if now.Add(-1 * t.logMessagePerDuration).After(t.lastBufferFill) { //time.Second
			t.skipMessageCallback()
			t.logger.Error("nats buffer overflow, error: buffer full", zap.Uint64("droppedMsgCount", t.skippedMsgCount))
			t.skippedMsgCount = 0
			t.lastBufferFill = now
		}
	}
}

//func (t *throttle[T]) SendMessage2(msg T, f func(ctx context.Context, msg T) error) {
//	t.queue <- msg
//}

//func (t *throttle[T]) SendMessage(msg T) {
//	t.queue <- msg
//}

func (t *throttle[T]) HandleMessage(func(ctx context.Context, msg T) error) {

}

func (t *throttle[T]) ForSkipped() bool {
	<-t.queue
	return true
}

type Order struct {

}

var t *throttle[Order]

func init1() {
	t = NewThrottle[Order](100, time.Minute)
    t.messageHandler = func(msg Order) error {
        // handle message
        return nil
    }
    t.skipMessageCallback = func(skippedMsgCount uint64) error {
        // handle skipped messages
        return nil
    }
    go t.Run(context.Background())
}

type PubOrderUpdate struct {

}

func NewPubOrderUpdate() {
	t := NewThrottle[Order](100, time.Minute)

}

func  (s *PubOrderUpdate) User(ctx context.Context, args Order) error {

	t.messageHandler = s.handleMessage
	t.skipMessageCallback = func(ctx context.Context, skippedMsgCount uint64) error {
		return nil
	}

	go func() {
		t.SendMessage2(ctx, args)
		//t.SendMessage(args)
		//t.HandleMessage(s.handleMessage)
		for t.ForSkipped() {
			s.logger.Error("nats buffer overflow, error: buffer full", zap.Uint64("droppedMsgCount", s.skippedPubs))
		}
	}()
}
