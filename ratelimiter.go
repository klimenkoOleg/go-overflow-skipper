package main

import (
	"context"
	"fmt"
	"time"
)

type Throttle[T any] struct {
	skippedMsgCount     uint64
	queue               chan T
	bucketDuration      time.Duration
	messageHandler      func(msg T)
	skipMessageCallback func(skippedMsgCount uint64)
	messageProducer     func(ctx context.Context) (T, error)
	lastBufferFill      time.Time
}

func NewThrottle[T any](bufferCapacity int, bucketDuration time.Duration) *Throttle[T] {
	return &Throttle[T]{
		queue:          make(chan T, bufferCapacity), // up to 'bufferCapacity' pending messages
		bucketDuration: bucketDuration,
	}
}

func (t *Throttle[T]) Run(ctx context.Context) error {
	go t.messageProcessor(ctx)
	for {
		msg, err := t.messageProducer(ctx)
		if err != nil {
			if ctx.Err() == nil {
				return fmt.Errorf("reading messageHandler  failed, error: %w", err)
			}
			return nil
		}

		select {
		case <-ctx.Done():
			return nil
		case t.queue <- msg:
		default:
			fmt.Println("dropping message")
			now := time.Now()
			t.skippedMsgCount++
			//  |now - 1sec|  Aster |now-0.1sec| => do nothing
			//  |now - 1sec|  Aster |now-2sec| => skip message
			if now.Add(-1 * t.bucketDuration).After(t.lastBufferFill) {
				t.skipMessageCallback(t.skippedMsgCount)
				t.skippedMsgCount = 0
				t.lastBufferFill = now
			}
		}
	}
}

func (t *Throttle[T]) messageProcessor(
	ctx context.Context,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-t.queue:
			if !ok {
				return
			}
			t.messageHandler(msg)
		}
	}
}

type Message struct {
	ID      string
	Message string
}

func main() {
	ctx := context.Background()
	throttler := NewThrottle[Message](10, time.Second)

	throttler.messageHandler = func(msg Message) {
		fmt.Printf("got in handler: %+v\n", msg)
		time.Sleep(time.Millisecond * 500)
	}
	throttler.skipMessageCallback = func(skippedMsgCount uint64) {
		fmt.Printf("skippedMsgCount: %d\n", skippedMsgCount)
	}

	throttler.messageProducer = func(ctx context.Context) (Message, error) {
		time.Sleep(time.Millisecond * 200)
		return Message{
			ID:      "1",
			Message: "hello world",
		}, nil
	}

	err := throttler.Run(ctx)
	if err != nil {
		fmt.Println("err", err)
	}

}
