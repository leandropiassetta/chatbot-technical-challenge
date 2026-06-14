package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type fakeSQS struct {
	mu               sync.Mutex
	receiveCalls     int
	deleteCalls      int
	deletedReceipts  []string
	lastReceiveInput *sqs.ReceiveMessageInput
	receiveFn        func(context.Context, *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error)
	deleteFn         func(context.Context, *sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error)
}

func (f *fakeSQS) ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	f.mu.Lock()
	f.receiveCalls++
	f.lastReceiveInput = params
	f.mu.Unlock()

	if f.receiveFn != nil {
		return f.receiveFn(ctx, params)
	}

	<-ctx.Done()
	return nil, ctx.Err()
}

func (f *fakeSQS) DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	f.mu.Lock()
	f.deleteCalls++
	f.deletedReceipts = append(f.deletedReceipts, aws.ToString(params.ReceiptHandle))
	f.mu.Unlock()

	if f.deleteFn != nil {
		return f.deleteFn(ctx, params)
	}

	return &sqs.DeleteMessageOutput{}, nil
}

func (f *fakeSQS) snapshot() (receiveCalls int, deleteCalls int, deletedReceipts []string, lastReceiveInput *sqs.ReceiveMessageInput) {
	f.mu.Lock()
	defer f.mu.Unlock()

	deletedReceipts = append([]string(nil), f.deletedReceipts...)
	return f.receiveCalls, f.deleteCalls, deletedReceipts, f.lastReceiveInput
}

func TestRunDeletesMessageAfterSuccessfulHandler(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msg := sqsMessage("msg-1", "receipt-1", `{"event_id":"evt-1"}`)
	client := &fakeSQS{}
	var receiveAttempt atomic.Int32

	client.receiveFn = func(ctx context.Context, _ *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
		if receiveAttempt.Add(1) == 1 {
			return &sqs.ReceiveMessageOutput{Messages: []types.Message{msg}}, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	client.deleteFn = func(context.Context, *sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error) {
		cancel()
		return &sqs.DeleteMessageOutput{}, nil
	}

	bodyCh := make(chan string, 1)
	worker := newTestWorker(t, client, func(_ context.Context, body string) error {
		bodyCh <- body
		return nil
	})

	runWorker(t, ctx, worker)

	select {
	case body := <-bodyCh:
		if body != `{"event_id":"evt-1"}` {
			t.Fatalf("unexpected body: %s", body)
		}
	default:
		t.Fatal("handler was not called")
	}

	_, deleteCalls, deletedReceipts, input := client.snapshot()
	if deleteCalls != 1 {
		t.Fatalf("expected 1 delete call, got %d", deleteCalls)
	}
	if len(deletedReceipts) != 1 || deletedReceipts[0] != "receipt-1" {
		t.Fatalf("unexpected deleted receipts: %v", deletedReceipts)
	}
	if input.WaitTimeSeconds != 20 {
		t.Fatalf("expected long polling with WaitTimeSeconds=20, got %d", input.WaitTimeSeconds)
	}
	if input.MaxNumberOfMessages != 10 {
		t.Fatalf("expected batch receive with MaxNumberOfMessages=10, got %d", input.MaxNumberOfMessages)
	}
}

func TestRunDoesNotDeleteMessageWhenHandlerFails(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := &fakeSQS{}
	var receiveAttempt atomic.Int32
	client.receiveFn = func(ctx context.Context, _ *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
		if receiveAttempt.Add(1) == 1 {
			return &sqs.ReceiveMessageOutput{Messages: []types.Message{
				sqsMessage("msg-1", "receipt-1", `{"event_id":"evt-1"}`),
			}}, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}

	worker := newTestWorker(t, client, func(context.Context, string) error {
		cancel()
		return errors.New("database temporarily unavailable")
	})

	runWorker(t, ctx, worker)

	_, deleteCalls, _, _ := client.snapshot()
	if deleteCalls != 0 {
		t.Fatalf("expected no delete call when handler fails, got %d", deleteCalls)
	}
}

func TestRunRetriesReceiveAfterTransientError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := &fakeSQS{}
	var receiveAttempt atomic.Int32
	client.receiveFn = func(ctx context.Context, _ *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
		switch receiveAttempt.Add(1) {
		case 1:
			return nil, errors.New("temporary sqs error")
		case 2:
			return &sqs.ReceiveMessageOutput{Messages: []types.Message{
				sqsMessage("msg-1", "receipt-1", `{"event_id":"evt-1"}`),
			}}, nil
		default:
			<-ctx.Done()
			return nil, ctx.Err()
		}
	}
	client.deleteFn = func(context.Context, *sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error) {
		cancel()
		return &sqs.DeleteMessageOutput{}, nil
	}

	worker := newTestWorker(t, client, func(context.Context, string) error {
		return nil
	})

	runWorker(t, ctx, worker)

	receiveCalls, deleteCalls, _, _ := client.snapshot()
	if receiveCalls < 2 {
		t.Fatalf("expected receive retry after transient error, got %d receive calls", receiveCalls)
	}
	if deleteCalls != 1 {
		t.Fatalf("expected message deletion after retry success, got %d delete calls", deleteCalls)
	}
}

func TestRunLogsDeleteFailureAndStopsWithoutPanic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := &fakeSQS{}
	var receiveAttempt atomic.Int32
	client.receiveFn = func(ctx context.Context, _ *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
		if receiveAttempt.Add(1) == 1 {
			return &sqs.ReceiveMessageOutput{Messages: []types.Message{
				sqsMessage("msg-1", "receipt-1", `{"event_id":"evt-1"}`),
			}}, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	client.deleteFn = func(context.Context, *sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error) {
		cancel()
		return nil, errors.New("delete failed")
	}

	worker := newTestWorker(t, client, func(context.Context, string) error {
		return nil
	})

	runWorker(t, ctx, worker)

	_, deleteCalls, deletedReceipts, _ := client.snapshot()
	if deleteCalls != 1 {
		t.Fatalf("expected 1 delete attempt, got %d", deleteCalls)
	}
	if len(deletedReceipts) != 1 || deletedReceipts[0] != "receipt-1" {
		t.Fatalf("unexpected deleted receipts: %v", deletedReceipts)
	}
}

func TestRunStopsWhenContextIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := make(chan struct{})
	var once sync.Once
	client := &fakeSQS{
		receiveFn: func(ctx context.Context, _ *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
			once.Do(func() { close(started) })
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	var handlerCalled atomic.Bool
	worker := newTestWorker(t, client, func(context.Context, string) error {
		handlerCalled.Store(true)
		return nil
	})

	errCh := make(chan error, 1)
	go func() {
		errCh <- worker.Run(ctx)
	}()

	waitForSignal(t, started, "receive to start")
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("expected nil error on shutdown, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not stop after context cancellation")
	}

	if handlerCalled.Load() {
		t.Fatal("handler should not be called")
	}
}

func TestRunRespectsConfiguredConcurrency(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := &fakeSQS{}
	var receiveAttempt atomic.Int32
	client.receiveFn = func(ctx context.Context, _ *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
		if receiveAttempt.Add(1) == 1 {
			return &sqs.ReceiveMessageOutput{Messages: []types.Message{
				sqsMessage("msg-1", "receipt-1", "{}"),
				sqsMessage("msg-2", "receipt-2", "{}"),
				sqsMessage("msg-3", "receipt-3", "{}"),
				sqsMessage("msg-4", "receipt-4", "{}"),
				sqsMessage("msg-5", "receipt-5", "{}"),
			}}, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}

	started := make(chan struct{}, 5)
	var active atomic.Int32
	var maxActive atomic.Int32

	cfg := testConfig()
	cfg.Concurrency = 2
	worker, err := NewWorker(client, cfg, func(ctx context.Context, _ string) error {
		current := active.Add(1)
		updateMax(&maxActive, current)
		started <- struct{}{}
		<-ctx.Done()
		active.Add(-1)
		return ctx.Err()
	}, discardLogger())
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- worker.Run(ctx)
	}()

	waitForNSignals(t, started, 2, "handlers to start")
	select {
	case <-started:
		t.Fatal("worker started more handlers than configured concurrency")
	case <-time.After(30 * time.Millisecond):
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("expected nil error on shutdown, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not stop after context cancellation")
	}

	if got := maxActive.Load(); got != 2 {
		t.Fatalf("expected max active handlers to be 2, got %d", got)
	}
}

func newTestWorker(t *testing.T, client *fakeSQS, handler HandlerFunc) *Worker {
	t.Helper()

	worker, err := NewWorker(client, testConfig(), handler, discardLogger())
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	return worker
}

func testConfig() WorkerConfig {
	return WorkerConfig{
		QueueURL:          "https://sqs.us-east-1.amazonaws.com/123456789012/session-events",
		Concurrency:       1,
		HandlerTimeout:    200 * time.Millisecond,
		ReceiveBackoff:    time.Millisecond,
		MaxReceiveBackoff: 2 * time.Millisecond,
		DeleteCallTimeout: 100 * time.Millisecond,
	}
}

func runWorker(t *testing.T, ctx context.Context, worker *Worker) {
	t.Helper()

	errCh := make(chan error, 1)
	go func() {
		errCh <- worker.Run(ctx)
	}()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("worker returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not stop")
	}
}

func sqsMessage(messageID, receiptHandle, body string) types.Message {
	return types.Message{
		MessageId:     aws.String(messageID),
		ReceiptHandle: aws.String(receiptHandle),
		Body:          aws.String(body),
		Attributes: map[string]string{
			string(types.MessageSystemAttributeNameApproximateReceiveCount): "1",
		},
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func waitForSignal(t *testing.T, ch <-chan struct{}, description string) {
	t.Helper()

	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func waitForNSignals(t *testing.T, ch <-chan struct{}, n int, description string) {
	t.Helper()

	for i := 0; i < n; i++ {
		waitForSignal(t, ch, description)
	}
}

func updateMax(max *atomic.Int32, value int32) {
	for {
		current := max.Load()
		if value <= current {
			return
		}
		if max.CompareAndSwap(current, value) {
			return
		}
	}
}
