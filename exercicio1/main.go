package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type SQSAPI interface {
	ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
}

type HandlerFunc func(ctx context.Context, body string) error

type WorkerConfig struct {
	QueueURL          string
	MaxMessages       int32
	WaitTimeSeconds   int32
	VisibilityTimeout int32
	Concurrency       int
	HandlerTimeout    time.Duration
	ReceiveBackoff    time.Duration
	MaxReceiveBackoff time.Duration
	DeleteCallTimeout time.Duration
}

type Worker struct {
	client  SQSAPI
	handler HandlerFunc
	cfg     WorkerConfig
	logger  *slog.Logger
}

func NewWorker(client SQSAPI, cfg WorkerConfig, handler HandlerFunc, logger *slog.Logger) (*Worker, error) {
	if client == nil {
		return nil, errors.New("sqs client is required")
	}
	if handler == nil {
		return nil, errors.New("handler is required")
	}

	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.Default()
	}

	return &Worker{
		client:  client,
		handler: handler,
		cfg:     cfg,
		logger:  logger,
	}, nil
}

func (c WorkerConfig) withDefaults() WorkerConfig {
	if c.MaxMessages <= 0 || c.MaxMessages > 10 {
		c.MaxMessages = 10
	}
	if c.WaitTimeSeconds <= 0 || c.WaitTimeSeconds > 20 {
		c.WaitTimeSeconds = 20
	}
	if c.VisibilityTimeout <= 0 {
		c.VisibilityTimeout = 60
	}
	if c.Concurrency <= 0 {
		c.Concurrency = 8
	}
	if c.ReceiveBackoff <= 0 {
		c.ReceiveBackoff = 500 * time.Millisecond
	}
	if c.MaxReceiveBackoff <= 0 {
		c.MaxReceiveBackoff = 10 * time.Second
	}
	if c.DeleteCallTimeout <= 0 {
		c.DeleteCallTimeout = 5 * time.Second
	}
	if c.HandlerTimeout <= 0 {
		c.HandlerTimeout = time.Duration(c.VisibilityTimeout)*time.Second - 5*time.Second
	}
	return c
}

func (c WorkerConfig) validate() error {
	if c.QueueURL == "" {
		return errors.New("queue URL is required")
	}
	if c.HandlerTimeout <= 0 {
		return errors.New("handler timeout must be positive")
	}
	if c.HandlerTimeout >= time.Duration(c.VisibilityTimeout)*time.Second {
		return fmt.Errorf("handler timeout %s must be lower than visibility timeout %ds", c.HandlerTimeout, c.VisibilityTimeout)
	}
	return nil
}

func (w *Worker) Run(ctx context.Context) error {
	jobs := make(chan types.Message, w.cfg.Concurrency*int(w.cfg.MaxMessages))

	var wg sync.WaitGroup
	for i := 0; i < w.cfg.Concurrency; i++ {
		wg.Add(1)
		go w.consume(ctx, i, jobs, &wg)
	}
	defer func() {
		close(jobs)
		wg.Wait()
	}()

	backoff := w.cfg.ReceiveBackoff
	for {
		if ctx.Err() != nil {
			return nil
		}

		out, err := w.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:                    aws.String(w.cfg.QueueURL),
			MaxNumberOfMessages:         w.cfg.MaxMessages,
			WaitTimeSeconds:             w.cfg.WaitTimeSeconds,
			VisibilityTimeout:           w.cfg.VisibilityTimeout,
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameApproximateReceiveCount},
			MessageAttributeNames:       []string{"All"},
		})
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}

			wait := withJitter(backoff)
			w.logger.Error("failed to receive SQS messages", "error", err, "backoff", wait)

			if !sleep(ctx, wait) {
				return nil
			}
			backoff = minDuration(backoff*2, w.cfg.MaxReceiveBackoff)
			continue
		}

		backoff = w.cfg.ReceiveBackoff
		for _, msg := range out.Messages {
			select {
			case jobs <- msg:
			case <-ctx.Done():
				return nil
			}
		}
	}
}

func (w *Worker) consume(ctx context.Context, workerID int, jobs <-chan types.Message, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-jobs:
			if !ok {
				return
			}
			w.processOne(ctx, workerID, msg)
		}
	}
}

func (w *Worker) processOne(ctx context.Context, workerID int, msg types.Message) {
	messageID := aws.ToString(msg.MessageId)
	receiptHandle := aws.ToString(msg.ReceiptHandle)
	if receiptHandle == "" {
		w.logger.Error("message without receipt handle", "message_id", messageID, "worker_id", workerID)
		return
	}

	handlerCtx, cancel := context.WithTimeout(ctx, w.cfg.HandlerTimeout)
	defer cancel()

	if err := w.handler(handlerCtx, aws.ToString(msg.Body)); err != nil {
		w.logger.Error(
			"message handling failed",
			"error", err,
			"message_id", messageID,
			"receive_count", msg.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)],
			"worker_id", workerID,
		)
		return
	}

	deleteCtx, cancelDelete := context.WithTimeout(context.WithoutCancel(ctx), w.cfg.DeleteCallTimeout)
	defer cancelDelete()

	if _, err := w.client.DeleteMessage(deleteCtx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(w.cfg.QueueURL),
		ReceiptHandle: msg.ReceiptHandle,
	}); err != nil {
		w.logger.Error("failed to delete SQS message", "error", err, "message_id", messageID)
		return
	}

	w.logger.Info("message processed", "message_id", messageID, "worker_id", workerID)
}

func sleep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func withJitter(d time.Duration) time.Duration {
	half := d / 2
	if half <= 0 {
		return d
	}
	return half + time.Duration(rand.Int63n(int64(half)))
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	queueURL := os.Getenv("QUEUE_URL")
	if queueURL == "" {
		logger.Error("QUEUE_URL environment variable is required")
		os.Exit(1)
	}

	awsCfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		logger.Error("failed to load AWS configuration", "error", err)
		os.Exit(1)
	}

	worker, err := NewWorker(
		sqs.NewFromConfig(awsCfg),
		WorkerConfig{QueueURL: queueURL},
		handleMessage,
		logger,
	)
	if err != nil {
		logger.Error("failed to create worker", "error", err)
		os.Exit(1)
	}

	if err := worker.Run(ctx); err != nil {
		logger.Error("worker stopped with error", "error", err)
		os.Exit(1)
	}
}

func handleMessage(ctx context.Context, body string) error {
	// Em produção, este handler deve extrair event_id/session_id e aplicar idempotência
	// antes de executar efeitos colaterais como escrita em banco, enriquecimento e logging.
	slog.InfoContext(ctx, "session event processed", "body_size", len(body))
	return nil
}
