package failsafe_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/failsafe-go/failsafe-go"
	"github.com/failsafe-go/failsafe-go/circuitbreaker"
	"github.com/failsafe-go/failsafe-go/hedgepolicy"
	"github.com/failsafe-go/failsafe-go/ratelimiter"
	"github.com/failsafe-go/failsafe-go/retrypolicy"
	"github.com/failsafe-go/failsafe-go/timeout"
)

type understandingEvents struct {
	mu     sync.Mutex
	events []string
}

type understandingWatchableContext struct {
	context.Context
	doneOnce   sync.Once
	doneCalled chan struct{}
	done       chan struct{}
}

func (c *understandingWatchableContext) Done() <-chan struct{} {
	c.doneOnce.Do(func() {
		close(c.doneCalled)
	})
	return c.done
}

func (c *understandingWatchableContext) Err() error {
	select {
	case <-c.done:
		return context.Canceled
	default:
		return nil
	}
}

func (e *understandingEvents) add(event string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.events = append(e.events, event)
}

func (e *understandingEvents) snapshot() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.events...)
}

func waitUnderstanding[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", what)
		var zero T
		return zero
	}
}

func TestUnderstandingExecutorChains(t *testing.T) {
	t.Run("retry+timeout cancels the shared root during a retry delay", func(t *testing.T) {
		events := &understandingEvents{}
		ctx, cancel := context.WithCancel(context.Background())
		entered := make(chan struct{})
		scheduled := make(chan struct{})
		done := make(chan error, 1)

		retryPolicy := retrypolicy.NewBuilder[string]().
			WithMaxRetries(2).
			WithDelay(time.Hour).
			OnFailure(func(event failsafe.ExecutionEvent[string]) {
				events.add(fmt.Sprintf("retry-attempt-failure attempts=%d", event.Attempts()))
			}).
			OnRetryScheduled(func(event failsafe.ExecutionScheduledEvent[string]) {
				events.add(fmt.Sprintf("retry-scheduled attempts=%d delay=%s", event.Attempts(), event.Delay))
				close(scheduled)
			}).
			Build()
		timeoutPolicy := timeout.NewBuilder[string](0).
			OnTimeoutExceeded(func(failsafe.ExecutionDoneEvent[string]) {
				<-entered
				events.add("timeout-exceeded")
			}).
			Build()

		go func() {
			_, err := failsafe.With(retryPolicy, timeoutPolicy).
				WithContext(ctx).
				OnFailure(func(event failsafe.ExecutionDoneEvent[string]) {
					events.add(fmt.Sprintf("outer-failure attempts=%d executions=%d error=%v", event.Attempts(), event.Executions(), event.Error))
				}).
				OnDone(func(failsafe.ExecutionDoneEvent[string]) {
					events.add("outer-done")
				}).
				GetWithExecution(func(exec failsafe.Execution[string]) (string, error) {
					close(entered)
					select {
					case <-exec.Context().Done():
						events.add("attempt-cancel-observed")
					case <-time.After(time.Second):
						t.Fatal("attempt was not canceled by its timeout")
					}
					events.add("attempt-return")
					return "", exec.Context().Err()
				})
			done <- err
		}()

		waitUnderstanding(t, scheduled, "retry scheduling")
		events.add("parent-cancel-requested")
		cancel()
		err := waitUnderstanding(t, done, "execution completion")
		if !errors.Is(err, timeout.ErrExceeded) {
			t.Fatalf("expected timeout.ErrExceeded, got %v", err)
		}

		want := []string{
			"timeout-exceeded",
			"attempt-cancel-observed",
			"attempt-return",
			"retry-attempt-failure attempts=1",
			"retry-scheduled attempts=1 delay=1h0m0s",
			"parent-cancel-requested",
			"outer-failure attempts=1 executions=1 error=timeout exceeded",
			"outer-done",
		}
		got := events.snapshot()
		t.Logf("events=%s", strings.Join(got, "|"))
		if strings.Join(got, "\n") != strings.Join(want, "\n") {
			t.Fatalf("unexpected events\nwant: %s\n got: %s", want, got)
		}
	})

	t.Run("hedge+circuit breaker records a late loser after outer listeners", func(t *testing.T) {
		events := &understandingEvents{}
		initialStarted := make(chan struct{})
		hedgeStarted := make(chan struct{})
		releaseWinner := make(chan struct{})
		releaseLoser := make(chan struct{})
		done := make(chan struct{})
		opened := make(chan struct{})

		breaker := circuitbreaker.NewBuilder[string]().
			WithFailureThreshold(1).
			OnSuccess(func(failsafe.ExecutionEvent[string]) {
				events.add("breaker-success")
			}).
			OnFailure(func(failsafe.ExecutionEvent[string]) {
				events.add("breaker-failure")
			}).
			OnOpen(func(circuitbreaker.StateChangedEvent) {
				events.add("breaker-open")
				close(opened)
			}).
			Build()
		hedge := hedgepolicy.NewBuilderWithDelayFunc[string](func(failsafe.ExecutionAttempt[string]) time.Duration {
			<-initialStarted
			return 0
		}).
			WithMaxHedges(1).
			OnHedge(func(event failsafe.ExecutionEvent[string]) {
				events.add(fmt.Sprintf("hedge-started attempts=%d", event.Attempts()))
			}).
			Build()

		go func() {
			_, _ = failsafe.With(hedge, breaker).
				OnSuccess(func(event failsafe.ExecutionDoneEvent[string]) {
					events.add(fmt.Sprintf("outer-success attempts=%d executions=%d", event.Attempts(), event.Executions()))
				}).
				OnFailure(func(failsafe.ExecutionDoneEvent[string]) {
					events.add("outer-failure")
				}).
				OnDone(func(failsafe.ExecutionDoneEvent[string]) {
					events.add("outer-done")
					close(done)
				}).
				GetWithExecution(func(exec failsafe.Execution[string]) (string, error) {
					if exec.IsHedge() {
						events.add("hedge-enter")
						close(hedgeStarted)
						<-exec.Context().Done()
						events.add("loser-cancel-observed")
						<-releaseLoser
						events.add("loser-return")
						return "", exec.Context().Err()
					}

					events.add("initial-enter")
					close(initialStarted)
					<-releaseWinner
					events.add("winner-return")
					return "winner", nil
				})
		}()

		waitUnderstanding(t, hedgeStarted, "hedge start")
		close(releaseWinner)
		waitUnderstanding(t, done, "outer completion")
		close(releaseLoser)
		waitUnderstanding(t, opened, "circuit breaker opening")

		want := []string{
			"initial-enter",
			"hedge-started attempts=2",
			"hedge-enter",
			"winner-return",
			"breaker-success",
			"outer-success attempts=2 executions=1",
			"outer-done",
			"loser-cancel-observed",
			"loser-return",
			"breaker-failure",
			"breaker-open",
		}
		got := events.snapshot()
		t.Logf("events=%s", strings.Join(got, "|"))
		if strings.Join(got, "\n") != strings.Join(want, "\n") {
			t.Fatalf("unexpected events\nwant: %s\n got: %s", want, got)
		}
	})

	t.Run("rate limiter+context cancel returns before taking a permit execution slot", func(t *testing.T) {
		events := &understandingEvents{}
		ctx := &understandingWatchableContext{
			Context:    context.Background(),
			doneCalled: make(chan struct{}),
			done:       make(chan struct{}),
		}
		functionStarted := make(chan struct{}, 1)
		done := make(chan error, 1)
		limiter := ratelimiter.NewSmoothBuilderWithMaxRate[string](time.Hour).
			WithMaxWaitTime(time.Hour).
			OnRateLimitExceeded(func(failsafe.ExecutionEvent[string]) {
				events.add("rate-limit-exceeded")
			}).
			Build()
		limiter.TryAcquirePermit()

		go func() {
			_, err := failsafe.With(limiter).
				WithContext(ctx).
				OnFailure(func(event failsafe.ExecutionDoneEvent[string]) {
					events.add(fmt.Sprintf("outer-failure attempts=%d executions=%d error=%v", event.Attempts(), event.Executions(), event.Error))
				}).
				OnDone(func(failsafe.ExecutionDoneEvent[string]) {
					events.add("outer-done")
				}).
				GetWithExecution(func(failsafe.Execution[string]) (string, error) {
					functionStarted <- struct{}{}
					return "unexpected", nil
				})
			done <- err
		}()

		waitUnderstanding(t, ctx.doneCalled, "rate limiter context wait")
		events.add("parent-cancel-requested")
		close(ctx.done)
		err := waitUnderstanding(t, done, "execution completion")
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}

		want := []string{
			"parent-cancel-requested",
			"outer-failure attempts=1 executions=0 error=context canceled",
			"outer-done",
		}
		got := events.snapshot()
		t.Logf("events=%s", strings.Join(got, "|"))
		if strings.Join(got, "\n") != strings.Join(want, "\n") {
			t.Fatalf("unexpected events\nwant: %s\n got: %s", want, got)
		}
		select {
		case <-functionStarted:
			t.Fatal("user function started while rate limiter was waiting")
		default:
		}
	})
}

func TestUnderstandingListenerCancelInvariant(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	limiter := ratelimiter.NewSmoothBuilderWithMaxRate[string](time.Hour).
		WithMaxWaitTime(time.Hour).
		OnRateLimitExceeded(func(failsafe.ExecutionEvent[string]) {
			t.Fatal("rate-limit-exceeded must not replace context cancellation")
		}).
		Build()
	limiter.TryAcquirePermit()

	var functionStarted bool
	var order []string
	_, err := failsafe.With(limiter).
		WithContext(ctx).
		OnFailure(func(failsafe.ExecutionDoneEvent[string]) {
			order = append(order, "outer-failure")
		}).
		OnDone(func(failsafe.ExecutionDoneEvent[string]) {
			order = append(order, "outer-done")
		}).
		GetWithExecution(func(failsafe.Execution[string]) (string, error) {
			functionStarted = true
			return "unexpected", nil
		})

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if functionStarted {
		t.Fatal("user function started")
	}
	want := []string{"outer-failure", "outer-done"}
	t.Logf("events=%s", strings.Join(order, "|"))
	if strings.Join(order, "|") != strings.Join(want, "|") {
		t.Fatalf("unexpected listener order: %v", order)
	}
}
