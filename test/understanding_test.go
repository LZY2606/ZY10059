package test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/failsafe-go/failsafe-go"
	"github.com/failsafe-go/failsafe-go/circuitbreaker"
	"github.com/failsafe-go/failsafe-go/hedgepolicy"
	"github.com/failsafe-go/failsafe-go/ratelimiter"
	"github.com/failsafe-go/failsafe-go/retrypolicy"
	"github.com/failsafe-go/failsafe-go/timeout"
)

// eventLog records listener and fn events in the order they occur.
// It is the only synchronization primitive these tests rely on, so that
// event ordering is established by happens-before edges (channel ops and
// mutex), never by sleeps.
type eventLog struct {
	mu     sync.Mutex
	events []string
}

func (l *eventLog) add(event string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, event)
}

func (l *eventLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.events...)
}

// logEvents writes the observed event sequence to the test log so that
// ANALYSIS.md timelines can be diffed against `go test -v` output.
func logEvents(t *testing.T, events []string) {
	t.Helper()
	for i, e := range events {
		t.Logf("event[%d]=%s", i, e)
	}
}

// Group 1: retry + timeout. Composition: Timeout(RetryPolicy(fn)) - the timeout
// wraps the whole retry loop in a single child context.
//
// Timeline under test:
//  1. fn.start        - the fn blocks on a channel, never on a timer
//  2. (timeout fires) - timeout.onTimeoutExceeded, then the attempt ctx is canceled
//  3. fn.canceled     - the fn observes cancellation and returns
//  4. retry sees IsCanceledWithResult and returns without scheduling a retry
//  5. executor.onFailure / executor.onDone
func TestUnderstandingRetryTimeoutChain(t *testing.T) {
	log := &eventLog{}
	release := make(chan struct{}) // test-controlled gate for the fn

	rp := retrypolicy.NewBuilder[bool]().
		WithMaxAttempts(3).
		WithDelay(time.Minute). // would stall the test if a retry were ever scheduled
		OnRetryScheduled(func(e failsafe.ExecutionScheduledEvent[bool]) { log.add("retry.onRetryScheduled") }).
		OnRetry(func(e failsafe.ExecutionEvent[bool]) { log.add("retry.onRetry") }).
		OnFailure(func(e failsafe.ExecutionEvent[bool]) { log.add("retry.onFailure") }).
		OnRetriesExceeded(func(e failsafe.ExecutionEvent[bool]) { log.add("retry.onRetriesExceeded") }).
		Build()
	to := timeout.NewBuilder[bool](50 * time.Millisecond).
		OnTimeoutExceeded(func(e failsafe.ExecutionDoneEvent[bool]) { log.add("timeout.onTimeoutExceeded") }).
		Build()
	// Timeout is the OUTER policy: it creates one child context around the entire
	// retry loop, so firing the timeout cancels every attempt and the retry delay.
	executor := failsafe.With(to, rp).
		OnSuccess(func(e failsafe.ExecutionDoneEvent[bool]) { log.add("executor.onSuccess") }).
		OnFailure(func(e failsafe.ExecutionDoneEvent[bool]) { log.add("executor.onFailure") }).
		OnDone(func(e failsafe.ExecutionDoneEvent[bool]) { log.add("executor.onDone") })

	result, err := executor.GetWithExecution(func(exec failsafe.Execution[bool]) (bool, error) {
		log.add("fn.start")
		select {
		case <-exec.Canceled():
			log.add("fn.canceled")
			return false, nil
		case <-release:
			return true, nil
		}
	})
	close(release)

	require.ErrorIs(t, err, timeout.ErrExceeded)
	require.False(t, result)
	events := log.snapshot()
	require.Equal(t, []string{
		"fn.start",
		"timeout.onTimeoutExceeded",
		"fn.canceled",
		"executor.onFailure",
		"executor.onDone",
	}, events)
	logEvents(t, events)
}

// Group 2: hedge + circuit breaker. Composition: HedgePolicy(CircuitBreaker(fn)).
//
// Timeline under test:
//  1. fn.original.start  - original attempt blocks until canceled
//  2. hedge.onHedge      - hedge delay elapsed, hedge attempt started
//  3. fn.hedge.return    - hedge attempt succeeds immediately
//  4. cb.onSuccess       - breaker records the WINNER's success
//  5. (hedge cancels original attempt)
//  6. executor.onSuccess / executor.onDone - Execute returns to the caller
//  7. fn.original.canceled + cb.onFailure - the LOSER returns late, and the
//     breaker still records its failure AFTER the executor completed
func TestUnderstandingHedgeCircuitBreakerChain(t *testing.T) {
	log := &eventLog{}
	originalStarted := make(chan struct{})
	loserGate := make(chan struct{}) // holds the loser fn back until Execute has returned
	loserDone := make(chan struct{}) // closed by cb.onFailure for the loser
	var originalStartOnce sync.Once

	cb := circuitbreaker.NewBuilder[bool]().
		// Capacity-2 window: keeps the winner's success AND the loser's late failure.
		// (The library default is a capacity-1 window, circuitbreakerbuilder.go:131.)
		WithFailureThreshold(2).
		OnSuccess(func(e failsafe.ExecutionEvent[bool]) { log.add("cb.onSuccess") }).
		OnFailure(func(e failsafe.ExecutionEvent[bool]) {
			log.add("cb.onFailure")
			close(loserDone)
		}).
		Build()
	hp := hedgepolicy.NewBuilderWithDelayFunc[bool](func(exec failsafe.ExecutionAttempt[bool]) time.Duration {
		// Deterministic hedge trigger: wait until the original attempt is
		// known to be blocked inside fn, then hedge immediately.
		<-originalStarted
		return 0
	}).
		OnHedge(func(e failsafe.ExecutionEvent[bool]) { log.add("hedge.onHedge") }).
		Build()
	executor := failsafe.With(hp, cb).
		OnSuccess(func(e failsafe.ExecutionDoneEvent[bool]) { log.add("executor.onSuccess") }).
		OnFailure(func(e failsafe.ExecutionDoneEvent[bool]) { log.add("executor.onFailure") }).
		OnDone(func(e failsafe.ExecutionDoneEvent[bool]) { log.add("executor.onDone") })

	result, err := executor.GetWithExecution(func(exec failsafe.Execution[bool]) (bool, error) {
		if exec.IsHedge() {
			log.add("fn.hedge.return")
			return true, nil
		}
		log.add("fn.original.start")
		originalStartOnce.Do(func() { close(originalStarted) })
		<-exec.Canceled() // canceled by the hedge policy once the hedge attempt wins
		log.add("fn.original.canceled")
		<-loserGate // stay blocked until the outer Execute has fully returned
		return false, errors.New("loser late error")
	})

	// The outer execution is done: the winner's result was returned.
	require.NoError(t, err)
	require.True(t, result)

	// Now release the loser and wait for the breaker to record its late failure.
	close(loserGate)
	<-loserDone

	events := log.snapshot()
	// Deterministic prefix: original starts, hedge fires, hedge wins, breaker records success.
	require.Equal(t, []string{"fn.original.start", "hedge.onHedge", "fn.hedge.return", "cb.onSuccess"}, events[:4])
	// The loser's late failure is recorded last, after the executor completed.
	require.Equal(t, "cb.onFailure", events[len(events)-1])
	// In between, the executor completed and the loser observed cancellation (relative
	// order of fn.original.canceled vs executor.onSuccess/onDone is a goroutine race).
	require.ElementsMatch(t, []string{"fn.original.canceled", "executor.onSuccess", "executor.onDone"}, events[4:7])
	// The breaker recorded BOTH the winner's success and the loser's late failure.
	require.Equal(t, uint(1), cb.Metrics().Successes())
	require.Equal(t, uint(1), cb.Metrics().Failures())
	logEvents(t, events)
}

// Group 3: rate limiter + context cancel. Composition: RateLimiter(fn) with a
// pre-canceled parent context.
//
// Timeline under test:
//  1. The only permit is drained up front, so the executor must wait ~1 hour.
//  2. The parent context is already canceled when Execute is called.
//  3. AcquirePermitWithMaxWait returns ctx.Err() instead of waiting.
//  4. executor.onFailure / executor.onDone - the fn NEVER runs.
func TestUnderstandingRateLimiterContextCancel(t *testing.T) {
	log := &eventLog{}

	rl := ratelimiter.NewSmoothBuilder[bool](1, time.Hour).
		WithMaxWaitTime(time.Hour).
		OnRateLimitExceeded(func(e failsafe.ExecutionEvent[bool]) { log.add("rl.onRateLimitExceeded") }).
		Build()
	// Drain the only immediately available permit so the next acquisition must wait.
	require.True(t, rl.TryAcquirePermit())

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // canceled before Execute: fully deterministic, no goroutine race

	var doneEvent failsafe.ExecutionDoneEvent[bool]
	executor := failsafe.With(rl).
		WithContext(ctx).
		OnSuccess(func(e failsafe.ExecutionDoneEvent[bool]) { log.add("executor.onSuccess") }).
		OnFailure(func(e failsafe.ExecutionDoneEvent[bool]) {
			log.add("executor.onFailure")
			doneEvent = e
		}).
		OnDone(func(e failsafe.ExecutionDoneEvent[bool]) { log.add("executor.onDone") })

	_, err := executor.GetWithExecution(func(exec failsafe.Execution[bool]) (bool, error) {
		log.add("fn.start") // must never run
		return true, nil
	})

	require.ErrorIs(t, err, context.Canceled)
	events := log.snapshot()
	require.Equal(t, []string{"executor.onFailure", "executor.onDone"}, events)
	// The attempt was counted but the fn was never executed.
	require.Equal(t, 1, doneEvent.Attempts())
	require.Equal(t, 0, doneEvent.Executions())
	logEvents(t, events)
}

// Invariant test: when the parent context and the timeout policy race to cancel
// the same execution, cancellation is observed exactly once and every listener
// fires at most once.
//
// The parent context is pre-canceled, so the timeout's timer is always stopped
// before it can fire: timeout.onTimeoutExceeded must NEVER appear, and the
// execution must end with exactly one fn.start/fn.canceled pair and exactly one
// terminal executor event.
func TestUnderstandingCancelExactlyOnceInvariant(t *testing.T) {
	log := &eventLog{}

	rp := retrypolicy.NewBuilder[bool]().
		WithMaxAttempts(3).
		WithDelay(time.Minute).
		OnRetryScheduled(func(e failsafe.ExecutionScheduledEvent[bool]) { log.add("retry.onRetryScheduled") }).
		OnRetry(func(e failsafe.ExecutionEvent[bool]) { log.add("retry.onRetry") }).
		Build()
	to := timeout.NewBuilder[bool](10 * time.Second). // stopped long before it can fire
								OnTimeoutExceeded(func(e failsafe.ExecutionDoneEvent[bool]) { log.add("timeout.onTimeoutExceeded") }).
								Build()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // parent context canceled before Execute

	executor := failsafe.With(rp, to).
		WithContext(ctx).
		OnSuccess(func(e failsafe.ExecutionDoneEvent[bool]) { log.add("executor.onSuccess") }).
		OnFailure(func(e failsafe.ExecutionDoneEvent[bool]) { log.add("executor.onFailure") }).
		OnDone(func(e failsafe.ExecutionDoneEvent[bool]) { log.add("executor.onDone") })

	_, err := executor.GetWithExecution(func(exec failsafe.Execution[bool]) (bool, error) {
		log.add("fn.start")
		<-exec.Canceled() // already closed
		log.add("fn.canceled")
		return false, nil
	})

	require.ErrorIs(t, err, context.Canceled)
	events := log.snapshot()
	require.Equal(t, []string{"fn.start", "fn.canceled", "executor.onFailure", "executor.onDone"}, events)
	logEvents(t, events)
}
