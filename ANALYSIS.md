# failsafe-go Executor Chain 行为分析

本文分析三组策略组合在 `Executor` 链中的调用顺序、取消传播与 listener 顺序。
所有断言都可以用 `test/understanding_test.go` 中的测试逐条核对。

## 验证方式

```bash
go build ./...
go test ./... -run 'Understanding' -count=1 -v
```

两条命令均从仓库根目录运行，退出码为 0。测试不依赖外部服务、环境变量或网络。
事件名（如 `fn.start`、`timeout.onTimeoutExceeded`、`cb.onSuccess`）与测试输出中的事件名一致。

## 调用链总览

`failsafe.With(p1, p2, ..., pn)` 在 `executor.execute` 中从右到左组合：`p1(p2(...pn(fn)...))`（executor.go:252-256）。每个策略的 `ToExecutor` 在每次 `Execute` 时创建新的 `policy.Executor`（executor.go:254）。最内层 `outerFn` 调用用户 `fn` 并累加 `executions` 计数（executor.go:234-250）。

- 使用 `BaseExecutor.Apply` 的策略（如 circuit breaker）：`PreExecute → innerFn → PostExecute`（policy/policyexecutor.go:48-58）。
- 自定义 `Apply` 的策略（retry、timeout、hedge、rate limiter）：各自实现循环/并发/等待逻辑，但都在 `Apply` 返回的闭包内调用 `innerFn`。

## 组合 1：retry + timeout — `Timeout(Retry(fn))`

`failsafe.With(to, rp)`：timeout 在最外层，retry 在 timeout 的 child context 中运行。

- **attempt 编号**：`execution.attempts` 初始为 1（execution.go:283-284），`InitializeRetry` 在每次重试前 `attempts.Add(1)`（execution.go:192）。
- **context 取消**：timeout 在 `Apply` 中通过 `CopyForCancellable` 创建子 context（timeout/timeoutexecutor.go:28，execution.go:256-260）。超时后 `execInternal.Cancel(timeoutResult)` 取消该子 context（timeout/timeoutexecutor.go:43），父 context 不受影响。
- **delay 等待**：retry 的 `time.NewTimer(delay)` 与 `exec.Canceled()` 竞争（retrypolicy/retryexecutor.go:72-77）；timeout 用 `time.AfterFunc`（timeout/timeoutexecutor.go:30）。
- **结果记录**：timeout 用 `atomic.Pointer` 的 `CompareAndSwap` 保证 timer 与 innerFn 只有一个写入结果（timeout/timeoutexecutor.go:29-32、48）。
- **精确时间线**（复现：`go test ./test/ -run TestUnderstandingRetryTimeoutChain -v`）：
  1. `fn.start`：fn 阻塞在 `exec.Canceled()`（timeout 的子 context）。
  2. `timeout.onTimeoutExceeded`：50ms 后 timer 触发，随后 `Cancel(timeoutResult)` 取消子 context。
  3. `fn.canceled`：fn 观察到取消并返回。
  4. `retry`：`IsCanceledWithResult` 在 `innerFn` 返回后立即命中取消（retrypolicy/retryexecutor.go:46-49），不再调度重试。
  5. `executor.onFailure` → `executor.onDone`。

## 组合 2：hedge + circuit breaker — `Hedge(CircuitBreaker(fn))`

`failsafe.With(hp, cb)`：每个 hedge attempt 都各自经过 circuit breaker。

- **attempt 编号**：`CopyForHedge` 在每次 hedge 时 `attempts.Add(1)` 且 `hedges.Add(1)`（execution.go:262-268）。
- **context 取消**：原始 attempt 用 `CopyForCancellable`（hedgepolicy/hedgeexecutor.go:61），hedge attempt 用 `CopyForHedge`（hedgepolicy/hedgeexecutor.go:67）。赢家确定后，其余 attempt 被 `execution.Cancel(result.result)` 取消（hedgepolicy/hedgeexecutor.go:115-123）。
- **delay 等待**：`awaitResult(timer.C)` 等待结果或 hedge delay（hedgepolicy/hedgeexecutor.go:41-55、101-102）。
- **breaker 记录**：`PreExecute` 里 `TryAcquirePermit`（circuitbreaker/circuitbreakerexecutor.go:17-22），`OnSuccess`/`OnFailure` 记录每个 attempt 的结果（circuitbreaker/circuitbreakerexecutor.go:24-37）。因此赢家和输家都会被记录。
- **精确时间线**（复现：`go test ./test/ -run TestUnderstandingHedgeCircuitBreakerChain -v`）：
  1. `fn.original.start`：原始 attempt 阻塞。
  2. `hedge.onHedge`：hedge delay 到期，启动 hedge attempt。
  3. `fn.hedge.return` → `cb.onSuccess`：hedge attempt 成功，breaker 记录成功。
  4. `executor.onSuccess` → `executor.onDone`：hedge 主循环取消原始 attempt 并返回（hedgepolicy/hedgeexecutor.go:113-125），**不等待输家 goroutine 退出**。
  5. `fn.original.canceled` → `cb.onFailure`：输家 fn 之后返回错误，breaker 仍记录该失败。

## 组合 3：rate limiter + context cancel — `RateLimiter(fn)` 与预取消的父 context

- **context 取消**：rate limiter 不创建子 context，直接用 `exec.Context()` 等待 permit（ratelimiter/ratelimiterexecutor.go:22）。`AcquirePermitsWithMaxWait` 在 `select` 中等待 timer 或 `ctx.Done()`（ratelimiter/ratelimiter.go:302-308）。
- **结果**：父 context 取消时返回 `ctx.Err()`，随后 `IsCanceledWithResult` 返回取消结果（ratelimiter/ratelimiterexecutor.go:24-26），fn 从未执行（`Executions()==0`）。
- **listener**：`onRateLimitExceeded` 仅在 `ErrExceeded` 时触发（ratelimiter/ratelimiterexecutor.go:27-31），context 取消时不触发。
- **精确时间线**（复现：`go test ./test/ -run TestUnderstandingRateLimiterContextCancel -v`）：
  1. `AcquirePermitWithMaxWait` 进入等待（permit 已预先耗尽）。
  2. `ctx.Done()` 已关闭（父 context 在 `Execute` 前取消），立即返回 `context.Canceled`。
  3. `executor.onFailure` → `executor.onDone`，fn 从未运行。

## 风险点

1. **callback 重入**：所有 listener 在调用方 goroutine 同步执行。`timeout.onTimeoutExceeded` 在 `time.AfterFunc` 的 goroutine 中运行（timeout/timeoutexecutor.go:30-44），此时 innerFn 可能仍在运行；listener 中若重入同一 executor 或阻塞，会与执行中的 attempt 竞争。CB 的状态变更 listener 在 `cb.mu` 解锁后调用（circuitbreaker/circuitbreaker.go:323-330），但 `OnSuccess`/`OnFailure` 在 `PostExecute` 中同步调用（policy/policyexecutor.go:61-69），重入 `executor.Get` 会嵌套执行。
2. **hedge 输家晚返回**：见组合 2。输家的结果仍会经过 CB 的 `PostExecute` 被记录，且发生在 `executor.onDone` 之后。
3. **timeout 与父 context 同时取消**：`CompareAndSwap` 保证结果只写一次（timeout/timeoutexecutor.go:29-32、48），但取消来源决定最终错误（`timeout.ErrExceeded` vs `context.Canceled`）。`TestUnderstandingCancelExactlyOnceInvariant` 证明取消恰好生效一次。
4. **retry 的可变 executor 状态**：`failedAttempts`/`retriesExceeded`/`lastDelay` 是 executor 实例字段（retrypolicy/retryexecutor.go:21-23），`ToExecutor` 每次 `Execute` 创建新实例（executor.go:254），因此并发 `Execute` 不共享；但同一 `Execute` 内 listener 重入 executor 会创建独立的新实例，状态不共享。
5. **rate limiter 的 `maxWaitTime` 默认值**：`NewSmooth`/`NewBursty` 默认 `maxWaitTime=0`（ratelimiter/ratelimiter.go:157 注释），超限立即返回 `ErrExceeded` 而不等待；只有显式 `WithMaxWaitTime` 后才会等待，此时才能被 context 取消。

## 推测与未验证项

以下结论由源码推断，未由新增测试直接断言：

- **Retry(Timeout)（顺序对调）**：timeout 只取消自己的子 context（timeout/timeoutexecutor.go:28），retry 层的 `IsCanceledWithResult` 检查的是 retry 层 exec（retrypolicy/retryexecutor.go:47），其 context 未被取消，因此 timeout 失败会被视为可重试的失败。开发中一次未保留的测试运行观察到此行为（retry 在 timeout 后重试）。
- **默认 circuit breaker（failureThreshold=1，容量 1 窗口）**：输家晚到的失败会驱逐赢家先前的成功记录（internal/util/stats.go:46-53），并可能使断路器打开（circuitbreaker/circuitstates.go:64-71）。

## 实际输出摘要

`go test ./... -run 'Understanding' -count=1 -v` 的实际输出（截取 `test` 包部分）：

```
=== RUN   TestUnderstandingRetryTimeoutChain
    understanding_test.go:103: event[0]=fn.start
    understanding_test.go:103: event[1]=timeout.onTimeoutExceeded
    understanding_test.go:103: event[2]=fn.canceled
    understanding_test.go:103: event[3]=executor.onFailure
    understanding_test.go:103: event[4]=executor.onDone
--- PASS: TestUnderstandingRetryTimeoutChain (0.05s)
=== RUN   TestUnderstandingHedgeCircuitBreakerChain
    understanding_test.go:179: event[0]=fn.original.start
    understanding_test.go:179: event[1]=hedge.onHedge
    understanding_test.go:179: event[2]=fn.hedge.return
    understanding_test.go:179: event[3]=cb.onSuccess
    understanding_test.go:179: event[4]=executor.onSuccess
    understanding_test.go:179: event[5]=executor.onDone
    understanding_test.go:179: event[6]=fn.original.canceled
    understanding_test.go:179: event[7]=cb.onFailure
--- PASS: TestUnderstandingHedgeCircuitBreakerChain (0.00s)
=== RUN   TestUnderstandingRateLimiterContextCancel
    understanding_test.go:224: event[0]=executor.onFailure
    understanding_test.go:224: event[1]=executor.onDone
--- PASS: TestUnderstandingRateLimiterContextCancel (0.00s)
=== RUN   TestUnderstandingCancelExactlyOnceInvariant
    understanding_test.go:267: event[0]=fn.start
    understanding_test.go:267: event[1]=fn.canceled
    understanding_test.go:267: event[2]=executor.onFailure
    understanding_test.go:267: event[3]=executor.onDone
--- PASS: TestUnderstandingCancelExactlyOnceInvariant (0.00s)
PASS
ok  	github.com/failsafe-go/failsafe-go/test	0.069s
```
