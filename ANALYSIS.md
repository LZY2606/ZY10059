# failsafe-go executor chain 分析

范围：只分析同步 `GetWithExecution` 路径；异步路径会额外在 `executeAsync` 中建立一个可取消子 context，但策略链本身相同。本文所有“已核对”断言都能由列出的源码行或新增测试 `understanding_executor_chains_test.go` 复现；推断单独列在“风险点”中。

## 0. 共用调用链

- `GetWithExecution` 进入 `executeSync(..., true)`，后者用配置的 context 创建根 `execution`：`executor.go:175`、`executor.go:210`。
- `execute` 先创建最内层用户函数闭包；用户函数返回后调用 `execInternal.record()` 增加 completed executions，再包装成 `Done/Success/SuccessAll=true` 的 `PolicyResult`：`executor.go:233`、`executor.go:241`。
- 策略按传入参数逆序 `Apply`，所以 `failsafe.With(A, B)` 的运行形态是 `A(B(userFn))`：`executor.go:252`。官方注释也用 `Fallback(RetryPolicy(CircuitBreaker(func)))` 说明这一点：`executor.go:94`。
- 最外层返回后，executor 只按最终 `SuccessAll` 调 success/failure 之一，然后无论成败都调 done：`executor.go:261`。
- attempt 状态由根 `execution` 内共享指针维护；`attempts/retries/hedges/executions` 是共享指针字段，而 `ctx`、`isHedge`、`lastError` 等随 copy 分发：`execution.go:78`。
- 根执行从 attempt 1 开始：`execution.go:282`；retry 在通过取消检查后才增加 attempts/retries：`execution.go:184`；hedge 在 `CopyForHedge` 中增加 attempts/hedges：`execution.go:262`。
- timeout 和普通可取消副本都通过 `context.WithCancel` 建子 context：`execution.go:256`；hedge 副本同样建子 context，并标记 hedge、增加计数：`execution.go:262`。
- 取消结果保存在共享的 `canceledResult` 指针里；context 已取消但没有显式结果时才返回 `ctx.Err()`，否则保留第一次写入的取消结果：`execution.go:234`。`Cancel` 对已取消 context 是 no-op，不会覆盖旧结果：`execution.go:200`。
- 策略基类的 PostExecute 先判失败；成功时把结果标为 done/success 并触发 policy success listener，失败时先标记 failure 再调用 policy failure listener：`policy/policyexecutor.go:61`。

## 1. retry + timeout

配置顺序使用 `failsafe.With(retryPolicy, timeoutPolicy)`，即 `RetryPolicy(Timeout(userFn))`，timeout 负责单次 attempt。

### 调用链

1. retry executor 进入无限循环，调用 `innerFn(exec)`；这里的 `exec` 仍是根 execution：`retrypolicy/retryexecutor.go:38`。
2. timeout executor 复制出单次 attempt 的可取消子 context：`timeout/timeoutexecutor.go:27`、`execution.go:256`。
3. timeout 用 `time.AfterFunc` 与内部执行结果做 CAS 竞争：`timeout/timeoutexecutor.go:30`。
4. timeout 赢时写入 `timeout.ErrExceeded`，先发 `OnTimeoutExceeded`，再 `execInternal.Cancel(timeoutResult)`：`timeout/timeoutexecutor.go:31`、`timeout/timeoutexecutor.go:33`、`timeout/timeoutexecutor.go:43`。
5. 用户函数看到的是 timeout 的子 context；它返回 `context.Canceled` 后，timeout 在 `PostExecute` 只把 `timeout.ErrExceeded` 判为失败，因此把保存的 timeout 结果继续标为失败：`timeout/timeoutexecutor.go:48`、`timeout/timeoutexecutor.go:52`、`timeout/timeoutexecutor.go:56`。
6. retry 在 `innerFn` 返回后先检查自己持有的根 execution 是否已取消：`retrypolicy/retryexecutor.go:46`。此时 timeout 只取消了子 context，根 context 尚未取消，所以 retry 会继续处理第 1 次 timeout 失败并进入 delay；父 context 后来取消后，才在 `InitializeRetry` 读到共享的 timeout 取消结果并停止。
7. retry 的普通失败路径会把 failedAttempts 加一，并按 retry 条件决定是否 done：`retrypolicy/retryexecutor.go:99`。
8. delay 在 retry executor 的 `select { timer.C / exec.Canceled() }` 中等待；监听的 `exec.Canceled()` 是根 context，不是 timeout 子 context：`retrypolicy/retryexecutor.go:72`。
9. delay 结束或被取消后，retry 调 `InitializeRetry`；只有未取消时才增加 attempts/retries 并清空 canceledResult：`retrypolicy/retryexecutor.go:79`、`execution.go:184`。
10. retry budget（若配置）在 executor 入口记录一次 primary execution，首次 permit 在 delay 后、`OnRetry` 前申请，在下一次 innerFn 返回后释放：`retrypolicy/retryexecutor.go:33`、`retrypolicy/retryexecutor.go:84`、`retrypolicy/retryexecutor.go:42`。budget 自身的 permit/inflight 记账见 `budget/budget.go:112` 和 `budget/budget.go:120`。

### 精确时间线

测试：`TestUnderstandingExecutorChains/retry+timeout_cancels_the_shared_root_during_a_retry_delay`，定义在 `understanding_executor_chains_test.go:73`。它用 `entered/scheduled/done` channel 固定顺序，不用 sleep 猜先后。timeout 配置为 0，仅用于触发真实 timeout 分支；callback 等 `entered` 关闭后才记录 timeout 事件并执行取消。

1. `attempt-enter`：用户函数关闭 `entered`，等待 timeout callback 放行。
2. `timeout-exceeded`：timeout callback CAS 成功并调用 listener。
3. `attempt-cancel-observed`：callback 在 listener 返回后调用 `Cancel(timeout.ErrExceeded)`，用户函数等待的 timeout 子 context 关闭。
4. `attempt-return`：用户函数返回其 context 的 `context.Canceled`。
5. timeout `PostExecute` 使用 atomic pointer 中保存的 `timeout.ErrExceeded`，并把它标为失败。
6. `retry-attempt-failure attempts=1`：retry 记录第 1 次失败；attempt 仍是 1，executions 已因用户函数返回而变成 1。
7. `retry-scheduled attempts=1 delay=1h0m0s`：retry listener 在等待前触发；1h delay 是 channel barrier，测试不实际等待一小时。
8. `parent-cancel-requested`：测试取消 executor 的父 context。
9. retry delay select 被根 context 唤醒；`InitializeRetry` 读到已存在的 timeout 取消结果，直接返回该结果，不把 attempt 增加到 2。
10. `outer-failure attempts=1 executions=1 error=timeout exceeded`，随后 `outer-done`。

实际日志摘要：

```text
events=timeout-exceeded|attempt-cancel-observed|attempt-return|retry-attempt-failure attempts=1|retry-scheduled attempts=1 delay=1h0m0s|parent-cancel-requested|outer-failure attempts=1 executions=1 error=timeout exceeded|outer-done
```

listener 顺序结论：该路径依次是 timeout policy listener、retry failure listener、retry scheduled listener、executor failure listener、executor done listener。取消发生在 delay 中时没有 retry listener；最终错误取已保存的 `timeout.ErrExceeded`，不是父 context 的 `context.Canceled`，因为 `isCanceledWithResult` 优先返回显式取消结果：`execution.go:234`。

### 最小复现

完整可运行代码就是 `understanding_executor_chains_test.go:73` 的第一个子测试。关键配置是 retry delay `time.Hour`（`understanding_executor_chains_test.go:80`）与 timeout 0（`understanding_executor_chains_test.go:91`）；用户函数只等待 context/channel，不 sleep：`understanding_executor_chains_test.go:107`。

## 2. hedge + circuit breaker

配置顺序使用 `failsafe.With(hedgePolicy, breaker)`，即 `HedgePolicy(CircuitBreaker(userFn))`。breaker 在每个实际启动的 hedge goroutine 内层独立记账。

### 调用链

1. hedge executor 保存 `parentExecution`，并分配 `maxHedges+1` 个 execution 槽位和带缓冲 result channel：`hedgepolicy/hedgeexecutor.go:27`。
2. 第 0 次调用 `CopyForCancellable`，建立 initial 子 context，但不增加 attempt：`hedgepolicy/hedgeexecutor.go:60`、`execution.go:256`。
3. 后续 hedge 若通过 budget，调用 `CopyForHedge`：标记 hedge、attempts+1、hedges+1，并建立另一个子 context：`hedgepolicy/hedgeexecutor.go:67`、`execution.go:262`。
4. `OnHedge` 在 goroutine 启动前、于 hedge executor 所在 goroutine 同步触发：`hedgepolicy/hedgeexecutor.go:69`。
5. 每个 attempt 都在独立 goroutine 中执行 `innerFn(hedgeExec)`；这里的 innerFn 是 circuit breaker：`hedgepolicy/hedgeexecutor.go:77`。
6. breaker 通用 `Apply` 先 `PreExecute`，许可失败直接返回 `ErrOpen`，否则执行 innerFn 并 PostExecute：`policy/policyexecutor.go:48`；breaker 的 PreExecute 调 `TryAcquirePermit`，拒绝时返回 `ErrOpen`：`circuitbreaker/circuitbreakerexecutor.go:17`。
7. breaker 成功时调用 `RecordSuccess`：`circuitbreaker/circuitbreakerexecutor.go:24`；失败时先把结果复制到 execution，再 `recordFailure`：`circuitbreaker/circuitbreakerexecutor.go:29`。
8. breaker 状态对象在记录成功/失败后释放 permit 并检查阈值：`circuitbreaker/circuitbreaker.go:392`、`circuitbreaker/circuitbreaker.go:398`。closed state 达到失败阈值会 open：`circuitbreaker/circuitstates.go:60`。
9. goroutine 把用户返回值包装为 `execResult`；`final` 来自 hedge 的 abort/cancel 条件，默认 builder 对任意结果都返回 true：`hedgepolicy/hedgeexecutor.go:84`、`hedgepolicy/hedge.go:195`。
10. 主 hedge loop 收到 final result 后取消每个子 context：winner 调 `Cancel(nil)`，loser 调 `Cancel(result.result)`；随后立即返回 winner result，并不等待 loser goroutine：`hedgepolicy/hedgeexecutor.go:113`。
11. hedge budget（若配置）在入口记录 primary execution；hedge permit 在启动 hedge 前申请，在该 goroutine 的 innerFn 返回后释放：`hedgepolicy/hedgeexecutor.go:33`、`hedgepolicy/hedgeexecutor.go:62`、`hedgepolicy/hedgeexecutor.go:80`。本复现没有配置 budget，因此没有 budget 事件。

### 精确时间线

测试：`TestUnderstandingExecutorChains/hedge+circuit_breaker_records_a_late_loser_after_outer_listeners`，定义在 `understanding_executor_chains_test.go:146`。channel 控制 initial 先启动、hedge 后启动、winner 先返回、loser 在 outer done 后才返回。

1. `initial-enter`：第 0 个 goroutine 拿到 breaker permit，关闭 `initialReady`，阻塞在 `releaseWinner`。
2. hedge delayFunc 从 `initialReady` 返回 0；`CopyForHedge` 把共享 attempts 增加到 2、hedges 增加到 1。
3. `hedge-started attempts=2`：hedge policy listener 触发。
4. `hedge-enter`：第二个 goroutine 通过 breaker 的 permit 检查并进入用户函数。
5. 测试关闭 `releaseWinner`，记录 `winner-return`；initial goroutine 成功穿过 breaker PostExecute，产生 `breaker-success`。
6. hedge 主 loop 收到 winner result，取消 initial 的子 context（`Cancel(nil)`）和 hedge loser 的子 context（参数为 winner result）。
7. executor 最外层得到成功结果，依次记录 `outer-success attempts=2 executions=1` 和 `outer-done`；attempts 为 2 是 initial+hedge，executions 为 1 是当时只有 winner goroutine 已经返回。
8. loser goroutine 已观察到自己的 context done，记录 `loser-cancel-observed`，但继续阻塞在 `releaseLoser`。
9. 测试在 outer done 后关闭 `releaseLoser`，记录 `loser-return`；用户函数显式返回 `exec.Context().Err()`，即 `context.Canceled`。
10. loser 内层 breaker 不知道自己是“输家”，仍把该返回值作为一次实际失败，记录 `breaker-failure`，阈值为 1 后记录 `breaker-open`。

实际日志摘要：

```text
events=initial-enter|hedge-started attempts=2|hedge-enter|winner-return|breaker-success|outer-success attempts=2 executions=1|outer-done|loser-cancel-observed|loser-return|breaker-failure|breaker-open
```

已核对结论：breaker 记录的是“哪个 goroutine 的内层用户函数实际返回了什么”，不是 hedge 最终 winner result。源码把 `innerFn(hedgeExec)` 的返回值直接送入 result channel：`hedgepolicy/hedgeexecutor.go:79`、`hedgepolicy/hedgeexecutor.go:93`；而 loser 被取消后并未被 join：`hedgepolicy/hedgeexecutor.go:114`。因此 breaker 可以在 executor success/done listener 已经结束后，再收到 loser 的失败 listener 和 open listener。

### 最小复现

完整代码见 `understanding_executor_chains_test.go:146`。关键点：breaker 阈值为 1（`understanding_executor_chains_test.go:155`），hedge delayFunc 等待 `initialStarted` 后返回 0（`understanding_executor_chains_test.go:168`），loser 观察取消后继续等 `releaseLoser`（`understanding_executor_chains_test.go:194`）。

## 3. rate limiter + context cancel

配置顺序使用单策略 `failsafe.With(limiter).WithContext(ctx)`，即 `RateLimiter(userFn)`。复现让 limiter 在等待 permit 时，父 context 已经取消。

### 调用链

1. 同步 executor 的根 execution 直接使用 `WithContext` 提供的 context；同步路径不再额外包一层 context：`executor.go:210`。异步路径才在 `executeAsync` 中调用 `context.WithCancel`：`executor.go:215`。
2. rate limiter 自定义 `Apply`，第一件事是 `AcquirePermitWithMaxWait(exec.Context(), maxWaitTime)`：`ratelimiter/ratelimiterexecutor.go:20`。
3. permit 计算先由 stats 预占：smooth limiter 更新 `nextFreePermitTime`，bursty limiter 扣减 `availablePermits`，然后才返回等待时间：`ratelimiter/ratelimiterstats.go:31`、`ratelimiter/ratelimiterstats.go:53`、`ratelimiter/ratelimiterstats.go:79`、`ratelimiter/ratelimiterstats.go:118`。
4. `AcquirePermitsWithMaxWait` 在 timer 与 `ctx.Done()` 之间 select；父 context 取消时停止 timer 并返回 `ctx.Err()`：`ratelimiter/ratelimiter.go:294`、`ratelimiter/ratelimiter.go:302`。
5. executor 收到错误后先问共享 execution 是否已取消；若已取消，直接返回取消结果：`ratelimiter/ratelimiterexecutor.go:22`、`ratelimiter/ratelimiterexecutor.go:24`。
6. 只有不是取消结果时，rate-limit-exceeded listener 才可能触发；`context.Canceled` 不匹配 `ErrExceeded`：`ratelimiter/ratelimiterexecutor.go:27`。
7. permit 获取成功才会调用 `innerFn(exec)`：`ratelimiter/ratelimiterexecutor.go:34`。因此取消发生在等待阶段时，用户函数不会启动，`execInternal.record()` 也不会执行。
8. attempt 编号仍由根 execution 初始化为 1；rate limiter 不增加 attempts/retries/hedges。被阻止的这次仍算一个 attempt，但 completed executions 为 0；初始化见 `execution.go:282`，completed counter 增加点见 `executor.go:241`。
9. 最终 executor 根据返回的 `context.Canceled` 把 `SuccessAll` 置为 false；通用 PostExecute 的失败路径负责该标记：`policy/policyexecutor.go:61`。随后只触发外层 failure/done：`executor.go:261`。

### 精确时间线

测试：`TestUnderstandingExecutorChains/rate_limiter+context_cancel_returns_before_taking_a_permit_execution_slot`，定义在 `understanding_executor_chains_test.go:235`。

1. 建立 1h 一个 permit 的 smooth limiter，并先用公开 API `TryAcquirePermit()` 消耗当前 permit。
2. executor 在 goroutine 中以测试 context 创建根 execution，attempts 初始化为 1。
3. limiter stats 先预占下一次 permit，得到等待时间，然后在 timer 与 `ctx.Done()` 间 select。
4. 测试等到 context 的 `Done()` 被调用后关闭取消 channel，并记录 `parent-cancel-requested`；select 随即走 `ctx.Done()` 分支并返回 `context.Canceled`。
5. limiter executor 的取消检查返回共享取消结果；不触发 `rate-limit-exceeded`，也不调用用户函数。
6. 最外层记录 `outer-failure attempts=1 executions=0 error=context canceled`。
7. 最外层记录 `outer-done`。
8. 测试最后检查 functionStarted channel，确认用户函数没有启动。

实际日志摘要：

```text
events=parent-cancel-requested|outer-failure attempts=1 executions=0 error=context canceled|outer-done
```

已核对结论：等待阶段取消的是 executor 的根 context（同步路径没有额外策略子 context）；delay 不在 failsafe executor 的额外等待循环里，而在 rate limiter 的 `AcquirePermitsWithMaxWait` select 内：`ratelimiter/ratelimiter.go:302`。该次不会被 breaker 记录，因为本组合没有 breaker；也不会产生 completed execution。一个值得注意的资源语义是 stats 在 context select 之前已经预占 permit，源码没有回滚步骤：`ratelimiter/ratelimiterstats.go:53` 与 `ratelimiter/ratelimiter.go:302`。

### 最小复现

完整代码见 `understanding_executor_chains_test.go:235`。唯一时序控制是可观察 `Done()` 的测试 context、取消 channel 和 functionStarted channel（`understanding_executor_chains_test.go:237`、`understanding_executor_chains_test.go:268`、`understanding_executor_chains_test.go:261`），没有 sleep 或外部服务。

## 4. 成功、失败、取消时的 listener 顺序

- 最外层：成功只调 `OnSuccess` 后调 `OnDone`；失败只调 `OnFailure` 后调 `OnDone`；二者互斥：`executor.go:261`。
- retry：每次普通失败先走 retry policy 的 `OnFailure`，可重试时在等待前触发 `OnRetryScheduled`，等待并完成 `InitializeRetry`、budget permit 后才触发 `OnRetry`：`retrypolicy/retryexecutor.go:90`。若在执行或等待期间已经取消，则直接返回取消结果，不会调用这些“准备下一次 retry”的 listener：`retrypolicy/retryexecutor.go:46`、`retrypolicy/retryexecutor.go:79`。
- timeout：timeout 赢得 CAS 时，`OnTimeoutExceeded` 在 `Cancel` 之前同步执行于 `time.AfterFunc` goroutine：`timeout/timeoutexecutor.go:33`、`timeout/timeoutexecutor.go:43`。
- hedge：`OnHedge` 在 hedge goroutine 启动前同步执行：`hedgepolicy/hedgeexecutor.go:69`。hedge executor 自定义 `Apply` 并直接返回 result，不走通用 `PostExecute`，所以它自身没有 success/failure policy listener。
- breaker：winner/loser goroutine 的内层 breaker 各自在 PostExecute 中同步触发 success/failure listener；状态 listener 在状态对象完成切换后、释放 breaker mutex 时调用：`circuitbreaker/circuitbreaker.go:292`。
- rate limiter：取消优先返回，不触发 `OnRateLimitExceeded`；只有错误是 `ratelimiter.ErrExceeded` 才触发该 listener：`ratelimiter/ratelimiterexecutor.go:24`、`ratelimiter/ratelimiterexecutor.go:27`。

## 5. 风险点

以下风险中，代码事实均可由源码核对；“影响”属于根据这些事实做的工程判断，已单独标明。

1. **callback 重入（事实）**：breaker 状态切换 listener 在 `transitionTo` 内部临时释放 `cb.mu` 后执行，listener 返回后再重新加锁：`circuitbreaker/circuitbreaker.go:323`。**影响（推断）**：listener 中公开调用 `Open/Close/TryAcquirePermit/Record*` 可重入同一把锁并改变状态；若 listener 还等待被同一执行链持有的其他锁/channel，可能自死锁或产生意外顺序。
2. **hedge 输家晚返回（事实）**：hedge 选定 winner 后只取消所有子 context 并立即返回，没有等待 loser goroutine：`hedgepolicy/hedgeexecutor.go:113`。内层 breaker 仍会记录 loser 后续返回值：`circuitbreaker/circuitbreakerexecutor.go:29`。新增测试证明 outer done 后仍可能出现 `breaker-failure|breaker-open`。**影响（推断）**：调用方若把 outer success/done 当作所有内层策略已记账，可能提前复用或关闭资源，随后被晚到的失败打开 breaker。
3. **timeout 与父 context 同时取消（事实）**：timeout callback 用 CAS 决定 timeout result 是否成为 atomic result，并调用 `Cancel` 写入共享取消结果：`timeout/timeoutexecutor.go:30`、`execution.go:200`。父 context 取消后，已有显式取消结果时不会覆盖：`execution.go:203`、`execution.go:234`。**影响（推断）**：同时发生时，最终错误可能是 `timeout.ErrExceeded` 或 `context.Canceled`，取决于 timeout callback 是否先完成写入；不能只按最终 context 状态判断错误来源。新增测试只证明“timeout 先写入、父 context 后取消”的确定性分支，不断言同时调度的输赢。
4. **等待 permit 时取消仍已预占 permit（事实）**：rate limiter stats 在进入 context select 前更新 permit 时间或余额：`ratelimiter/ratelimiterstats.go:53`、`ratelimiter/ratelimiterstats.go:118`；ctx 取消路径只停 timer 并返回：`ratelimiter/ratelimiter.go:302`，没有回滚 stats 的调用。**影响（推断）**：被取消的等待可能仍推迟后续请求的可用时间；调用方不能假定取消的等待完全不消耗限流配额。
5. **listener 阻塞会改变取消和 goroutine 回收时机（事实+推断）**：timeout listener 在 `Cancel` 前执行：`timeout/timeoutexecutor.go:33`；breaker 状态 listener 返回前 `transitionTo` 不会完成：`circuitbreaker/circuitbreaker.go:323`；hedge 不 join loser：`hedgepolicy/hedgeexecutor.go:113`。**影响（推断）**：在这些 listener 中阻塞、加锁或执行同 executor 调用，可能延后子 context 关闭、延长临界区或放大晚返回 goroutine 的生命周期；listener 应保持快速、非重入。

## 6. 新增不变量测试

`TestUnderstandingListenerCancelInvariant` 定义在 `understanding_executor_chains_test.go:294`。它只使用公开 API：预取消 context、配置 rate limiter 的 `OnRateLimitExceeded`、executor 的 `OnFailure/OnDone`，并通过普通布尔值确认用户函数未启动；不读取任何私有字段。

证明的不变量：

- 在 rate limiter 等待 permit 期间，若 context 已取消，取消结果优先，`rate-limit-exceeded` listener 不得触发。
- 用户函数不得启动。
- 外层 listener 顺序必须是 `outer-failure|outer-done`。

实际日志摘要：

```text
events=outer-failure|outer-done
```

## 7. 复现与验收命令

准备阶段已从仓库根目录执行，且不作为演示：

```sh
go build ./...
```

验收命令必须从仓库根目录直接运行：

```sh
go test ./... -run 'Understanding' -count=1 -v
```

本次实际输出摘要（完整输出可重新运行命令获得）：

```text
=== RUN   TestUnderstandingExecutorChains
=== RUN   TestUnderstandingExecutorChains/retry+timeout_cancels_the_shared_root_during_a_retry_delay
=== RUN   TestUnderstandingExecutorChains/hedge+circuit_breaker_records_a_late_loser_after_outer_listeners
=== RUN   TestUnderstandingExecutorChains/rate_limiter+context_cancel_returns_before_taking_a_permit_execution_slot
--- PASS: TestUnderstandingExecutorChains
=== RUN   TestUnderstandingListenerCancelInvariant
--- PASS: TestUnderstandingListenerCancelInvariant
PASS
```

新增测试名称为：

- `TestUnderstandingExecutorChains`
- `TestUnderstandingExecutorChains/retry+timeout_cancels_the_shared_root_during_a_retry_delay`
- `TestUnderstandingExecutorChains/hedge+circuit_breaker_records_a_late_loser_after_outer_listeners`
- `TestUnderstandingExecutorChains/rate_limiter+context_cancel_returns_before_taking_a_permit_execution_slot`
- `TestUnderstandingListenerCancelInvariant`

未修改生产代码或公开行为；新增文件为 `ANALYSIS.md` 和 `understanding_executor_chains_test.go`。

## 8. 无法确认或有意未断言的事项

- 没有测试 timeout callback 与父 context cancel 在完全同一调度瞬间触发时谁赢；源码显示这是并发 CAS/取消写入，本文只把它列为风险，不把某一方写成必然结果。
- 新增 hedge 测试证明 breaker 的晚到失败可以发生在 outer listener 之后，但没有断言所有生产使用中必然如此；若用户函数立即响应取消，晚到事件可能与 outer done 非常接近。
- timeout policy 的 builder 没有公开 fake clock 注入；测试使用 0 duration 触发真实 timeout 分支，并用 channel barrier 固定 listener 与取消后的返回顺序。测试中的 `time.After(time.Second)` 只作为 fail-fast watchdog，不用于猜测成功时序。
