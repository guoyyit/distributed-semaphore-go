# distributed-semaphore-go

一个**本地分布式信号量服务**：多个实例在同一进程内通过内存消息总线协调，按名称
管理有限数量的许可（permit）。申请与释放可在任意实例发起；实例重启、持有者
崩溃或租约到期时，许可不会泄漏或永久丢失。不依赖任何外部服务（无 etcd / ZooKeeper / Redis）。

## 特性

1. **按名称申请一个或多个许可**：每个命名信号量容量固定；许可按租约（lease）
   持有，带单调递增的 **fencing token**；同一许可不会同时授予两个持有者，释放后可复用。
2. **等待队列**：许可不足时整体进入队列，支持
   - 先到先得（FIFO）
   - 优先级（priority 大者优先，同优先级按到达顺序）
   等待取消与等待超时都会释放已占位的队列资源，不产生泄漏。
3. **Fencing token**：每次续期轮转新 token；持有者身份失效（租约到期/被抢占）后，
   旧 token 发起的释放或续期一律被拒绝，不能影响其他持有者。
4. **自动回收**：持有者崩溃或租约到期时，leader 将「过期回收 + 等待者提升」作为
   **一个原子批次**提交；回收过程与并发申请/释放相互隔离，许可计数与队列始终自洽。
5. **幂等释放**：重复释放同一持有者、释放已被回收的许可都是安全空操作——不会重复
   增加可用数，也不会错误唤醒等待者。
6. **多实例并发与恢复**：操作经复制日志提交；重启后从持久化日志 + 快照恢复已授予
   许可与等待队列，不重复授予、不丢失可用许可；leader 宕机自动重新选举并继续服务。
7. **多许可原子申请**：一次申请 N 个许可要么全部获得，要么全部进入等待，绝不部分授予
   而错误阻塞队首之后的等待者。

## 快速开始

```bash
go run ./cmd/demo      # 启动 3 实例集群并演示主要语义
go test ./...          # 全部自动化测试
go test ./... -race    # 带数据竞争检测
```

### 客户端示例

```go
bus := cluster.NewBus()
var nodes []*cluster.Instance
for i := 0; i < 3; i++ {
    cfg := config.DefaultConfig()
    cfg.InstanceID = fmt.Sprintf("node-%d", i)
    cfg.DataDir = filepath.Join("/tmp/sema", cfg.InstanceID)
    n, _ := cluster.NewInstance(cfg, bus)
    n.Start(context.Background())
    nodes = append(nodes, nodes)
}

cl := cluster.NewClient(nodes...).WithTTL(5 * time.Second)
cl.Init(ctx, "orders", 3) // 容量 3 的命名信号量

// 原子申请 2 个许可，最多排队等待 10 秒。
token, expiresAt, requestID, err := cl.Acquire(ctx, "orders", "worker-a", 2, 0, 10*time.Second)

token2, _ := cl.Renew(ctx, "orders", "worker-a", token) // 续期并轮转 token
cl.Release(ctx, "orders", "worker-a", token2)           // 必须携带最新 token
// cl.Cancel(ctx, "orders", requestID)                  // 取消仍在排队的申请
```

`Acquire` 返回的 `requestID` 是幂等键：网络超时或 leader 切换后用同一 `requestID`
重试（`AcquireWithID`）不会重复申请，只会重附到原结果。

## 一致性语义

### 单 leader + 多数派复制（Raft 子集）

- 每个任期（term）经选举产生唯一 leader；term / 投票结果持久化到磁盘
  （`meta.json`），防止陈旧 leader。
- 所有写操作由 leader 规划为确定性事件，先 fsync 到本地 WAL，再复制到 follower；
  收到多数派确认后才提交并应用。
- follower 的提交位点不得超过其日志长度；冲突条目按 Raft 规则回退截断（内存与
  WAL 同时截断）。
- 新 leader 就职先提交一条 **no-op 条目**，从而一并提交上一任期遗留的已复制条目
  （Raft 图 8 安全性）。

### 规划视图（防并发超发）

事件从追加到多数派提交存在窗口。leader 维护一个**推测规划视图**（已提交状态 +
未提交尾部），所有 `Plan*` 都基于它，因此并发请求不会基于陈旧可用数而全部被立即
批准；未提交条目在 leader 下台时整体丢弃。

### 原子批次与队首规则

- 多许可申请：可用数不足时只产生一条「整体等待」事件，不预占部分许可。
- 等待者提升严格按队首（head-of-line）：队首 N 个许可无法满足时不会绕过它去授予
  后面的请求——既保证 FIFO/优先级语义，也避免大请求造成计数错误。
- 租约/等待到期回收时，「过期事件 + 所有因此可满足的提升」由 leader 一次规划、
  一次 fsync、一次复制提交，任一时刻对外可见的状态要么全旧要么全新。

### Fencing token

- token 全局单调递增（每个授予、每次续期都获得新值）。
- 续期必须携带当前 token 才会成功并轮转；租约到期后持有者从授予表删除，旧 token 的
  释放/续期成为**无副作用的空操作或被拒绝**，绝不能触达新持有者。
- token 与许可状态都随事件持久化，恢复后保持单调。

### 幂等性

- 每个操作带客户端生成的唯一 ID；状态机持久化「操作结果表」，重复投递只重附原结果，
  不会二次执行。
- `Release`：持有者不存在（已释放/已过期）→ 成功空操作；token 过期 → 拒绝。两者都
  不改变可用计数。
- `Cancel`：目标已不在队列 → 成功空操作；取消已授予的请求 → 拒绝。

### 持久化与恢复

- `store.WAL`：定长帧 + CRC32 + 每条 `fsync`；崩溃导致的撕裂尾部在重启时校验并截断，
  只以最后一条完整记录为恢复点。
- `store.Snapshot`：原子写（临时文件 + rename）的状态机压缩镜像。
- 提交高水位记录在 `meta.json`：重启只重放 **已提交** 前缀，未提交尾部丢弃，由新
  leader 重新提议。已授予许可、fencing token、等待队列、等待截止时间、操作结果都从
  日志/快照重建，满足「不重复授予、不丢失可用许可」。

## 配置项（`config.Config`）

| 字段 | 默认值 | 说明 |
| --- | --- | --- |
| `InstanceID` | 必填 | 实例在进程内集群中的唯一标识，同时用于选举与数据目录。 |
| `DataDir` | `.data/<InstanceID>` | WAL、快照、term/vote/commit 元数据目录。 |
| `HeartbeatInterval` | `50ms` | leader 广播心跳/复制的间隔。 |
| `ElectionTimeout` | `200ms` | follower 多久收不到 leader 后发起选举（应显著大于心跳间隔）。 |
| `LeaseTTL` | `5s` | 申请未显式指定租约 TTL 时使用的默认值。 |

客户端侧（`cluster.Client`）：`WithTTL`（许可租约时长）、
`WithPolicy`（`QueueFIFO` / `QueuePriority`）；`Acquire` 的 `waitTimeout`
为排队等待上限（`0` 表示一直等待），`priority` 为优先级模式下的优先级。

> 计时建议：`ElectionTimeout` ≥ 4 × `HeartbeatInterval`；租约 TTL 应显著大于选举
> 超时，避免短暂无 leader 期间正常持有者被误判过期。

## 包结构

```
config/     配置定义
semaphore/  纯确定性状态机：授予/队列/fencing/租约/快照（无 I/O、无并发原语）
store/      WAL（CRC+fsync+崩溃截断）、原子快照、term/vote/commit 元数据
cluster/    单 leader 复制、选举、租约回收后台循环、客户端
cmd/demo/   端到端演示
```

## 测试覆盖

| 场景 | 测试 |
| --- | --- |
| 租约到期自动回收并提升等待者 | `TestLeaseExpirationReclaimsAndPumps`、`TestLeaseExpiryClusterReclaims` |
| 等待超时释放占位 | `TestWaiterTimeoutReleasesSlotAndGrantsNext`、`TestWaitTimeoutFreesQueueSlot` |
| fencing token 轮转与旧 token 失效 | `TestFencingTokenRotationAndStaleReject`、`TestFencingPreemptionStaleToken` |
| 并发申请/释放不超发 | `TestConcurrentAcquireReleaseNoOvergrant`、`TestMultiInstanceInitiation` |
| 释放幂等（重复/过期） | `TestFencingTokenRotationAndStaleReject` |
| 崩溃恢复（许可+队列） | `TestCrashRecoveryRestoresGrantsAndQueue` |
| 快照压缩后从快照+日志后缀恢复 | `TestCompactAndRestartFromSnapshot` |
| leader 故障切换 | `TestLeaderFailoverContinuesService` |
| 多许可原子申请与取消 | `TestAtomicMultiPermitGrantOrFullWait`、`TestMultiPermitAtomicAndCancel` |
| 优先级排队 | `TestPriorityQueueOrdering`、`TestPriorityWaitersServedInOrder` |
| WAL 撕裂截断 / 快照 / 元数据 | `store/*_test.go` |

## 已知边界（模拟环境）

- 传输为进程内非阻塞邮箱，不模拟真实网络分区；极端丢消息依赖心跳与客户端重试收敛。
- 时钟为各实例本地 `time.Now`（单进程内等价），未实现跨机时钟校正。
- 已提供并测试快照压缩 API（`Instance.compactLocked`，保存快照并重写 WAL 后缀），
  恢复路径同时支持「纯 WAL」与「快照 + 后缀」；当前未内置周期触发调度，也未实现
  Raft 的 InstallSnapshot 给落后 follower 补快照（本模拟内 follower 通常保留完整日志）。
