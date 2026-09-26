# 本地分布式信号量（distributed-semaphore-go）

一个在**单进程内以多个独立实例模拟分布式节点**的命名信号量服务。多个实例通过进程内消息总线（`bus`）协调同一命名信号量的有限许可；申请与释放可在任一实例发起；实例重启或持有者异常时，许可不会泄漏也不会永久丢失。不依赖任何外部服务（无 Redis / ZooKeeper / etcd）。

## 安装与使用

```go
b := semaphore.NewBus()

owner, _ := semaphore.NewInstance(semaphore.Config{
    InstanceID:       "node-a",
    DataDir:          "./data/node-a", // 持久化目录；"" 表示纯内存
    DefaultLeaseTTL:  10 * time.Second,
    TickInterval:     25 * time.Millisecond,
    SnapshotAfter:    1000,
}, b)
defer owner.Close()

ctx := context.Background()
_ = owner.EnsureSemaphore(ctx, "db-pool", 5, semaphore.FIFO)

lease, err := owner.Acquire(ctx, "db-pool", semaphore.AcquireOptions{
    Count:    1,        // 一次申请多个许可（原子）
    Priority: 0,        // 仅 Priority 策略下有意义
    Timeout:  3 * time.Second, // 等待排队的超时；0 表示随 ctx
})
if err != nil { /* 处理 */ }

// lease.Fence 为 fencing token；lease.ExpiresAt 为租约到期时间
if err := owner.Release(ctx, lease); err != nil { /* 处理 */ }
```

## 架构

- **确定性属主（owner）**：每个信号量按 `hash(name)` 在当前集群成员（按 ID 排序）中选出唯一 owner。所有 `Acquire / Release / Renew / Cancel / Preempt` 都通过进程内 `bus` 路由到 owner。排序保证所有实例对“谁是 owner”得出一致结论。
- **WAL + 快照**：owner 对每一次状态转换先追加落盘（`fsync`），成功后才修改内存。每个信号量在 owner 的 `DataDir` 下有独立文件：
  - `snapshot-<name>.json`：压缩快照（原子 rename）；
  - `events-<name>.log`：快照之后的增量事件。

  状态 = 快照 + 其后事件。事件序号（`Seq`）单调，重放幂等。
- **租约回收循环**：每个实例以 `TickInterval` 扫描其拥有的信号量，回收到期持有者（释放许可并尝试唤醒等待者）与超时等待者（删除占位）。
- **心跳续约**：获得许可的实例默认每 `TTL/3` 自动续约；续约成功会拿到新 fencing token。心跳只是优化，**权威安全网是 owner 端租约到期回收**，所以心跳失败/进程崩溃都不会破坏状态。

## 一致性语义

### 1. 许可互斥与复用
同一许可任何时刻只授予一个持有者；释放或租约回收后立即可复用。`used + 等待授予` 永远不超过 `permits`。

### 2. 排队策略
- `FIFO`：严格按到达顺序服务；
- `Priority`：按优先级从高到低，同优先级保持到达顺序。

队列采用**严格队头阻塞**：队头请求放不下时，不绕过它去服务后面更小的请求，避免多许可请求被“碎片”饿死。

### 3. 多许可原子申请
`Count > 1` 的申请要么一次性全部获得，要么整体进入等待；不会部分授予。释放/回收触发的唤醒也在**同一批事件**中原子完成（释放事件 + 若干授予事件共用一次 WAL 追加与一次状态应用），许可计数与等待队列任何时刻自洽。

### 4. 取消与超时不泄漏
- `ctx` 取消会向 owner 发送 `cancel`，owner 原子删除该等待占位；
- 等待 `Timeout`（或 ctx deadline）到期由 owner tick 删除占位；
- 取消与授予竞争时：若 owner 刚授予，调用方会立即归还该许可，不泄漏。
- 删除等待占位后，后续等待者才能前移。

### 5. Fencing token（防护令牌）
- 每个信号量有单调递增的 fencing 序号；每次**授予**和每次**续约**都产生新 token；
- 续期必须携带“最新” token；旧 token 立即失效；
- 持有者身份失效（租约到期回收 / 被 `Preempt` 抢占）后，其旧 token 发起的 `Release` 为**幂等空操作**（持有者已不存在，不会重复增加可用许可、不会错误唤醒等待者），`Renew` 返回 `ErrStaleFencingToken`；
- token 不匹配的释放返回 `ErrStaleFencingToken`，且不改变任何状态。

### 6. 幂等释放
重复释放同一持有者、释放已回收/已抢占的持有者：不产生事件、不增加可用许可、不唤醒等待者，返回成功。

### 7. 租约与崩溃恢复
- 所有授予/释放/排队/续约都先持久化后生效；
- owner 重启时重放快照+日志，恢复持有者集合、fencing 序号与等待队列；不会重复授予，也不会丢失可用许可；
- 已入队但授予通知未确认（`notified` 标记未落盘）的许可会在重启后重新推送；通知失败会重试，最终兜底是该租约到期自动回收，不会永久丢失许可。
- 持有者崩溃 → 租约到期自动回收；等待者崩溃 → 其排队超时到期自动移除。

### 8. 抢占（Preempt）
`Preempt(ctx, name, holderIDs)` 原子撤销指定持有者并在同一转换中唤醒可服务的等待者（典型用于高优先级）。被抢占者的 fencing token 立即失效，其后续释放/续约无法影响其他持有者。

## 错误

| 错误 | 含义 |
| --- | --- |
| `ErrStaleFencingToken` | token 不是最新（已续约/已抢占/身份失效） |
| `ErrAcquireCanceled` | 等待被 ctx 取消 |
| `ErrAcquireTimeout` | 等待排队超时 |
| `ErrWaiterNotFound` | 等待者不存在（内部/已完成） |
| `ErrNoRoute` | 集群中没有可路由的 owner 实例 |
| `ErrClosed` | 实例已关闭 |
| `ErrInvalidPermitCount` | 许可数非法（如 0/负数） |
| `ErrUnknownSemaphore` | 信号量未创建 |

## 配置项（`Config`）

| 字段 | 默认 | 说明 |
| --- | --- | --- |
| `InstanceID` | 必填 | 集群内唯一实例 ID；owner 路由依据 |
| `DataDir` | `""`（纯内存） | WAL/快照目录；生产与崩溃恢复测试需要持久化 |
| `DefaultLeaseTTL` | `10s` | 授予许可的租约时长，也是自动续约周期基准（`TTL/3`） |
| `TickInterval` | `25ms` | owner 扫描租约/等待超时的周期 |
| `SnapshotAfter` | `0`（不压缩） | 单个信号量增量事件达到该阈值后触发快照压缩 |
| `Clock` | 系统时钟 | 注入假时钟用于确定性测试 |

`AcquireOptions`：

| 字段 | 说明 |
| --- | --- |
| `Count` | 原子申请的许可数（默认 1） |
| `Priority` | 优先级，仅 `Priority` 策略生效（大者优先） |
| `Timeout` | 排队最大等待时长；0 表示随调用 ctx |

## 范围与限制（模拟环境）

- 多节点是**同一进程内**的独立实例，网络由内存 `bus` 模拟，不模拟消息丢失/分区；跨进程部署不在范围内。
- 属主身份由当前存活成员的确定性哈希决定；**不实现自动故障转移**（owner 宕机后，以相同 `InstanceID` 重启即恢复其名下信号量）。
- `EnsureSemaphore` 要求同名信号量以相同 `permits/strategy` 创建，否则返回错误。
- 等待/持有者 ID 内部编码为 `<前缀>-<InstanceID>-<随机串>` 用于跨实例回传，因此 **InstanceID 不应包含 `-`**；持有者/等待者标识由库自动生成，调用方只需保存返回的 `Lease`。

## 测试

```bash
go test ./... -race
```

覆盖：
- 租约到期自动回收并转交等待者；
- fencing token 续约轮换与抢占失效；
- 多实例并发申请/释放（不超卖、可达满容量、最终状态自洽）；
- 幂等释放、陈旧 token 拒绝；
- 多许可原子申请（不部分授予、不被小请求越队）与等待取消/超时释放占位；
- 崩溃恢复（owner 与持有者重启后恢复持有者与队列，许可不重发、不丢失、可复用）；
- 快照压缩与恢复。
