# 实验5：并发控制

本实验在NJUDB中实现基于两阶段锁协议（Two-Phase Locking，2PL）的记录级并发控制。必做部分包括锁管理、死锁处理以及事务提交与回滚；选做部分实现事务级快照隔离。

## 实验前置要求

开始前请先完成Lab01–Lab04，或确认对应gold libraries能支持数据库完整运行。你需要能够沿executor、表句柄、索引句柄和事务管理器追踪一次记录修改，并掌握：

- 事务的增长、收缩、提交和回滚状态及其基本语义；
- 隔离级别、共享锁、排他锁、意向锁、锁兼容矩阵和锁升级；
- 两阶段锁与严格两阶段锁的区别，以及GROWING/SHRINKING状态转换；
- 条件变量的等待谓词、唤醒和互斥保护，能够分析多线程执行时间线；
- Wait-Die、Wound-Wait或等待图检测等死锁处理方法；
- 表记录、索引项、事务写集合和锁集合在提交或回滚时的一致性。

实验代码中的入口统一使用`NJUDB_STUDENT_TODO(l5, ...)`标记；Bonus使用`NJUDB_STUDENT_TODO(l5_bonus, ...)`标记。开始编码前建议先阅读`transaction.h`、`lock_manager.h`和`txn_manager.h`，画出一次读、写、提交和回滚的调用链。

## 预备知识

### 并发执行与事务隔离

数据库允许多个事务交错执行，以提高处理器、内存和存储设备的利用率，并降低短事务的等待时间。交错执行必须保持事务隔离性，使每个事务观察到的数据库状态符合所选隔离级别。若两个事务缺少协调，即使每条语句单独执行都正确，组合结果仍可能违反业务约束。

考虑余额为100的账户A。事务T1从A支出20，事务T2向A存入50。两个操作按照任一串行顺序执行时，最终余额都应为130。下面的交错使两个事务基于同一个旧值计算，T1的修改被T2覆盖，形成丢失更新（lost update）。

| 时刻 | 事务T1 | 事务T2 | 数据库中的A |
|---|---|---|---:|
| 1 | 读取A=100 |  | 100 |
| 2 |  | 读取A=100 | 100 |
| 3 | 写入A=80 |  | 80 |
| 4 |  | 写入A=150 | 150 |

如果T2在T1提交前读取A=80，随后T1回滚，T2便使用了从未生效的状态，这种现象称为脏读。类似的交错还可能产生不可重复读和幻读。本实验必做部分实现读已提交和可重复读。读已提交在一次记录读取结束后释放共享锁；可重复读将共享锁持有至事务结束。两种隔离级别的写锁都持有至事务提交或回滚。读未提交的无读锁路径由框架提供。MVCC Bonus使用事务开始时的一致快照，使同一事务中的多次读取基于同一个可见版本边界。

### 锁、资源层次与SS2PL

锁把事务对资源的访问意图登记为共享状态。共享锁（Shared Lock，S）允许多个读者同时访问同一资源；排他锁（Exclusive Lock，X）要求持有者独占资源。读操作通常申请S锁，写操作需要X锁。当请求与已授予锁不兼容时，请求事务进入等待状态，直至阻塞条件消失或事务被终止。

NJUDB按照表、页、元组三个层次管理资源。事务若要在较低层次加锁，需要先在祖先资源上登记意向锁。意向共享锁（IS）表示事务计划在后代资源上申请S锁；意向排他锁（IX）表示计划在后代资源上申请X锁；共享意向排他锁（SIX）组合了当前资源的共享访问与后代资源的排他意图。意向锁使表级请求可以仅检查表级锁队列，就能判断其他事务是否正在访问表内页面或元组，无需遍历全部低层资源。层次关系也约束释放顺序，低层锁应先于其祖先锁释放，否则表级状态无法准确概括后代访问。

两阶段锁协议（Two-Phase Locking，2PL）把事务生命周期划分为增长阶段和收缩阶段。事务在增长阶段获取或升级锁；一旦开始释放锁，便进入收缩阶段，不能再获取新锁。该约束能够保证冲突可串行化。严格两阶段锁（Strict 2PL，SS2PL）进一步要求写锁保持到事务提交或回滚，从而阻止其他事务读取或覆盖尚未确定的写入，并避免级联回滚。不同隔离级别可以对读锁的持有时间采用不同规则，但写锁在事务终结前不能提前暴露。

锁升级会把事务已经持有的较弱锁转换为较强锁。例如，事务先读取元组并持有S锁，随后更新该元组时需要升级为X锁。升级过程必须与普通请求共同遵守兼容性和队列顺序；同一资源若同时存在多个升级者，它们可能各自持有S锁并等待对方释放，因此系统通常限制同一时刻只有一个有效升级者。

### 等待、死锁与事务终结

等待使互相冲突的事务能够按顺序完成，也会引入死锁风险。若T1持有账户A的X锁并等待账户B，而T2持有B的X锁并等待A，两个事务之间形成循环等待。Wait-Die和Wound-Wait根据事务时间顺序决定等待或回滚，用确定性的优先关系破坏环形成的条件；死锁检测允许事务先等待，再从等待图中寻找环并选择牺牲者。三种策略的共同要求是保证系统最终取得进展，并在事务退出时完整清理等待请求、已持有锁和唤醒关系。

事务提交和回滚构成并发控制的终结阶段。提交确认事务修改成为数据库状态的一部分，然后释放协议要求持有到终点的锁。回滚需要撤销事务已经完成的表记录和索引修改，使数据库恢复到事务开始前可接受的状态，再释放锁并唤醒等待者。数据、索引、事务状态、写集合与锁集合共同描述一个事务的效果；其中任何部分提前对其他事务可见，都会形成中间状态。理解这一整体关系有助于分析“测试挂起”“回滚后索引仍包含键”或“同一事务被两个线程同时终结”等问题。

### MVCC的基本思想

多版本并发控制（Multi-Version Concurrency Control，MVCC）为同一逻辑记录保留多个版本。事务开始时获得读时间戳，并按照该时间点解析版本链，因此旧快照可以在写事务产生新版本时继续读取已经提交的历史版本。可见性判断需要区分已提交版本、其他事务的未提交版本、当前事务自己的写入以及逻辑删除。快照隔离通常允许读者与写者并发，同时要求同一记录上的并发写入通过写冲突规则选出合法结果。

版本不能无限保留。只要仍有活跃事务可能读取某个历史版本，该版本就不能回收；当版本早于所有活跃快照且不再参与可见性解析时，垃圾回收器才可以安全清理。watermark概括最老活跃快照的位置，是判断历史版本是否仍可能被访问的安全边界。MVCC改变了读写协调方式，但事务提交、回滚、索引一致性和写入原子性仍然需要作为一个整体维护。

总结，2PL的完整调用链如下。`TxnManager::Begin`创建事务后，执行器通过`TableHandle`申请表级意向锁和记录锁。`LockManager`根据锁兼容关系决定授予或等待，并在出现循环等待时执行死锁处理。事务提交时释放锁；事务回滚时先恢复索引和表记录，再释放锁。事务状态、锁队列和写集合必须在这条路径上保持一致。MVCC Bonus使用另一条路径。事务开始时获得读时间戳，读取操作据此从数据页或undo record中选择可见记录。写入操作先准备版本，再修改数据页、索引和事务写集合。提交操作发布版本的提交时间戳，回滚操作恢复物理状态并废弃未提交版本。当watermark推进到安全边界后，垃圾回收器才能清理旧版本和已删除记录。

## 仅完成本实验时的配置

开始Lab05必做部分时，修改`configure.sh`顶部配置，让Lab05从源码构建、其余实验使用gold。

```bash
SOURCE_LABS="05"
ENABLE_LAB05_MVCC=false
ENABLE_LAB06_WAL=false
BUILD_TYPE="Debug"
CLEAN_BUILD=true
```

然后执行：

```bash
./configure.sh
cmake --build build -j
```
完成Bonus后只需把`ENABLE_LAB05_MVCC`改为`true`，重新运行`./configure.sh`并编译。首次切换配置时使用`CLEAN_BUILD=true`；干净配置成功后可以改回`false`。

## 1.学习目标

完成本实验后，你应当能够：

- 根据多粒度锁兼容矩阵判断锁请求能否授予；
- 实现表、页、元组三级资源上的加锁、解锁与锁升级；
- 理解GROWING/SHRINKING状态转换以及不同隔离级别的释放规则；
- 使用Wait-Die、Wound-Wait或等待图检测处理死锁；
- 在提交和回滚时维护数据、索引与锁的一致性；
- Bonus：实现时间戳、版本链、快照可见性、原子写协调与垃圾回收。

## 2.代码导读

| 模块 | 主要文件 | 职责 |
|---|---|---|
| 事务对象 | `src/concurrency/transaction.h` | 保存事务状态、隔离级别、协议、锁集合和写集合 |
| 锁管理器 | `src/concurrency/lock_manager.{h,cpp}` | 锁队列、兼容性、升级、等待、死锁处理 |
| 事务管理器 | `src/concurrency/txn_manager.{h,cpp}` | Begin/Commit/Abort生命周期 |
| 表访问 | `src/system/handle/table_handle.cpp` | 在实际读写前后接入表锁和元组锁 |
| MVCC（Bonus） | `timestamp_oracle.*`、`version_chain.*`、`version_manager.*` | 时间戳、水位线、版本组织与可见性 |
| MVCC写入（Bonus） | `src/execution/mvcc_write_coordinator.*` | 将版本、表页、索引和事务写集合组成原子操作 |
| GC（Bonus） | `garbage_collector.*` | 按最老活跃快照清理不可达版本和墓碑 |

锁队列中的请求按到达顺序排列。已授予请求和等待请求共享同一队列；实现时必须同时保护队列、升级者状态以及事务持有锁集合，不能只修改其中一处。

## 3. SS2PL（必做，90pts）

### t1：锁兼容与升级规则

本阶段建立锁请求能否进入临界区的基本规则。系统包含`S`、`X`、`IS`、`IX`、`SIX`五种锁模式。

| 函数 | 输入 | 输出或状态变化 | 在本阶段中的作用 |
|---|---|---|---|
| `LockRequestQueue::CanGrant` | 请求事务ID `txn_id`和目标锁模式`lock_mode`；锁队列由当前对象提供 | 返回`bool`；不修改队列 | 判断目标锁是否与队列中已经授予给其他事务的锁兼容 |
| `LockManager::AreLocksCompatible` | 已持有锁模式`held`和待申请锁模式`requested` | 返回两个锁模式能否共存 | 为普通请求、等待唤醒和死锁处理提供统一的兼容性判断 |
| `LockManager::CanLockUpgrade` | 当前锁模式`old_mode`和目标锁模式`new_mode` | 返回该模式转换是否合法 | 相同模式返回`true`；拒绝降级和无效转换，并允许实现中定义的增强方向 |

`CanGrant`回答当前请求是否与已授予请求兼容，`CanLockUpgrade`回答同一事务能否改变已有锁模式。这两个问题的输入和约束不同。实现前应先写出5×5兼容矩阵，再根据锁模式的强弱关系列出合法升级方向。

### t2：通用加锁、解锁与表访问调用链

本阶段把表、页和记录三类资源的公共逻辑集中到通用加锁与解锁函数中。

| 函数 | 输入 | 输出或状态变化 | 在本阶段中的作用 |
|---|---|---|---|
| `LockManager::GenericLock` | 事务、目标锁模式、资源标识、对应锁表、锁集合登记与撤销回调、错误信息中的资源名称 | 成功时返回`true`并更新锁队列和事务锁集合；预防策略中止请求时返回`false`；非法状态或非法升级时抛出异常 | 处理重复申请、锁升级、立即授予、等待和死锁策略，是三类资源加锁入口的公共实现 |
| `LockManager::GenericUnlock` | 事务、资源标识、对应锁表、锁集合撤销回调，以及是否强制解锁`force_unlock` | 成功时返回`true`，移除锁请求和事务锁集合记录，唤醒等待者；非法释放时中止事务并抛出异常 | 根据隔离级别处理事务阶段转换，并完成单个资源的释放 |
| `LockManager::UnlockAll` | 待终结事务`txn` | 无返回值；依次清空记录锁、页锁和表锁，并删除该事务的等待图边 | 在提交或回滚结束时一次性释放事务持有的全部锁 |

框架已经提供`TableHandle::AcquireTableLock`、`AcquireTupleLock`、`ShouldAcquireReadLock`和`ReleaseTupleLockImmediately`。2PL读操作通过这些函数进入锁管理器。读未提交不申请读锁；读已提交在一次记录读取后释放共享锁；可重复读将共享锁持有至事务结束。正常执行路径由事务终结阶段通过`UnlockAll`统一释放写锁。

`GenericLock`的等待条件必须在循环中重新检查。条件变量被唤醒只表示队列状态可能发生变化。锁升级或等待失败时，还要同步清理请求、升级标志、等待图边和事务锁集合。`UnlockAll`按照“记录→页→表”的顺序释放层次化锁。

### t3：死锁预防与检测

本阶段处理不能立即授予的锁请求。系统提供Wait-Die、Wound-Wait和等待图检测三种策略。

| 函数 | 输入 | 输出或状态变化 | 在本阶段中的作用 |
|---|---|---|---|
| `LockManager::ApplyPreventionProtocol` | 请求事务、目标资源的锁队列和目标锁模式 | 返回`GRANT`、`WAIT`、`ABORT_SELF`或`ABORT_HOLDERS` | 根据当前死锁策略、冲突持有者和事务时间顺序选择后续动作 |
| `LockManager::BuildWaitForGraph` | 无显式参数；读取表、页和记录锁表 | 无返回值；重建`wait_for_graph_` | 为每个等待事务加入指向当前持有者的边 |
| `LockManager::DFS` | 当前事务ID、已访问集合、递归栈和牺牲者输出指针 | 返回是否发现环；发现环时写入待中止事务ID | 在等待图中执行深度优先搜索 |
| `LockManager::HasCycle` | 牺牲者输出指针`abort_txn_id` | 返回等待图是否有环，并通过输出参数给出牺牲者 | 遍历等待图并调用`DFS`完成环检测 |

Wait-Die允许老事务等待，年轻事务遇到更老的冲突持有者时中止。Wound-Wait允许老事务中止冲突的年轻持有者，年轻事务等待老事务。Detection先允许事务进入等待，再周期性重建等待图并检测环。中止事务后必须清理其等待请求和已持有锁，否则其他事务仍无法继续。

### t4：事务生命周期和物理回滚

本阶段管理事务从创建到提交或回滚的完整生命周期。

| 函数 | 输入 | 输出或状态变化 | 在本阶段中的作用 |
|---|---|---|---|
| `TxnManager::Begin` | 事务ID、隔离级别和并发控制协议 | 无返回值；创建`Transaction`，设置初始状态和时间顺序，并加入活动事务表 | 建立后续锁请求和死锁策略所需的事务状态 |
| `TxnManager::Commit` | 事务ID | 无返回值；取得唯一终结权，完成协议相关提交动作，将事务设为`COMMITTED`并移出活动事务表 | 2PL路径释放事务持有的全部锁；MVCC路径的处理属于Bonus |
| `TxnManager::Abort` | 事务ID和用于访问表、索引的`DatabaseHandle` | 无返回值；将事务设为`ABORTED`，逆序恢复修改，释放锁并移出活动事务表 | 撤销事务对索引和表记录的影响，使失败事务不留下可见的中间状态 |

`ClaimTransactionForEnd`和`FinishTransaction`已经给出，用于保证同一事务只有一个线程执行终结路径。2PL回滚先逆序恢复索引写集合，再逆序恢复表写集合，最后释放锁。恢复顺序与原操作相反，避免回滚过程中暴露数据与索引不一致的状态。

BEGIN、COMMIT、CLR、ABORT的日志链接和强制刷盘已经作为跨实验框架提供；这些调用不会替代本实验的事务状态、物理回滚和锁释放TODO。

## 4. MVCC与快照隔离（10pts，选做）

Bonus不要求2PL事务使用版本链，也不能破坏SS2PL的语义。

### b1：时间戳与水位线

MVCC事务和垃圾回收共同使用的时间顺序如下

| 函数 | 输入 | 输出或状态变化 | 在本阶段中的作用 |
|---|---|---|---|
| `TimestampOracle::GetNextTimestamp` | 无 | 返回当前可分配时间戳，并推进全局时间线 | 为事务开始和提交分配严格递增的时间戳 |
| `TimestampOracle::GetCurrentTimestamp` | 无 | 返回当前时间线，不修改状态 | 查询尚未分配的当前边界 |
| `TimestampOracle::RegisterTransaction` | 事务读时间戳`read_ts` | 无返回值；把读时间戳加入活动集合 | 标记仍可能访问历史版本的快照 |
| `TimestampOracle::UnregisterTransaction` | 事务读时间戳`read_ts` | 无返回值；从活动集合移除该时间戳 | 在事务终结后解除其对历史版本回收的限制 |
| `TimestampOracle::GetWatermark` | 无 | 返回最小活动读时间戳；没有活动事务时返回当前时间线 | 给出版本裁剪和物理删除的安全边界 |
| `TimestampOracle::GetActiveTransactionCount` | 无 | 返回活动读时间戳数量 | 用于测试和运行状态检查 |

### b2：版本链与可见性

本阶段管理单个逻辑记录的undo版本链。数据页保存最新物理值，undo record保存写入前的记录。

| 函数 | 输入 | 输出或状态变化 | 在本阶段中的作用 |
|---|---|---|---|
| `VersionChain::CanInsertVersion` | 无显式参数；读取链头 | 返回链头是否允许加入新版本 | 在准备写入前快速检查是否存在未提交写者 |
| `VersionChain::InsertVersion` | 新的`UndoRecord*` | 无返回值；在排他临界区内再次检查冲突，并把版本插入链头 | 原子完成写写冲突复查和未提交undo记录登记 |
| `VersionChain::RemoveUncommittedHead` | 待撤销的版本指针 | 返回是否成功移除；成功时修改链头和版本数量 | 在后续物理写入失败时撤销刚准备的版本 |
| `VersionChain::PrepareExistingWrite` | 写事务ID和本次操作是否为删除 | 返回`ExistingVersionPreparation` | 同一事务重复写时复用未提交链头；其他事务持有链头时报告冲突 |
| `VersionChain::RestoreExistingWrite` | 版本指针和修改前的删除标志 | 无返回值；恢复版本的删除状态 | 撤销一次失败的重复写准备 |
| `VersionChain::CanReclaimDelete` | 预期链头和watermark | 返回已提交删除是否可以物理回收 | 确认没有活动快照仍需要被删除记录的旧值 |
| `VersionChain::ResolveVisibility` | 读时间戳和读事务ID | 返回`READ_PAGE`、`READ_UNDO`或`NOT_VISIBLE`，必要时携带旧记录副本 | 决定读取当前页、历史记录或跳过该记录 |
| `VersionChain::PruneVersions` | 最老活动快照的时间戳 | 返回删除的undo record数量，并裁剪链表 | 清理已回滚版本和不会再被任何快照访问的历史后缀 |

可见性判断必须覆盖当前事务自己的写入、其他事务的未提交写入、已提交版本和已回滚版本。同一记录的冲突检查和链头修改需要在同一个排他临界区内复查，避免检查与插入之间发生竞态。

### b3：版本管理与事务接入

本阶段把单条版本链接入全局版本表、事务生命周期和逻辑删除回收。

| 函数 | 输入 | 输出或状态变化 | 在本阶段中的作用 |
|---|---|---|---|
| `VersionManager::PrepareVersion` | 事务、表ID、RID、写入前记录和删除标志 | 返回`VersionPreparation`，其中包含版本指针、是否新建、旧删除标志和版本链句柄 | 获取或创建版本链，复用本事务链头或为首次写入创建undo record |
| `VersionManager::RollbackPreparedVersion` | 事务和`VersionPreparation` | 无返回值；删除新建链头，或恢复复用链头的原状态 | 撤销版本准备，供物理写或索引写失败时调用 |
| `VersionManager::RegisterPendingDelete` | 表、相关索引、删除前记录和删除版本 | 无返回值；登记待物理回收记录 | 保存回收记录所需的表、索引和版本信息 |
| `VersionManager::CancelPendingDelete` | 表、RID和删除版本 | 无返回值；移除匹配的待回收项 | 在删除操作失败时撤销登记 |
| `VersionManager::ResolveVisibility` | 表ID、RID、读时间戳和读事务ID | 返回`VersionVisibilityResult` | 将读取请求路由到对应表记录的版本链；没有版本链时读取当前页 |
| `VersionManager::PruneVersions` | watermark | 返回所有版本链合计裁剪的版本数 | 遍历全局版本表并清理旧版本 |
| `VersionManager::ReclaimDeletedTuples` | watermark | 返回完成物理回收的记录数，并删除相应索引项、表记录和版本链 | 在旧快照不再需要记录后完成逻辑删除的物理清理 |
| `VersionManager::GetPendingDeleteDPT` | 无 | 返回待回收删除记录涉及的页面及其最早恢复LSN | 让Lab06检查点保留尚未完成物理回收的删除页面 |
| `RollbackMVCCWrites` | 待回滚事务和数据库句柄 | 无返回值；逆序恢复事务的索引写集合和表写集合 | 为MVCC运行期回滚恢复最新物理状态 |
| `TxnManager`的MVCC分支 | 事务ID、协议和事务已有的undo records | Begin注册读时间戳；Commit发布提交时间戳；Abort废弃版本并注销读时间戳 | 把快照和版本状态接入事务开始、提交与回滚 |

版本准备只建立逻辑写入的前置状态。物理页或索引修改失败时，调用者必须使用`VersionPreparation`恢复版本链。MVCC事务提交后，undo record获得提交时间戳；事务回滚时，物理状态和版本状态都要恢复。

### b4：原子写协调与GC

本阶段完成MVCC写入的跨组件原子性，并定期回收失效状态。

| 函数 | 输入 | 输出或状态变化 | 在本阶段中的作用 |
|---|---|---|---|
| `MVCCWriteCoordinator::Insert` | 表、索引列表、待插入记录、MVCC事务和版本管理器 | 成功时返回新RID，并更新版本链、数据页、索引和事务写集合；失败时恢复已完成步骤 | 保证新RID的版本占位先于记录对其他事务可见 |
| `MVCCWriteCoordinator::Update` | 表、索引列表、旧记录、新记录、MVCC事务和版本管理器 | 无普通返回值；成功时原子更新版本、数据页、索引和写集合，失败时恢复原状态 | 协调一次记录更新的全部参与组件 |
| `MVCCWriteCoordinator::Delete` | 表、索引列表、删除前记录、MVCC事务和版本管理器 | 无普通返回值；建立删除版本并登记延迟物理回收，失败时撤销登记和版本准备 | 让旧快照仍能通过undo record读取被逻辑删除的记录 |
| `GarbageCollector::CollectGarbage` | 无显式参数；使用版本管理器和时间戳管理器 | 无返回值；按watermark回收删除记录、裁剪旧版本并更新统计 | 执行一次完整的全局垃圾回收 |
| `GarbageCollector::PruneTuple` | 表ID和RID | 返回该记录裁剪的版本数，并更新累计统计 | 对指定记录执行同步版本裁剪 |

一次写入会同时影响版本链、数据页、若干索引和事务写集合。任一阶段失败时，都要按相反顺序恢复已经完成的步骤。GC必须与前台写入共享版本链同步规则，不能回收仍被活动快照引用的状态。

## 5. 实验问题

请结合你的代码位置、事务时间线和测试证据回答下面问题。

1. 选择一个“请求必须等待、随后被授予或终止”的锁队列场景，说明队列、升级者标记、事务锁集合和事务状态在各时刻如何变化；你的实现如何避免重复授予或遗留waiter？
2. 说明你实际实现的死锁处理方案。给出至少两个事务形成冲突的具体时间线，解释何时发现冲突、如何选择牺牲者，以及牺牲者退出后由谁清理请求并唤醒其他事务。
3. 选择一次包含表记录和索引修改的事务回滚，沿executor、表句柄、事务写集合与锁管理器说明恢复顺序；如果其中一步顺序错误，哪个测试或SQL现象能够暴露数据与索引不一致？
4. 描述一个你实际调试过的并发失败，包括线程交错、最初现象、用于缩小范围的日志或测试，以及原因。完成MVCC Bonus的同学可以再说明该场景在快照隔离路径下的可见性或写冲突结果有何不同。

## 6. 测试与评分入口

### 6.1. 全部Lab05单元测试

| 可执行程序 | 源文件 | 定义数 | 范围 | 性质 |
|---|---|---:|---|---|
| `lock_manager_test` | `test/concurrency/lock_manager_test.cpp` | 20 | 五种锁模式、升级、层次锁、三种死锁策略、隔离级别 | 必做 |
| `strict_2pl_test` | `test/concurrency/strict_2pl_test.cpp` | 7 | 持锁到提交、并发终结、多资源锁与死锁 | 必做 |
| `two_pl_index_abort_test` | `test/concurrency/two_pl_index_abort_test.cpp` | 3 | INSERT/UPDATE/DELETE回滚后的表索引一致性 | 必做 |
| `version_chain_test` | `test/concurrency/version_chain_test.cpp` | 24 | undo record、可见性、重复写、裁剪、并发句柄安全 | Bonus |
| `timestamp_mvcc_test` | `test/concurrency/timestamp_mvcc_test.cpp` | 14 | 时间戳、水位线、事务管理、版本管理 | Bonus |
| `snapshot_isolation_test` | `test/concurrency/snapshot_isolation_test.cpp` | 6 | 快照可见性、读己之写、提交时间戳 | Bonus |
| `garbage_collector_test` | `test/concurrency/garbage_collector_test.cpp` | 10 | 前后台GC、活跃快照、墓碑与并发 | Bonus |
| `mvcc_write_conflict_test` | `test/concurrency/mvcc_write_conflict_test.cpp` | 6 | 顺序/并发写冲突、TOCTOU、异常安全 | Bonus |
| `mvcc_atomic_write_test` | `test/concurrency/mvcc_atomic_write_test.cpp` | 19 | 页/索引/版本原子性、重复写、失败恢复、逻辑删除 | Bonus |

列出某个二进制中的每一个展开后用例：

```bash
./build/bin/lock_manager_test --gtest_list_tests
./build/bin/mvcc_atomic_write_test --gtest_list_tests
```

运行必做测试：

```bash
cmake --build build --target lock_manager_test strict_2pl_test two_pl_index_abort_test -j
./build/bin/lock_manager_test
./build/bin/strict_2pl_test
./build/bin/two_pl_index_abort_test
```

运行包括Bonus在内的全部并发测试：

```bash
ctest --test-dir build --output-on-failure \
  -R 'lock_manager|strict_2pl|two_pl_index_abort|version_chain|timestamp_mvcc|snapshot_isolation|garbage_collector|mvcc_write_conflict|mvcc_atomic_write'
```

### 6.2. 全部Lab05 SQL并发测试

`test/sql/lab05/2pl/lab05_2pl_concurrency.py`包含15个场景：读读兼容、写阻塞读、写写串行、回滚唤醒、不同RID并发、二事务环、8客户端分片更新、1写7读、5写热点、四事务等待图、READ COMMITTED提前释放读锁、多行事务串行、7客户端混合DML、唯一索引回滚重试、8客户端完整DML工作流。

`test/sql/lab05/mvcc/lab05_mvcc_concurrency.py`包含15个Bonus场景：未提交更新旧值可见、读己之写、旧快照稳定、插入提交前后可见性、删除快照、同RID单赢家、冲突不破坏索引、重复更新、重复更新删除回滚、8客户端分片更新、5写热点拒绝、7客户端混合DML、唯一键回滚重试、8客户端完整DML、MVCC/2PL数据库共存。

服务器启动后可使用：

```bash
bash test/sql/lab05/2pl/evaluate.sh build/bin test/sql/lab05
bash test/sql/lab05/mvcc/evaluate.sh build-mvcc/bin test/sql/lab05
```

## 7. 推荐实现顺序与调试

1. 只实现t1，先跑兼容矩阵和升级相关filter。
2. 实现t2的无竞争路径，再加入等待、升级、解锁与隔离级别。
3. 实现t3，并对每个等待点确认队列清理和唤醒。
4. 实现t4，最后跑表/索引回滚和SQL多客户端测试。
5. 必做稳定后，再按b1 → b2 → b3 → b4完成Bonus。

常见问题：

- 测试挂住：通常是条件变量等待条件、漏唤醒、互斥锁重入或牺牲者锁未释放。
- 参数化兼容测试失败：检查枚举顺序是否和矩阵行列一致。
- 回滚后索引错误：检查写集合是否逆序处理，以及旧/新tuple是否使用正确。
- 偶现双写成功：冲突检查和版本插入必须在同一个排他临界区内再次确认。

## 8. 实验提交
提交前检查清单
- 必做部分通过`lock_manager_test`、`strict_2pl_test`和`two_pl_index_abort_test`；
- 选做MVCC时，额外运行全部Bonus单元测试与`mvcc/evaluate.sh`；
- 运行2PL SQL评分脚本，确认串行用例和15个并发场景全部通过；
- 确认没有提交`build/`、`data/`、SQL `output/`或其他运行产物。

提交要求

1. 提交一份PDF格式的实验报告，文件名为`lab5_学号_姓名.pdf`。报告首页应包含下列信息：

   | 学号 | 姓名 | 邮箱 | 完成题目 |
   |---|---|---|---|
   | 12345678 | 张三 | zhangsan@smail.nju.edu.cn | t1/t2/t3/t4/b1/b2/b3/b4 |

   “完成题目”只填写实际完成的阶段。报告正文应说明实现思路、关键不变量、测试结果和调试过程。请引用关键函数位置和测试证据，避免大段粘贴代码，推荐2-4页。

2. 提交代码目录`njudb/src`。不要包含构建目录、数据文件或测试输出。

3. 将报告和代码打包为`lab5_学号_姓名.zip`并上传至提交平台。解压后的根目录应当为：

```text
├── lab5_12345678_张三.pdf
└── src
    ├── CMakeLists.txt
    ├── concurrency
    ├── execution
    ├── log
    ├── storage
    └── system
```

## 致谢

本次实验由何云天（Yuntian He，ythecs.nju@gmail.com）开发测试，感谢他的贡献！
