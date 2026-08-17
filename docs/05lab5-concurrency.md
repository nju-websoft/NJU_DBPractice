# 实验 5：并发控制

本实验在 NJUDB 中实现事务并发控制。必做部分采用严格两阶段锁协议（Strict 2PL，简称 SS2PL）；MVCC 与快照隔离是 Bonus，不影响必做部分得分。

开始前请先完成 Lab01–Lab04，并确保项目能从源码编译。实验代码中的入口统一使用 `NJUDB_STUDENT_TODO(l5, ...)` 标记；Bonus 使用 `NJUDB_STUDENT_TODO(l5_bonus, ...)` 标记。

开始 Lab05 必做部分时，修改 `configure.sh` 顶部配置，让 Lab05 从源码构建、其余实验使用 gold：

```bash
SOURCE_LABS="05"
ENABLE_LAB05_MVCC=false
ENABLE_LAB06_WAL=false
CLEAN_BUILD=true
```

然后执行：

```bash
./configure.sh
cmake --build build -j
```

默认服务器只启用 SS2PL，因此必做部分不会进入 MVCC 或 Lab06 TODO。完成 Bonus 后只需把 `ENABLE_LAB05_MVCC` 改为 `true`，重新运行 `./configure.sh` 并编译。

## 1. 学习目标

完成本实验后，你应当能够：

- 根据多粒度锁兼容矩阵判断锁请求能否授予；
- 实现表、页、元组三级资源上的加锁、解锁与锁升级；
- 理解 GROWING/SHRINKING 状态转换以及不同隔离级别的释放规则；
- 使用 Wait-Die、Wound-Wait 或等待图检测处理死锁；
- 在提交和回滚时维护数据、索引与锁的一致性；
- Bonus：实现时间戳、版本链、快照可见性、原子写协调与垃圾回收。

## 2. 代码导读

| 模块 | 主要文件 | 职责 |
|---|---|---|
| 事务对象 | `src/concurrency/transaction.h` | 保存事务状态、隔离级别、协议、锁集合和写集合 |
| 锁管理器 | `src/concurrency/lock_manager.{h,cpp}` | 锁队列、兼容性、升级、等待、死锁处理 |
| 事务管理器 | `src/concurrency/txn_manager.{h,cpp}` | Begin/Commit/Abort 生命周期 |
| 表访问 | `src/system/handle/table_handle.cpp` | 在实际读写前后接入表锁和元组锁 |
| MVCC（Bonus） | `timestamp_oracle.*`、`version_chain.*`、`version_manager.*` | 时间戳、水位线、版本组织与可见性 |
| MVCC 写入（Bonus） | `src/execution/mvcc_write_coordinator.*` | 将版本、表页、索引和事务写集合组成原子操作 |
| GC（Bonus） | `garbage_collector.*` | 按最老活跃快照清理不可达版本和墓碑 |

锁队列中的请求按到达顺序排列。已授予请求和等待请求共享同一队列；实现时必须同时保护队列、升级者状态以及事务持有锁集合，不能只修改其中一处。

## 3. 必做部分：SS2PL

### t1：锁兼容与升级规则

实现位置：

- `LockRequestQueue::CanGrant`
- `LockManager::AreLocksCompatible`
- `LockManager::CanLockUpgrade`

系统包含 `S`、`X`、`IS`、`IX`、`SIX` 五种锁模式。兼容性判断必须忽略同一事务已经持有的锁，但不能越过队列中更早的等待者。升级应是单调增强的；同一资源同时只能有一个升级者，否则两个升级者可能相互等待。

建议先在纸上写出完整的 5×5 兼容矩阵，再实现升级有向图。不要把“兼容”和“允许升级”混为一件事。

### t2：通用加锁、解锁与表访问调用链

实现位置：

- `LockManager::GenericLock`
- `LockManager::GenericUnlock`
- `LockManager::UnlockAll`

`TableHandle::AcquireTableLock`、`AcquireTupleLock`、`ShouldAcquireReadLock` 和
`ReleaseTupleLockImmediately` 是已给出的跨层调用胶水。阅读它们可以了解执行器如何进入锁管理器，
但它们不再是 t2 TODO；评分重点是 `LockManager` 内部的队列、等待、升级、释放与事务锁集维护。

关键不变量：

1. SHRINKING 状态不能再获得新锁。
2. SS2PL 的写锁必须一直持有到事务结束。
3. `READ_COMMITTED` 可在一次读取结束后释放 S 锁，但不能因此阻止后续写锁获取。
4. `REPEATABLE_READ` 和 `SERIALIZABLE` 的读锁在正常执行中不应提前释放。
5. 释放层次化锁时按“元组 → 页 → 表”的顺序进行。
6. 等待或升级失败时，要同时清理队列请求、升级标志、等待图边和事务锁集合。

`GenericLock` 是本实验最重要的临界区。等待条件必须用循环重新检查；条件变量被唤醒不等于锁已经可授予。调用可能再次进入锁管理器的逻辑时，还要检查互斥锁的持有关系，避免自死锁。

### t3：死锁预防与检测

实现位置：

- `LockManager::ApplyPreventionProtocol`
- `LockManager::BuildWaitForGraph`
- `LockManager::DFS`
- `LockManager::HasCycle`

系统支持三种策略：

- Wait-Die：老事务等待，年轻事务遇到老持有者时回滚；
- Wound-Wait：老事务抢占年轻事务，年轻事务等待老事务；
- Detection：允许等待，周期性构建等待图并选择牺牲者。

等待图中的边方向是“等待者 → 阻塞它的持有者”。环检测后必须选择确定性的牺牲者，并释放其已经持有的所有资源，否则其他事务仍无法前进。

### t4：事务生命周期和物理回滚

实现位置：

- `TxnManager::Begin`
- `TxnManager::Commit`
- `TxnManager::Abort`

Begin 负责创建事务、设置状态与时间顺序并注册到活动事务表。Commit 应先完成协议相关动作，再将事务设为 COMMITTED。Abort 必须逆序回滚索引写集合和表写集合，之后释放锁并安全移除活动事务。

实现时保留 `ClaimTransactionForEnd`/`FinishTransaction` 的单终结路径：提交与回滚可能被不同线程同时触发，但一个事务只能有一个终结者。索引与表的回滚顺序要和原操作相反，任何提前释放锁的做法都会暴露中间状态。

BEGIN、COMMIT、CLR、ABORT 的日志链接和强制刷盘已经作为跨实验框架提供；这些调用不会替代本实验的事务状态、物理回滚和锁释放 TODO。

## 4. Bonus：MVCC 与快照隔离

Bonus 不要求 2PL 事务使用版本链，也不能破坏 SS2PL 的语义。

### b1：时间戳与水位线

实现 `TimestampOracle` 的时间戳分配、事务注册/注销和 watermark。Watermark 是所有活跃读时间戳的最小值；没有活跃事务时应回到当前时间线。

### b2：版本链与可见性

实现 `VersionChain` 的版本插入、同事务重复写复用、写写冲突、快照解析和裁剪。版本链头可能处于未提交、已提交或已回滚状态；读者还可能正是写者本身。请为这些组合画出状态表后再编码。

### b3：版本管理与事务接入

实现 `VersionManager` 的版本准备/撤销、逻辑删除登记、可见性路由、版本清理和物理墓碑回收，并在事务提交/回滚时提交或废弃对应 undo record。

### b4：原子写协调与 GC

实现 `MVCCWriteCoordinator::{Insert,Update,Delete}` 和 `GarbageCollector`。一次写入会同时影响版本链、数据页、若干索引和事务 bookkeeping；任一阶段失败都必须按相反顺序恢复，不能留下“半次写入”。

## 5. 测试与评分入口

### 5.1 全部 Lab05 单元测试

Lab05 共公开 9 个 GoogleTest 源文件、109 个 `TEST/TEST_F/TEST_P` 定义。参数化测试展开后实际 case 数会更多。

| 可执行程序 | 源文件 | 定义数 | 范围 | 性质 |
|---|---|---:|---|---|
| `lock_manager_test` | `test/concurrency/lock_manager_test.cpp` | 20 | 五种锁模式、升级、层次锁、三种死锁策略、隔离级别 | 必做 |
| `strict_2pl_test` | `test/concurrency/strict_2pl_test.cpp` | 7 | 持锁到提交、并发终结、串行化、多资源死锁 | 必做 |
| `two_pl_index_abort_test` | `test/concurrency/two_pl_index_abort_test.cpp` | 3 | INSERT/UPDATE/DELETE 回滚后的表索引一致性 | 必做 |
| `version_chain_test` | `test/concurrency/version_chain_test.cpp` | 24 | undo record、可见性、重复写、裁剪、并发句柄安全 | Bonus |
| `timestamp_mvcc_test` | `test/concurrency/timestamp_mvcc_test.cpp` | 14 | 时间戳、水位线、事务管理、版本管理 | Bonus |
| `snapshot_isolation_test` | `test/concurrency/snapshot_isolation_test.cpp` | 6 | 快照可见性、读己之写、提交时间戳 | Bonus |
| `garbage_collector_test` | `test/concurrency/garbage_collector_test.cpp` | 10 | 前后台 GC、活跃快照、墓碑与并发 | Bonus |
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

运行包括 Bonus 在内的全部并发测试：

```bash
ctest --test-dir build --output-on-failure \
  -R 'lock_manager|strict_2pl|two_pl_index_abort|version_chain|timestamp_mvcc|snapshot_isolation|garbage_collector|mvcc_write_conflict|mvcc_atomic_write'
```

### 5.2 全部 Lab05 SQL 并发测试

`test/sql/lab05/2pl/lab05_2pl_concurrency.py` 包含 15 个场景：读读兼容、写阻塞读、写写串行、回滚唤醒、不同 RID 并发、二事务环、8 客户端分片更新、1 写 7 读、5 写热点、四事务等待图、READ COMMITTED 提前释放读锁、多行事务串行、7 客户端混合 DML、唯一索引回滚重试、8 客户端完整 DML 工作流。

`test/sql/lab05/mvcc/lab05_mvcc_concurrency.py` 包含 15 个 Bonus 场景：未提交更新旧值可见、读己之写、旧快照稳定、插入提交前后可见性、删除快照、同 RID 单赢家、冲突不破坏索引、重复更新、重复更新删除回滚、8 客户端分片更新、5 写热点拒绝、7 客户端混合 DML、唯一键回滚重试、8 客户端完整 DML、MVCC/2PL 数据库共存。

服务器启动后可使用：

```bash
bash test/sql/lab05/2pl/evaluate.sh build/bin test/sql/lab05
bash test/sql/lab05/mvcc/evaluate.sh build-mvcc/bin test/sql/lab05
```

## 6. 推荐实现顺序与调试

1. 只实现 t1，先跑兼容矩阵和升级相关 filter。
2. 实现 t2 的无竞争路径，再加入等待、升级、解锁与隔离级别。
3. 实现 t3，并对每个等待点确认队列清理和唤醒。
4. 实现 t4，最后跑表/索引回滚和 SQL 多客户端测试。
5. 必做稳定后，再按 b1 → b2 → b3 → b4 完成 Bonus。

常见问题：

- 测试挂住：通常是条件变量等待条件、漏唤醒、互斥锁重入或牺牲者锁未释放。
- 参数化兼容测试失败：检查枚举顺序是否和矩阵行列一致。
- 回滚后索引错误：检查写集合是否逆序处理，以及旧/新 tuple 是否使用正确。
- 偶现双写成功：冲突检查和版本插入必须在同一个排他临界区内再次确认。

## 7. 提交前检查清单

- 必做部分通过 `lock_manager_test`、`strict_2pl_test` 和 `two_pl_index_abort_test`；
- 选做 MVCC 时，额外运行全部 Bonus 单元测试与 `mvcc/evaluate.sh`；
- 运行 2PL SQL 评分脚本，确认串行用例和 15 个并发场景全部通过；
- 确认没有提交 `build/`、`data/`、SQL `output/` 或其他运行产物。
