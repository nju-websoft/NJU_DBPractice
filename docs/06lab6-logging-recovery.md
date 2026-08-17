# 实验 6：日志系统与故障恢复

本实验为 NJUDB 加入预写日志（Write-Ahead Logging，WAL）、检查点和 ARIES 风格恢复。Lab06 建立在 Lab05 的事务生命周期之上；恢复阶段同时处理 2PL 与 MVCC 产生的物理页修改。

WAL 在默认服务器构建中关闭，以保证 Lab05 可独立运行。实现 Lab06 必做的 2PL 路径时，修改 `configure.sh` 顶部配置：

```bash
SOURCE_LABS="06"
ENABLE_LAB05_MVCC=false
ENABLE_LAB06_WAL=true
CLEAN_BUILD=true
```

然后运行 `./configure.sh` 和 `cmake --build build -j`。只有在同时实现并验证 MVCC Bonus 及其崩溃恢复时，才改为：

```bash
SOURCE_LABS="05 06"
ENABLE_LAB05_MVCC=true
ENABLE_LAB06_WAL=true
```

实验入口统一使用 `NJUDB_STUDENT_TODO(l6, ...)` 标记。

## 1. 学习目标

完成本实验后，你应当能够：

- 设计可校验、可持久化的变长日志记录格式；
- 在线程安全的日志缓冲区中分配严格递增的 LSN；
- 实现 WAL 的“日志先于数据页”约束和 force-at-commit；
- 使用 ATT、DPT 和模糊检查点缩短恢复扫描；
- 实现 Analysis、Redo、Undo 三阶段恢复与 CLR；
- 在恢复后从表数据重建非日志化索引，并保证崩溃重试幂等。

## 2. 总体不变量

本实验最重要的关系是：

```text
事务 prev_lsn 形成日志链
        ↓
页修改记录的 LSN 写入 page_lsn
        ↓
刷数据页前必须保证 persistent_lsn >= page_lsn
        ↓
崩溃后 Analysis 构造 ATT/DPT，Redo 重演历史，Undo 回滚 loser
```

必须始终满足：

1. 日志在对应脏页之前持久化。
2. Commit 返回之前，COMMIT 记录已经持久化。
3. LSN 分配顺序、缓冲区物理顺序和文件顺序一致。
4. CLR 只允许 redo，并通过 `undo_next_lsn` 指向下一条待撤销记录。
5. Redo 使用 page LSN 保持幂等；同一恢复过程再次崩溃后仍可继续。
6. 文件 ID 只在进程内稳定；持久日志与 DPT 必须能用稳定文件名重新定位页面。

## 3. 代码导读

| 模块 | 主要文件 | 职责 |
|---|---|---|
| 日志记录 | `src/log/log_record.{h,cpp}` | 记录大小、序列化、反序列化与格式校验 |
| 日志管理器 | `src/log/log_manager.{h,cpp}` | LSN、双缓冲、刷盘、读取与 master record |
| WAL 接入 | `txn_manager.cpp`、`table_handle.cpp`、`buffer_pool_manager.cpp` | 事务日志、页日志、日志先于页面 |
| 检查点 | `src/log/checkpoint_manager.{h,cpp}` | ATT/DPT 收集、序列化、周期检查点 |
| 恢复 | `src/log/recovery.{h,cpp}` | Analysis、Redo、Undo 与 CLR |
| 索引重建 | `src/system/handle/database_handle.cpp` | 恢复后原子重建索引及崩溃标记 |

## 4. 实验任务

### t1：日志记录格式

实现位置：

- `LogRecord::GetSize`
- `LogRecord::SerializeTo`
- `LogRecord::DeserializeFrom`

公共头包含 LSN、总长度、事务 ID、前驱 LSN 和记录类型。BEGIN、UPDATE、CLR、CHECKPOINT_END 还带有不同的类型字段。序列化与大小计算必须完全一致；反序列化任何字段前都要检查剩余字节数，并拒绝未知枚举、过短记录、超长记录和尾部多余数据。

不要直接持久化指针、`std::string` 或 `std::vector` 的对象布局。变长字段应使用“长度 + 字节串”，并对长度进行上界验证。

### t2：日志管理器与并发刷盘

实现位置：

- `LogManager::InitializeLogFile`
- `LogManager::AppendLogRecord`
- `LogManager::FlushLog`
- `LogManager::FlushLogUpTo`
- `LogManager::WriteToLogFile`
- `LogManager::ReadLogRecord`
- `LogManager::GetLogRecordsFrom`

LSN 的分配和记录写入活动缓冲区必须处于同一临界区，否则并发线程可能得到递增 LSN，却以相反顺序写入文件。双缓冲交换后应尽快释放 append 锁，再进行慢速 I/O；多个 flush 仍需单独串行化。

`persistent_lsn` 只能推进到已经完成文件写入和同步的批次最大 LSN。`FlushLogUpTo(x)` 返回时，LSN x 必须可从持久文件读取；请求不存在的未来 LSN 应直接报错，不能无限循环。

读取端需要容忍崩溃留下的不完整尾记录，但不能把损坏的完整记录静默当作正常日志。

### 已给框架：事务、页面修改与 WAL 接线（原 t3 调用链）

以下位置保留完整实现，供你阅读日志系统如何接入各层；它们不是 Lab06 TODO：

- `TxnManager::{Begin,Commit,Abort}` 中的 Lab06 部分
- `TableHandle::LogPageModification`
- `BufferPoolManager::FlushFrameLocked` 中的 WAL 分支

`TxnManager` 在 Begin/Commit/Abort 中维护日志链；每次页修改记录 UPDATE，并把返回 LSN 同时写入事务 `prev_lsn` 和页面 `page_lsn`；Commit 追加 COMMIT 并强制刷盘。

2PL 回滚的物理逆操作仍属于 Lab05；已给框架在每次成功补偿后写入 CLR，并在全部回滚完成后写 ABORT。你实现的 `LogRecord` 和 `LogManager` 必须为这条调用链提供正确语义。

Buffer Pool 只对 WAL 管理的数据页解释 `page_lsn`。索引页或文件头中的普通字节不能误作 LSN。脏但仍被 pin 的页面不能被 FlushAllPages 强行刷出。

### t4：模糊检查点

实现位置：

- `Checkpoint::{SerializeTo,DeserializeFrom,GetSize}`
- `CheckpointManager::CreateCheckpoint`
- `CheckpointManager::CollectATT`
- `CheckpointManager::CollectDPT`

检查点记录所有活跃事务的最后 LSN、协议和状态，以及每个脏页第一次变脏时的 recLSN。模糊检查点不要求暂停事务或强刷所有数据页，因此 CHECKPOINT_BEGIN 与 CHECKPOINT_END 之间产生的日志也必须在恢复分析中保留。

只有 BEGIN 与有效 END 配对且 END 载荷可反序列化时，master record 指向的检查点才可使用。

### t5：Analysis、Redo、Undo

实现位置：

- `Recovery::Recover`
- `Recovery::AnalyzeLog`
- `Recovery::Redo`
- `Recovery::Undo`
- `Recovery::FindLastCheckpoint`
- `Recovery::ApplyLogRecord`
- `Recovery::UndoLogRecord`

Analysis 从最近有效检查点开始合并 ATT/DPT，并继续扫描后续日志。Redo 的起点是 DPT 中最小 recLSN，按日志顺序重复 UPDATE 与 CLR；应用前同时检查 DPT recLSN 和页面 LSN。

Undo 使用按 LSN 降序的全局优先队列处理所有 loser，而不是逐事务依次回滚。遇到 CLR 时沿 `undo_next_lsn` 跳转；遇到普通 UPDATE 时执行反操作并生成新的 CLR。事务链结束后写 ABORT。这样二次崩溃会跳过已经补偿的动作。

### t6：恢复后的索引重建

实现 `DatabaseHandle::RebuildIndexes`。当前 WAL 记录表页修改，索引通过恢复后的最终表状态重建。重建应使用临时文件和持久化 marker：崩溃后 marker 仍在，下一次启动会重新执行；全部临时索引完成并替换成功后才清除 marker。

要特别处理空表、同名索引位于不同数据库、唯一键冲突以及替换过程中的多次崩溃。

## 5. 测试与评分入口

### 5.1 全部 Lab06 单元测试

Lab06 共公开 7 个 GoogleTest 源文件、103 个测试定义。其中 88 个属于 2PL 必做路径，15 个覆盖 MVCC/混合协议 Bonus。

| 可执行程序 | 源文件 | 定义数 | 必做/Bonus | 覆盖范围 |
|---|---|---:|---|---|
| `log_record_test` | `test/log/log_record_test.cpp` | 16 | 必做 | 各记录类型往返、空/大数据、CLR、检查点、损坏输入、日志链 |
| `log_manager_test` | `test/log/log_manager_test.cpp` | 26 | 必做 | 初始化、append/flush/read、并发顺序、持久 LSN、溢出、后台线程、master record、尾记录 |
| `wal_integration_test` | `test/log/wal_integration_test.cpp` | 13 | 9 必做 + 4 Bonus | 事务 BEGIN/COMMIT、混合协议、表 DML 日志、force-at-commit、检查点 |
| `recovery_test` | `test/log/recovery_test.cpp` | 29 | 22 必做 + 7 Bonus | ATT/DPT、模糊检查点、redo/undo/CLR、嵌套恢复、路由、压力与真实崩溃 |
| `index_recovery_test` | `test/log/index_recovery_test.cpp` | 5 | 必做 | 多索引重建、持久 marker、空表、跨数据库同名索引 |
| `wal_flush_all_pages_test` | `test/log/wal_flush_all_pages_test.cpp` | 10 | 必做 | FlushPage/FlushAll/Eviction/Delete 的 WAL、pin、recLSN、非数据页 |
| `mvcc_recovery_undo_test` | `test/log/mvcc_recovery_undo_test.cpp` | 4 | Bonus | MVCC 与混合协议 loser 的 ATT/Undo |

列出任一二进制中的所有具体用例：

```bash
./build/bin/recovery_test --gtest_list_tests
./build/bin/log_manager_test --gtest_list_tests
```

运行必做的 88 个测试定义（两个混合源文件使用负向 filter 排除 Bonus）：

```bash
ctest --test-dir build-lab06 --output-on-failure \
  -R 'log_record|log_manager|index_recovery|wal_flush_all_pages'

./build-lab06/bin/wal_integration_test \
  --gtest_filter='*-*MixedProtocol*'

./build-lab06/bin/recovery_test \
  --gtest_filter='*-*MVCC*:*Mixed*:RecoveryTest.PeriodicCheckpointing'
```

完成 MVCC Bonus 后，运行全部 103 个 Lab06 单元测试：

```bash
cmake --build build-lab06-mvcc --target \
  log_record_test log_manager_test wal_integration_test recovery_test \
  index_recovery_test wal_flush_all_pages_test mvcc_recovery_undo_test -j

ctest --test-dir build-lab06-mvcc --output-on-failure \
  -R 'log_record|log_manager|wal_integration|recovery|index_recovery|wal_flush_all_pages|mvcc_recovery_undo'
```

调试时建议按 t1 → t2 → t4 → t5 → t6 的顺序运行核心 TODO，并结合已给出的 t3 调用链做集成验证。可用 GoogleTest filter 缩小范围，例如：

```bash
./build/bin/log_record_test --gtest_filter='*Serialization*'
./build/bin/recovery_test --gtest_filter='*Checkpoint*'
```

### 5.2 全部 Lab06 SQL/进程级崩溃测试

入口为 `test/sql/lab06/recovery/evaluate.sh`，核心驱动是 `lab06_recovery_crash.py`。默认以 2PL 必做模式运行；设置 `NJUDB_INCLUDE_MVCC_BONUS=1` 后，才在每个恢复阶段加入 MVCC Bonus 工作负载。两种模式都最终报告 15/15，覆盖：

- 已提交与未提交的 2PL/MVCC 混合工作负载；
- 服务进程被强制 kill 后的第一次恢复；
- Redo 后、逐条 Undo 后再次崩溃并重试；
- 恢复刚结束又崩溃的嵌套恢复；
- 恢复后检查点以及再次启动；
- 索引重建 marker、关闭索引、临时索引、rename、重新打开索引五个崩溃窗口；
- 确定性多轮 crash/restart 压力；
- 最终只读状态、唯一约束、负值回滚和键复用验证。

运行方式：

```bash
bash test/sql/lab06/recovery/evaluate.sh build-lab06/bin test/sql/lab06
```

运行混合协议 Bonus 恢复评分：

```bash
NJUDB_INCLUDE_MVCC_BONUS=1 bash test/sql/lab06/recovery/evaluate.sh \
  build-lab06-mvcc/bin test/sql/lab06
```

该脚本会启动和终止本机 NJUDB 服务，并创建测试数据库。请先停止手工启动的同端口服务，且不要在包含重要数据的运行目录中执行。

## 6. 推荐调试方法

- 序列化错误：同时打印 `GetSize()`、实际写入字节数和反序列化最终 offset。
- 并发日志乱序：记录“分配 LSN、写缓冲 offset、flush 批次最大 LSN”三个值。
- WAL 失败：同时观察 page LSN、persistent LSN、页类型和 pin count。
- 恢复不幂等：检查 Apply 前的 page LSN 判断，以及 CLR 的 `undo_next_lsn`。
- 检查点漏日志：检查 BEGIN/END 之间的扫描和 ATT/DPT 合并方向。
- 索引二次启动失败：检查 marker 的持久化时机和临时文件替换边界。

## 7. 提交前检查清单

- 必做提交运行 88 个 2PL 单元测试定义；Bonus 提交再运行全部 7 个可执行文件/103 个定义；
- 运行默认 2PL `recovery/evaluate.sh`，确认 15/15；Bonus 提交再使用 `NJUDB_INCLUDE_MVCC_BONUS=1` 运行混合协议 15/15；
- 在测试后再次启动数据库，检查恢复幂等性和索引可用性；
- 确认没有提交 `build/`、`data/`、WAL 文件、索引重建 marker 或 SQL `output/`。
