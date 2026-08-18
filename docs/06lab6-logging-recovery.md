# 实验6：日志系统与故障恢复

本实验为NJUDB加入预写日志（Write-Ahead Logging，WAL）、检查点和ARIES风格恢复。Lab06建立在Lab05的事务生命周期之上；恢复阶段同时处理2PL与MVCC产生的物理页修改。

## 实验前置要求与准备知识

开始本实验前，请先完成Lab05的SS2PL必做部分，或确认Lab05 gold libraries能支持事务提交、回滚和并发访问。你需要掌握：

- 事务生命周期、写集合以及表记录和索引修改的调用链；
- WAL的基本原则，以及日志持久化顺序与数据页写回顺序的关系；
- LSN、事务`prev_lsn`、page LSN和persistent LSN的含义；
- 检查点、活跃事务表（ATT）和脏页表（DPT）的职责；
- ARIES风格Analysis、Redo、Undo三阶段的输入、输出和核心不变量；
- CLR、`undo_next_lsn`和恢复过程再次崩溃时的幂等性要求。

建议先从一次UPDATE的表页修改出发，追踪普通执行、日志追加、提交刷盘和数据页刷盘路径，再阅读recovery manager的入口。实验入口统一使用`NJUDB_STUDENT_TODO(l6, ...)`标记。

## 预备知识

### 故障模型与恢复目标

事务提交表示数据库已经承诺保留该事务的效果。由于数据页通过缓冲池异步写回，提交时被修改的页面可能仍在内存中；另一方面，缓冲池也可能在事务提交前把脏页写入磁盘。系统崩溃会丢失内存状态，而磁盘上可能同时包含部分已提交修改和部分未提交修改。恢复系统需要在重启后重建一个满足原子性与持久性的状态：已提交事务的效果必须保留，未提交事务的效果必须消除。

允许未提交脏页写回磁盘的策略称为STEAL，它使缓冲池可以在内存不足时选择更广泛的淘汰对象，同时产生了Undo需求。提交时不强制写回事务修改的全部数据页称为NO-FORCE，它降低提交延迟并减少随机I/O，同时产生了Redo需求。NJUDB采用与STEAL/NO-FORCE相适应的日志恢复机制，日志负责记录页面修改的历史，数据页可以按照缓冲管理策略独立写回。

仍以实验5手册中的账户转账为例。事务T1把账户A从100改为80，并把账户B从50改为70；事务T2随后把A从80改为70。崩溃时，T1已经提交，T2尚未提交。若B所在页面仍未写回，恢复必须重做T1对B的更新；若A所在页面已经包含T2写入的70，恢复还必须撤销T2，使A回到T1提交后的80。恢复不能仅根据磁盘页面的当前数值推断事务状态，因此需要一条具有顺序、事务归属和前后映像的持久日志。

### WAL、LSN与日志链

预写日志（Write-Ahead Logging，WAL）的核心约束是日志先于对应数据页持久化。页面包含某条更新的结果之前，描述该更新的日志记录必须已经到达稳定存储；事务向客户端报告提交成功之前，提交日志也必须持久化。第一条约束保证磁盘出现未提交修改时存在可供Undo的信息，第二条约束保证已经确认的事务在崩溃后存在可供Redo的依据。

日志序列号（Log Sequence Number，LSN）为日志记录建立全局顺序。每条事务日志通过`prev_lsn`指向同一事务的前一条记录，形成事务自己的反向日志链；页面的page LSN表示已经作用于该页面的最新日志；日志管理器的persistent LSN表示已经确认持久化的日志前缀。刷写page LSN为L的页面之前，系统必须先保证persistent LSN不小于L。

下面的日志序列对应上述两个事务，`prev_lsn`使恢复过程能够从事务末端沿链回溯。表中的LSN只用于说明相对顺序。

| LSN | 事务 | 类型 | 修改或含义 | `prev_lsn` |
|---:|---|---|---|---:|
| 10 | T1 | BEGIN | 开始转账 | 无 |
| 20 | T1 | UPDATE | A：100→80 | 10 |
| 30 | T1 | UPDATE | B：50→70 | 20 |
| 40 | T1 | COMMIT | T1提交 | 30 |
| 50 | T2 | BEGIN | 开始支出 | 无 |
| 60 | T2 | UPDATE | A：80→70 | 50 |

崩溃发生后，T1属于已提交事务，T2属于需要撤销的事务。某条UPDATE是否需要实际重做，还取决于页面是否可能缺少该更新以及当前page LSN。日志顺序提供历史，page LSN提供页面已经处理到的位置，两者共同支持幂等判断。

### 模糊检查点、ATT与DPT

从日志文件开头扫描可以恢复数据库，但恢复时间会随日志增长。检查点保存某一时刻的恢复摘要，使Analysis可以从较近位置开始。活跃事务表（Active Transaction Table，ATT）记录尚未结束的事务及其最后LSN；脏页表（Dirty Page Table，DPT）记录尚不能确认已完全落盘的页面，以及页面第一次变脏所对应的recLSN。DPT中的最小recLSN给出Redo可能需要开始的位置，因为更早的更新已经不可能只存在于内存。

模糊检查点（fuzzy checkpoint）允许正常事务在检查点生成期间继续运行。BEGIN_CHECKPOINT与END_CHECKPOINT之间可能出现新的事务和页面修改，因此恢复分析需要把检查点摘要与检查点结束后的日志共同合并。只有完整、可解析的检查点记录才能作为恢复起点；不完整检查点可能来自崩溃，不能提供可靠状态。

### Analysis、Redo与Undo

ARIES风格恢复分为Analysis、Redo和Undo三个阶段。Analysis从最近有效检查点继续扫描日志，重建崩溃时的ATT与DPT，并区分已经完成的winner transaction和仍需处理的loser transaction。这个阶段恢复的是内存元数据，不直接决定每条页面记录是否已经存在于磁盘。

Redo从DPT中的最小recLSN开始按日志顺序“重复历史”，其中包括后来需要撤销的事务修改和恢复期间产生的补偿记录。对某个页面执行更新前，需要结合DPT的recLSN与页面的page LSN判断该更新是否已经生效。按历史顺序恢复页面状态能够让数据库到达崩溃瞬间的逻辑位置，并使后续Undo拥有一致的起点。

Undo处理Analysis确定的loser transaction。恢复器从各事务的最后LSN开始逆向遍历事务链，并按照全局LSN从大到小协调多个事务的撤销顺序。每完成一次补偿，系统写入补偿日志记录（Compensation Log Record，CLR）。CLR描述已经执行的撤销动作，并通过`undo_next_lsn`指出该事务下一条仍需撤销的日志。

恢复过程本身也可能再次崩溃。重启后的Redo会重演已经持久化的CLR，随后Undo沿`undo_next_lsn`越过已完成的补偿，避免对同一更新重复撤销。由此，恢复算法的每个阶段都必须允许重复执行，并依靠日志与page LSN判断已经完成的工作。

### 恢复后的索引状态

NJUDB的WAL记录表页上的物理修改，索引属于能够从表记录重新推导的派生结构。表恢复完成后，索引文件仍可能对应崩溃前的中间状态，因此系统需要扫描最终表状态并重建索引。重建过程也可能被崩溃打断，持久化marker用于表明重建尚未完整结束；后续启动检测到marker时重新执行重建，直至所有替换步骤成功完成。该过程把恢复正确性从数据页扩展到查询路径和唯一性约束，使重启后的逻辑数据库与索引视图保持一致。

以上知识在代码中形成一条连续路径。记录发生修改时，`TableHandle::LogPageModification`构造物理UPDATE日志，`LogManager::AppendLogRecord`为其分配LSN并写入日志缓冲区。返回的LSN同时更新事务`prev_lsn`和页面`page_lsn`。事务提交时，COMMIT日志必须先持久化；数据页刷盘时，缓冲池必须先把日志刷到该页的`page_lsn`。检查点收集ATT和DPT。系统重启后，`Recovery`依次执行Analysis、Redo和Undo，随后`DatabaseHandle::RebuildIndexes`根据恢复后的表记录重建索引。日志链、页面状态和索引状态由此连接成完整的恢复流程。

## 仅完成本实验时的配置

WAL在默认服务器构建中关闭，以保证Lab05可独立运行。实现Lab06必做的2PL路径时，修改`configure.sh`顶部配置：

```bash
SOURCE_LABS="06"
ENABLE_LAB05_MVCC=false
ENABLE_LAB06_WAL=true
BUILD_TYPE="Debug"
CLEAN_BUILD=true
```

然后运行`./configure.sh`和`cmake --build build -j`。只有在同时实现并验证MVCC Bonus及其崩溃恢复时，才改为：

```bash
SOURCE_LABS="05 06"
ENABLE_LAB05_MVCC=true
ENABLE_LAB06_WAL=true
```

首次切换到Lab06时使用`CLEAN_BUILD=true`；干净配置成功后可以改回`false`。只完成Lab06必做部分时不要把Lab05加入`SOURCE_LABS`，否则会同时从源码编译两个实验。

## 1.学习目标

完成本实验后，你应当能够：

- 设计可校验、可持久化的变长日志记录格式；
- 在线程安全的日志缓冲区中分配严格递增的LSN；
- 实现WAL的“日志先于数据页”约束和force-at-commit；
- 使用ATT、DPT和模糊检查点缩短恢复扫描；
- 实现Analysis、Redo、Undo三阶段恢复与CLR；
- 在恢复后从表数据重建非日志化索引，并保证崩溃重试幂等。

故障恢复的主要流程如下：

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
2. Commit返回之前，COMMIT记录已经持久化。
3. LSN分配顺序、缓冲区物理顺序和文件顺序一致。
4. CLR只允许redo，并通过`undo_next_lsn`指向下一条待撤销记录。
5. Redo使用page LSN保持幂等；同一恢复过程再次崩溃后仍可继续。
6. 文件ID只在进程内稳定；持久日志与DPT必须能用稳定文件名重新定位页面。

## 3.代码导读

| 模块 | 主要文件 | 职责 |
|---|---|---|
| 日志记录 | `src/log/log_record.{h,cpp}` | 记录大小、序列化、反序列化与格式校验 |
| 日志管理器 | `src/log/log_manager.{h,cpp}` | LSN、双缓冲、刷盘、读取与master record |
| WAL接入 | `txn_manager.cpp`、`table_handle.cpp`、`buffer_pool_manager.cpp` | 事务日志、页日志、日志先于页面 |
| 检查点 | `src/log/checkpoint_manager.{h,cpp}` | ATT/DPT收集、序列化、周期检查点 |
| 恢复 | `src/log/recovery.{h,cpp}` | Analysis、Redo、Undo与CLR |
| 索引重建 | `src/system/handle/database_handle.cpp` | 恢复后原子重建索引及崩溃标记 |

## 4.实验任务

### t1：日志记录格式

本阶段把内存中的`LogRecord`转换为稳定的字节格式，并能够从日志文件恢复原对象。

| 函数 | 输入 | 输出或状态变化 | 在本阶段中的作用 |
|---|---|---|---|
| `LogRecord::GetSize` | 当前日志对象及其记录类型和变长字段 | 返回完整序列化长度；记录类型无效时抛出异常 | 为日志缓冲区预留准确空间，并校验`SerializeTo`的写入长度 |
| `LogRecord::SerializeTo` | 容量至少为`GetSize()`的目标缓冲区`buffer` | 返回实际写入字节数；把公共头和类型专属字段写入缓冲区 | 将日志对象编码为可持久化的连续字节 |
| `LogRecord::DeserializeFrom` | 输入缓冲区`buffer`及其长度`buffer_size` | 返回重建的`LogRecord`；截断、长度不符、枚举无效或存在多余字节时抛出异常 | 从持久字节恢复日志对象，并验证记录边界和字段组合 |

公共头包含LSN、总长度、事务ID、前驱LSN和记录类型。BEGIN记录保存并发控制协议；UPDATE记录保存稳定文件名、页号、槽号、操作类型、协议以及前后映像；CLR保存`undo_next_lsn`、补偿操作和redo映像；CHECKPOINT_END保存检查点字节串。COMMIT、ABORT和CHECKPOINT_BEGIN没有额外载荷。

不要持久化指针或C++容器的对象布局。文件名、前后映像和检查点数据都使用“长度字段+字节内容”的格式。`GetSize`、`SerializeTo`和`DeserializeFrom`必须对同一种记录给出完全一致的长度。

### t2：日志管理器与并发刷盘

本阶段管理日志文件、双缓冲、LSN分配、持久化边界和恢复读取。

| 函数 | 输入 | 输出或状态变化 | 在本阶段中的作用 |
|---|---|---|---|
| `LogManager::InitializeLogFile` | 无显式参数；使用构造函数保存的日志文件名 | 无返回值；创建并写入文件头，或读取已有文件头；设置`next_lsn_`、`persistent_lsn_`和追加位置 | 建立新日志文件或从已有日志继续追加 |
| `LogManager::AppendLogRecord` | 可修改的日志对象`log_record` | 返回新分配的LSN；同时设置对象LSN，并把序列化结果追加到活动缓冲区 | 保证LSN顺序与缓冲区中的物理记录顺序一致 |
| `LogManager::FlushLog` | 无显式参数；使用活动缓冲区和刷盘缓冲区 | 无返回值；交换双缓冲，把当前批次写入并同步到文件，最后推进`persistent_lsn_` | 持久化当前活动缓冲区中的完整日志批次 |
| `LogManager::FlushLogUpTo` | 必须持久化的目标LSN | 无返回值；直到`persistent_lsn_`不小于目标；目标为`INVALID_LSN`时直接返回，目标尚未分配时抛出异常 | 为事务提交和数据页刷盘提供指定LSN的持久化保证 |
| `LogManager::WriteToLogFile` | 待写缓冲区、有效字节数和批次最大LSN | 无返回值；追加日志字节，更新文件头的最后LSN和记录数，并执行文件同步 | 完成一个已交换批次的实际文件I/O |
| `LogManager::ReadLogRecord` | 目标LSN | 返回对应的`LogRecord`；记录不存在或日志损坏时抛出异常 | 沿事务日志链读取指定记录 |
| `LogManager::GetLogRecordsFrom` | 起始LSN `start_lsn` | 返回LSN不小于起点的日志向量；忽略崩溃留下的不完整尾记录 | 为Analysis、Redo和检查点查找提供顺序扫描接口 |

LSN分配和记录写入活动缓冲区必须位于同一个临界区，否则LSN顺序可能与文件顺序不一致。`FlushLog`先交换双缓冲，再释放活动缓冲区互斥量，使其他事务可以继续追加；实际文件写入仍由独立的刷盘互斥量串行化。`persistent_lsn_`只能推进到已经完成文件写入和同步的批次最大LSN。

### t3：事务、页面修改与WAL接线（框架已完成，无需实现但很重要，需要理解）

| 函数 | 输入 | 输出或状态变化 | 在调用链中的作用 |
|---|---|---|---|
| `TxnManager::Begin`中的日志分支 | 新事务ID和并发控制协议 | 追加BEGIN记录，并把返回LSN写入事务`prev_lsn` | 建立事务日志链的起点 |
| `TxnManager::Commit`中的日志分支 | 待提交事务及其`prev_lsn` | 追加COMMIT记录，更新`prev_lsn`并调用`FlushLog` | 在返回提交结果前持久化事务日志 |
| `TxnManager::Abort`中的日志分支 | 待回滚事务、写集合和数据库句柄 | 2PL物理回滚时追加CLR，完成后追加并刷出ABORT；MVCC运行期回滚追加并刷出ABORT | 记录运行期回滚已经完成的补偿和终止状态 |
| `TableHandle::LogPageModification` | 事务、RID、修改前后字节及长度 | 追加UPDATE记录，更新事务`prev_lsn`，并把LSN写入页面`page_lsn` | 把一次表记录物理修改接入事务日志链和页面WAL状态 |
| `BufferPoolManager::FlushFrameLocked` | 缓冲帧ID | 返回是否成功刷出；对表数据页先调用`FlushLogUpTo(page_lsn)`，再写数据页并清除脏标志 | 保证日志先于对应数据页持久化，并拒绝刷出仍被pin的脏页 |

本节代码由框架提供，不包含需要实现的TODO，也不作为独立实验题目计分。请仔细阅读这些函数，理解`t1`和`t2`实现的日志组件如何接入事务执行与页面刷盘流程。后续检查点和故障恢复功能依赖这条调用链。2PL回滚中的物理逆操作属于Lab05。Lab06提供的日志接线在每次成功补偿后写入CLR，并在回滚结束后写入ABORT。缓冲池只对表数据页解释`page_lsn`；文件头和索引页不走这条页面WAL分支。

### t4：模糊检查点

本阶段把活动事务和脏页状态保存为模糊检查点。检查点生成期间不暂停普通事务，也不强制刷出全部数据页。

| 函数 | 输入 | 输出或状态变化 | 在本阶段中的作用 |
|---|---|---|---|
| `Checkpoint::GetSize` | 当前检查点中的ATT和DPT | 返回检查点序列化后的总字节数 | 为CHECKPOINT_END载荷分配准确空间 |
| `Checkpoint::SerializeTo` | 容量至少为`GetSize()`的目标缓冲区 | 无返回值；写入检查点LSN、ATT条目和DPT条目 | 将检查点摘要编码为CHECKPOINT_END的载荷 |
| `Checkpoint::DeserializeFrom` | 检查点缓冲区和长度 | 返回重建的`Checkpoint`；截断、字段无效或长度不符时抛出异常 | 在恢复分析中读取并验证检查点摘要 |
| `CheckpointManager::CollectATT` | 无显式参数；读取事务管理器 | 返回活动事务条目向量，每项包含事务ID、最后LSN、协议和状态 | 记录检查点时仍未结束的2PL与MVCC事务 |
| `CheckpointManager::CollectDPT` | 无显式参数；读取缓冲池和可选的版本管理器 | 返回以稳定文件名和页号为键、recLSN为值的DPT | 合并缓冲池脏页与尚未物理回收的MVCC删除页面 |
| `CheckpointManager::CreateCheckpoint` | 无显式参数；使用日志、事务、缓冲池和版本管理器 | 无返回值；追加BEGIN和END记录，刷出日志，更新master record和`last_checkpoint_lsn_` | 生成一个能够在重启后定位和反序列化的完整检查点 |

ATT记录每个活动事务的最后LSN、并发控制协议和状态。DPT记录每个脏页第一次变脏时的recLSN。CHECKPOINT_BEGIN与CHECKPOINT_END之间仍可产生普通日志，因此Analysis必须从BEGIN位置继续扫描。只有BEGIN与有效END配对，且END载荷能够完成反序列化时，该检查点才能作为恢复起点。

### t5：Analysis、Redo、Undo

本阶段使用日志和检查点恢复崩溃时的物理表状态。2PL与MVCC的页面修改都经过相同的Redo；两种协议的未提交事务都执行物理Undo。

| 函数 | 输入 | 输出或状态变化 | 在本阶段中的作用 |
|---|---|---|---|
| `Recovery::Recover` | 无显式参数；使用日志管理器、缓冲池和已登记的数据库句柄 | 无返回值；依次完成Analysis、Redo、Undo，最后尝试刷出恢复后的页面 | 组织一次完整恢复，不在各阶段之间遗漏状态 |
| `Recovery::FindLastCheckpoint` | 无显式参数；扫描持久日志 | 返回最近一组有效CHECKPOINT_BEGIN的LSN；不存在时返回`INVALID_LSN` | 验证BEGIN/END配对和检查点载荷，确定Analysis起点 |
| `Recovery::AnalyzeLog` | 无显式参数；读取最近有效检查点及其后的日志 | 无返回值；重建`att_`和`dpt_`，并设置`redo_start_lsn_` | 找出崩溃时未结束的事务、可能缺少更新的页面和Redo起点 |
| `Recovery::Redo` | 无显式参数；使用DPT和`redo_start_lsn_` | 无返回值；按LSN顺序处理UPDATE和CLR，必要时修改表页 | 从DPT最小recLSN开始重复历史，并跳过不在DPT或早于页面recLSN的记录 |
| `Recovery::ApplyLogRecord` | 一条UPDATE或CLR日志 | 无返回值；解析所属数据库和表，按操作类型更新槽位和页面LSN | 在表页格式之上执行一条幂等的物理Redo |
| `Recovery::Undo` | 无显式参数；使用Analysis得到的ATT | 无返回值；按全局LSN降序撤销loser transaction，追加CLR和ABORT，刷出日志并移除已完成ATT条目 | 协调多个未提交事务的反向日志链，并支持恢复期间再次崩溃 |
| `Recovery::UndoLogRecord` | 待撤销的UPDATE日志和事务ID | 返回新写入CLR的LSN；执行相反的物理操作，并把CLR接到事务恢复日志链 | 完成一条更新的补偿，并通过`undo_next_lsn`记录下一条待撤销日志 |

Analysis合并检查点ATT/DPT和后续日志，DPT中的最小recLSN成为Redo起点。Redo按日志顺序处理UPDATE与CLR；`ApplyLogRecord`再依据页面LSN决定物理操作是否已经生效。Undo使用按LSN降序的全局优先队列。遇到CLR时沿`undo_next_lsn`继续，遇到UPDATE时调用`UndoLogRecord`生成新的CLR。事务链结束后追加ABORT。

### t6：恢复后的索引重建

本阶段根据恢复后的最终表状态重建派生索引。

| 函数 | 输入 | 输出或状态变化 | 在本阶段中的作用 |
|---|---|---|---|
| `DatabaseHandle::RebuildIndexes` | 无显式参数；使用当前数据库中的表、已打开索引及其描述信息 | 无普通返回值；先持久化恢复后的表，创建marker和临时索引，扫描表记录填充临时索引，替换正式文件并重新打开索引；全部成功后清除marker | 使索引内容与恢复后的表记录一致，并让中途崩溃的重建能够在下次启动时重试 |

WAL没有单独记录索引修改，因此重建输入是当前数据库句柄中的索引描述和最终表记录。没有索引时，函数只需清理遗留marker。存在索引时，marker必须在关闭旧索引前持久化；所有临时索引完成替换并重新打开后才能清除。唯一键冲突或文件操作失败会保留marker，使后续启动重新执行完整重建。

## 5. 实验问题

请结合你的代码位置、日志序列或crash测试现象，回答下面问题。

1. 选择一次会修改数据页的事务操作，说明从日志记录生成到事务提交和数据页刷盘之间的LSN变化与先后关系；你的实现在哪些函数中保证WAL和force-at-commit？
2. 从你运行过的一次崩溃恢复场景中选择一段日志，说明Analysis如何形成ATT/DPT、Redo从哪里开始并跳过哪些操作、Undo最终处理哪些loser transaction。
3. 为什么Undo过程中需要写CLR，并使用`undo_next_lsn`而不是简单沿原日志链重复撤销？结合“恢复过程中再次崩溃”的测试说明你的实现如何保持幂等。
4. 描述一个你实际调试过的日志、恢复或索引重建问题：使用了哪个crash point，第一次启动和再次启动分别出现什么现象，你收集了哪些LSN/page LSN/文件状态证据，最终如何确认修复有效？

## 6. 测试与评分入口

### 6.1. 全部Lab06单元测试

Lab06共公开7个GoogleTest源文件、103个测试定义。其中88个属于2PL必做路径，15个覆盖MVCC/混合协议Bonus。

| 可执行程序 | 源文件 | 定义数 | 必做/Bonus | 覆盖范围 |
|---|---|---:|---|---|
| `log_record_test` | `test/log/log_record_test.cpp` | 16 | 必做 | 各记录类型往返、空/大数据、CLR、检查点、损坏输入、日志链 |
| `log_manager_test` | `test/log/log_manager_test.cpp` | 26 | 必做 | 初始化、append/flush/read、并发顺序、持久LSN、溢出、后台线程、master record、尾记录 |
| `wal_integration_test` | `test/log/wal_integration_test.cpp` | 13 | 9必做 + 4 Bonus | 事务BEGIN/COMMIT、混合协议、表DML日志、force-at-commit、检查点 |
| `recovery_test` | `test/log/recovery_test.cpp` | 29 | 22必做 + 7 Bonus | ATT/DPT、模糊检查点、redo/undo/CLR、嵌套恢复、路由、压力与真实崩溃 |
| `index_recovery_test` | `test/log/index_recovery_test.cpp` | 5 | 必做 | 多索引重建、持久marker、空表、跨数据库同名索引 |
| `wal_flush_all_pages_test` | `test/log/wal_flush_all_pages_test.cpp` | 10 | 必做 | FlushPage/FlushAll/Eviction/Delete的WAL、pin、recLSN、非数据页 |
| `mvcc_recovery_undo_test` | `test/log/mvcc_recovery_undo_test.cpp` | 4 | Bonus | MVCC与混合协议loser的ATT/Undo |

列出任一二进制中的所有具体用例：

```bash
./build/bin/recovery_test --gtest_list_tests
./build/bin/log_manager_test --gtest_list_tests
```

运行必做的88个测试定义（两个混合源文件使用负向filter排除Bonus）：

```bash
ctest --test-dir build --output-on-failure \
  -R 'log_record|log_manager|index_recovery|wal_flush_all_pages'

./build/bin/wal_integration_test \
  --gtest_filter='*-*MixedProtocol*'

./build/bin/recovery_test \
  --gtest_filter='*-*MVCC*:*Mixed*:RecoveryTest.PeriodicCheckpointing'
```

完成MVCC Bonus后，运行全部103个Lab06单元测试：

```bash
cmake --build build --target \
  log_record_test log_manager_test wal_integration_test recovery_test \
  index_recovery_test wal_flush_all_pages_test mvcc_recovery_undo_test -j

ctest --test-dir build --output-on-failure \
  -R 'log_record|log_manager|wal_integration|recovery|index_recovery|wal_flush_all_pages|mvcc_recovery_undo'
```

调试时建议按t1 → t2 → t4 → t5 → t6的顺序运行核心TODO，并结合已给出的t3调用链做集成验证。可用GoogleTest filter缩小范围，例如：

```bash
./build/bin/log_record_test --gtest_filter='*Serialization*'
./build/bin/recovery_test --gtest_filter='*Checkpoint*'
```

### 6.2. 全部Lab06 SQL/进程级崩溃测试

入口为`test/sql/lab06/recovery/evaluate.sh`，核心驱动是`lab06_recovery_crash.py`。默认以2PL必做模式运行；设置`NJUDB_INCLUDE_MVCC_BONUS=1`后，才在每个恢复阶段加入MVCC Bonus工作负载。两种模式都最终报告15/15，覆盖：

- 已提交与未提交的2PL/MVCC混合工作负载；
- 服务进程被强制kill后的第一次恢复；
- Redo后、逐条Undo后再次崩溃并重试；
- 恢复刚结束又崩溃的嵌套恢复；
- 恢复后检查点以及再次启动；
- 索引重建marker、关闭索引、临时索引、rename、重新打开索引五个崩溃窗口；
- 确定性多轮crash/restart压力；
- 最终只读状态、唯一约束、负值回滚和键复用验证。

运行方式：

```bash
bash test/sql/lab06/recovery/evaluate.sh build/bin test/sql/lab06
```

运行混合协议Bonus恢复评分：

```bash
NJUDB_INCLUDE_MVCC_BONUS=1 bash test/sql/lab06/recovery/evaluate.sh \
  build/bin test/sql/lab06
```

该脚本会启动和终止本机NJUDB服务，并创建测试数据库。请先停止手工启动的同端口服务，且不要在包含重要数据的运行目录中执行。

## 7. 推荐调试方法

- 序列化错误：同时打印`GetSize()`、实际写入字节数和反序列化最终offset。
- 并发日志乱序：记录“分配LSN、写缓冲offset、flush批次最大LSN”三个值。
- WAL失败：同时观察page LSN、persistent LSN、页类型和pin count。
- 恢复不幂等：检查Apply前的page LSN判断，以及CLR的`undo_next_lsn`。
- 检查点漏日志：检查BEGIN/END之间的扫描和ATT/DPT合并方向。
- 索引二次启动失败：检查marker的持久化时机和临时文件替换边界。

## 8. 实验提交
提交前检查清单
- 必做提交运行88个2PL单元测试定义；Bonus提交再运行全部7个可执行文件/103个定义；
- 运行默认2PL `recovery/evaluate.sh`，确认15/15；Bonus提交再使用`NJUDB_INCLUDE_MVCC_BONUS=1`运行混合协议15/15；
- 在测试后再次启动数据库，检查恢复幂等性和索引可用性；
- 确认没有提交`build/`、`data/`、WAL文件、索引重建marker或SQL `output/`。

实验提交要求
1. 提交一份PDF格式的实验报告，文件名为`lab6_学号_姓名.pdf`。报告首页应包含下列信息：

   | 学号 | 姓名 | 邮箱 | 完成题目 |
   |---|---|---|---|
   | 12345678 | 张三 | zhangsan@smail.nju.edu.cn | t1/t2/t4/t5/t6/MVCC恢复Bonus |

   “完成题目”只填写实际完成的阶段；`t3`是已给的WAL调用链，不作为独立完成项填写。报告正文应说明日志格式、WAL不变量、检查点与恢复流程、测试结果和调试过程。请引用关键函数位置、LSN变化或崩溃测试证据，避免大段粘贴代码，推荐2-4页。

2. 提交代码目录`njudb/src`。不要包含构建目录、数据文件、WAL文件、索引重建marker或测试输出。

3. 将报告和代码打包为`lab6_学号_姓名.zip`并上传至提交平台。解压后的根目录应当为：

```text
├── lab6_12345678_张三.pdf
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
