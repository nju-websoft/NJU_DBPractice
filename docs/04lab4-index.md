# 实验4: 索引
> 本次实验有一定难度，建议尽早开始。
## B+树

B+树是一种多路平衡查找树，广泛应用于数据库系统的索引结构。其基本结构包括内部节点和叶子节点。所有的关键字都存储在叶子节点中，内部节点只存储用于导航的键值。叶子节点之间通过指针相连，便于范围查询。

B+树的主要操作包括插入、删除和查找。插入和删除操作会在必要时分裂或合并节点，以保持树的平衡。查找操作从根节点开始，根据键值逐层向下，最终在叶子节点找到目标数据或确定其不存在。B+树能够高效地支持大量数据的动态插入和范围查询，是数据库索引的常用实现方式。


## 查询优化器
查询优化器的物理优化可以根据当前查询谓词的特性以及在查询表格上是否建有索引，将顺序遍历（Sequential Scan, SeqScan）优化为索引扫描（Index Scan, IdxScan）。这种优化策略能够大幅提升需要排序的SQL和有约束的过滤查询的执行效率。

对于复合列索引（即索引建立在多个列上），查询优化器会根据查询条件选择最优的索引匹配方式。一般来说，只有查询条件中包含索引的前缀列（即索引定义时最左边的一个或多个列），才能有效利用该复合索引。这被称为“最左前缀匹配原则”。例如，若有索引`(a, b, c)`，则查询条件中包含`a`或`a, b`，甚至`a, b, c`时，索引都可以被利用；但如果只包含`b`或`c`，则无法使用该索引。

此外，查询优化器还会根据谓词的选择性（即过滤数据的比例）、索引的类型（如B+树索引、哈希索引等）、表的大小以及统计信息等因素，综合判断是否采用索引扫描。对于高选择性的查询（即能过滤掉大量数据的条件），索引扫描通常更优；而对于低选择性的查询，顺序扫描可能更高效。

总之，查询优化器通过分析查询条件和表的索引结构，智能地选择最优的物理执行计划，从而提升数据库查询的整体性能。

## 实验要求

### t1: B+树（80 pts）

本实验要求你实现一个完整的B+树索引，如上所述，B+树具有以下特点：
- 所有数据都存储在叶子节点中
- 内部节点只存储键值用于导航
- 叶子节点通过链表连接，支持范围查询
- 树的高度保持平衡，保证查询性能

开始之前，我们建议使用[可视化工具](https://www.cs.usfca.edu/~galles/visualization/BPlusTree.html)熟悉B+树的插入与删除操作，重点理解插入后的内部节点和叶节点如何分裂（Split），以及删除某个键值导致叶节点或内部节点重新分布（Redistribute）。**不需要考虑重复值**。

需要实现的所有代码均在`src/storage/index/index_bptree.cpp`和`src/storage/index/index_bptree.h`中。

#### 页面结构
- **BPTreePage**: 所有节点的基类，包含基本的页面信息
- **BPTreeLeafPage**: 叶子节点，存储实际的键值对
- **BPTreeInternalPage**: 内部节点，存储键值和子节点指针
- **BPTreeIndexHeader**: 索引头部信息，存储在文件的第一页

#### 核心类
- **BPTreeIndex**: B+树索引的主要实现类，提供插入、删除、查询等操作

#### 第一部分：基础页面操作 (BPTreePage)

#### 1. `Init` 方法
**功能**: 初始化B+树页面的基本信息
**要点**:
- 设置索引ID、页面ID、父页面ID
- 设置节点类型（叶子或内部节点）
- 初始化大小为0，设置最大容量

#### 2. 基本属性方法
- `IsLeaf()`: 检查节点类型是否为叶子节点
- `IsRoot()`: 检查父页面ID是否为无效值
- `GetSize()/SetSize()`: 获取/设置当前存储的键值对数量
- `GetMaxSize()`: 获取节点的最大容量
- `GetPageId()/GetParentPageId()/SetParentPageId()`: 页面ID相关操作

#### 3. `IsSafe` 方法
**功能**: 判断当前操作是否会引起节点分裂或合并
**逻辑**:
- 插入操作：检查插入后是否会超过最大容量
- 删除操作：检查删除后是否会低于最小容量（通常是最大容量的一半）

#### 第二部分：叶子节点操作 (BPTreeLeafPage)

#### 1. `Init` 方法
**功能**: 初始化叶子节点
**要点**:
- 调用父类的Init方法，指定节点类型为LEAF
- 设置下一个叶子节点的页面ID为无效值
- 设置键的大小

#### 2. 数据访问方法
- `KeyAt(index)`: 返回指定位置的键
- `ValueAt(index)`: 返回指定位置的值（RID）
- `SetKeyAt(index, key)`: 设置指定位置的键
- `SetValueAt(index, value)`: 设置指定位置的值

#### 3. `KeyIndex` 方法
**功能**: 查找键应该插入的位置
**逻辑**:
- 使用二分查找找到第一个大于等于目标键的位置
- 如果所有键都小于目标键，返回size_

#### 4. `LowerBound` 和 `UpperBound` 方法
**功能**: 支持范围查询的边界查找
- `LowerBound`: 找到第一个大于等于目标键的位置
- `UpperBound`: 找到第一个大于目标键的位置

#### 5. `Lookup` 方法
**功能**: 查找指定键的所有值
**逻辑**:
- 使用KeyIndex找到起始位置
- 向后遍历所有相等的键，收集对应的RID

#### 6. `Insert` 方法
**功能**: 插入键值对
**逻辑**:
1. 找到插入位置
2. 将该位置之后的所有元素向后移动
3. 在指定位置插入新的键值对
4. 增加size_

#### 7. `RemoveRecord` 方法
**功能**: 删除指定的键
**逻辑**:
1. 找到要删除的键的位置
2. 将该位置之后的所有元素向前移动
3. 减少size_

#### 8. 节点分裂和合并方法
- `MoveHalfTo`: 将当前节点的一半数据移动到新节点
- `MoveAllTo`: 将当前节点的所有数据移动到目标节点
- `CopyNFrom`: 从给定的数组复制指定数量的数据

#### 第三部分：内部节点操作 (BPTreeInternalPage)

#### 1. `Init` 方法
**功能**: 初始化内部节点
**要点**:
- 调用父类的Init方法，指定节点类型为INTERNAL
- 设置键的大小

#### 2. 数据访问方法
类似叶子节点，但值类型是page_id_t而不是RID

#### 3. `Lookup` 方法
**功能**: 根据键找到对应的子页面ID
**逻辑**:
- 从索引1开始遍历（索引0的键是无效的）
- 找到第一个大于目标键的位置
- 返回前一个位置对应的子页面ID

#### 4. `LookupForLowerBound` 和 `LookupForUpperBound`
**功能**: 支持范围查询的子页面查找

#### 5. `PopulateNewRoot` 方法
**功能**: 当创建新根节点时初始化其内容
**逻辑**:
- 设置第一个子指针指向旧根
- 设置分隔键
- 设置第二个子指针指向新页面

#### 6. `InsertNodeAfter` 方法
**功能**: 在指定子页面之后插入新的键值对
**逻辑**:
1. 找到旧值的位置
2. 将该位置之后的元素向后移动
3. 插入新的键值对

#### 7. 节点操作方法
- `MoveHalfTo`: 移动一半数据到新节点，需要更新子页面的父指针
- `MoveAllTo`: 移动所有数据，包括来自父节点的中间键
- `CopyNFrom`: 复制数据并更新父指针

#### 第四部分：B+树主要操作 (BPTreeIndex)

#### 1. 页面管理

##### `NewPage` 方法
**功能**: 分配新页面
**逻辑**:
- 检查是否有空闲页面可重用
- 如果没有，分配新的页面
- 更新头部信息中的页面数量

请注意Page Header跟BPTree Page Header在页面中的位置。

##### `DeletePage` 方法
**功能**: 释放页面并加入空闲列表

#### 2. 页面查找

##### `FindLeafPage` 方法
**功能**: 从根节点开始找到包含指定键的叶子页面
**逻辑**:
1. 从根页面开始
2. 如果是叶子节点，直接返回
3. 如果是内部节点，使用Lookup找到下一层页面
4. 重复直到找到叶子节点

##### `FindLeafPageForRange` 方法
**功能**: 支持范围查询的叶子页面查找

#### 3. 插入操作

##### `Insert` 方法
**功能**: 插入键值对的入口方法
**逻辑**:
- 如果树为空，调用StartNewTree
- 否则调用InsertIntoLeaf

##### `StartNewTree` 方法
**功能**: 创建只有一个叶子节点的新树
**逻辑**:
1. 分配新页面作为根
2. 初始化为叶子节点
3. 插入第一个键值对
4. 更新头部信息

##### `InsertIntoLeaf` 方法
**功能**: 向叶子节点插入键值对
**逻辑**:
1. 找到目标叶子页面
2. 如果有空间，直接插入
3. 如果没有空间，需要分裂：
   - 创建新叶子节点
   - 将所有数据（包括新插入的）重新分配
   - 调用InsertIntoParent处理父节点

##### `InsertIntoParent` 方法
**功能**: 处理节点分裂后的父节点更新
**逻辑**:
1. 如果旧节点是根，调用InsertIntoNewRoot
2. 否则在现有父节点中插入新的键值对
3. 如果父节点也需要分裂，递归处理

##### `InsertIntoNewRoot` 方法
**功能**: 创建新的根节点
**逻辑**:
1. 分配新页面作为根
2. 初始化为内部节点
3. 设置两个子指针和分隔键
4. 更新所有相关页面的父指针

#### 4. 删除操作

##### `Delete` 方法
**功能**: 删除指定键的入口方法
**逻辑**:
1. 找到包含键的叶子页面
2. 删除键值对
3. 如果节点变得过小，调用CoalesceOrRedistribute

##### `CoalesceOrRedistribute` 方法
**功能**: 处理删除后可能的节点合并或重分布
**逻辑**:
1. 检查节点是否过小
2. 找到兄弟节点
3. 如果兄弟节点有多余元素，进行重分布
4. 否则进行合并

##### `Coalesce` 方法
**功能**: 合并两个相邻节点
**逻辑**:
1. 确定左右节点
2. 将右节点的所有数据移动到左节点
3. 从父节点删除对应的键和指针
4. 递归处理父节点

##### `AdjustRoot` 方法
**功能**: 处理根节点的特殊情况
**逻辑**:
- 如果根节点为空叶子节点，树变空
- 如果根节点是只有一个子节点的内部节点，子节点成为新根

#### 5. 查询操作

##### `Search` 方法
**功能**: 查找指定键的所有值
**逻辑**:
1. 找到包含键的叶子页面
2. 调用叶子节点的Lookup方法

##### `SearchRange` 方法
**功能**: 范围查询
**逻辑**:
1. 找到包含低键的叶子页面
2. 从该页面开始遍历
3. 收集范围内的所有值
4. 通过叶子节点链表继续到下一页面

#### 6. 迭代器实现

##### `BPTreeIterator` 类
**功能**: 提供顺序遍历B+树的能力
**方法**:
- `IsValid()`: 检查迭代器是否有效
- `Next()`: 移动到下一个键值对
- `GetKey()/GetRID()`: 获取当前位置的键和值

#### 实现建议

##### 1. 实现顺序
建议按以下顺序实现：
1. BPTreePage的基本方法
2. BPTreeLeafPage的数据访问和查找方法
3. BPTreeInternalPage的基本方法
4. 简单的查询操作（Search）
5. 插入操作（从简单到复杂）
6. 删除操作
7. 迭代器

#### 2. 调试技巧
- 利用提供的DebugPrint函数输出节点内容
- 在TEST_BPTREE模式下使用较小的节点大小便于调试
- 逐步测试，确保每个方法都正确实现

#### 3. 常见错误
- 忘记更新父指针
- 数组越界访问
- 页面pinning/unpinning不匹配：建议使用`PageGuard`类，可以在析构函数中自动释放页面。例如，你可以使用下面的代码访问leaf page：
```c++
auto leaf_guard = tree_->buffer_pool_manager_->FetchPageRead(tree_->index_id_, leaf_page_id_);
auto leaf_node  = reinterpret_cast<const BPTreeLeafPage *>(PageContentPtr(leaf_guard.GetData()));
```
- 二分查找的边界条件错误：建议可以先用朴素遍历确保逻辑是正确的。

#### 4. 并发
为了简化实现，在做读写操作时只需要将整个索引锁住即可。对于更高效的并发控制算法，感兴趣的同学可以了解一下`latch crabbing`，实现更高效的并发算法。

### t2: 索引句柄（Index Handle）和索引扫描算子（IdxScanExecutor）（10 pts）
本题需要索引通过句柄和算子集成到执行器中。在确保`t1`通过测试后，删除`src/storage/index/index_bptree.cpp`第30行的宏定义`#define TEST_BPTREE`以开始实验`t2`。
首先完成Index Handle，其最主要的任务就是根据索引的Key Schema从原始记录中提取key value，然后调用索引的插入，删除和更新。

其次需要在实验2完成的插入和更新算子中加入索引的Uniqueness检查，即如果发现更新的记录违反了索引中无重复值的假设，需要抛出`WSDB_INDEX_FAIL`异常。

最后，你需要实现`IdxScanExecutor`。
在此之前你需要先了解查询优化器中索引选择的逻辑。
举个例子，假设有一个员工表 `employees`，包含以下字段：
- `emp_id` (INT)
- `department` (VARCHAR)
- `salary` (INT)
- `age` (INT)
- `city` (VARCHAR)

#### 索引定义
```sql
-- 索引1：单列索引
CREATE INDEX idx_emp_id ON employees(emp_id);

-- 索引2：复合索引
CREATE INDEX idx_dept_salary ON employees(department, salary);

-- 索引3：三列复合索引
CREATE INDEX idx_dept_salary_age ON employees(department, salary, age);
```

#### 查询条件示例

##### 场景1：部分匹配复合索引
```sql
WHERE department = 'IT' AND salary > 50000 AND city = 'Beijing'
```

**条件向量 (conds)**：
```cpp
conds = [
    Condition(department, OP_EQ, 'IT'),      // 位置0
    Condition(salary, OP_GT, 50000),         // 位置1
    Condition(city, OP_EQ, 'Beijing')        // 位置2
]
```

**索引分析**：
- `idx_emp_id`: 无匹配条件，匹配数 = 0
- `idx_dept_salary`: 
  - department = 'IT' (OP_EQ) ✓
  - salary > 50000 (OP_GT) ✓
  - 匹配数 = 2
- `idx_dept_salary_age`:
  - department = 'IT' (OP_EQ) ✓
  - salary > 50000 (OP_GT) ✓
  - age 字段无条件，但前两个字段已匹配
  - 匹配数 = 2

**选择结果**：选择 `idx_dept_salary` 或 `idx_dept_salary_age`（取决于实现，通常选择第一个找到的）

**最终结果**：
```cpp
// 选择 idx_dept_salary
index_conds = [
    Condition(department, OP_EQ, 'IT'),
    Condition(salary, OP_GT, 50000)
]

// 剩余条件
conds = [
    Condition(city, OP_EQ, 'Beijing')  // 需要额外的Filter操作
]
```

##### 场景2：范围条件阻断后续字段
```sql
WHERE department = 'IT' AND salary > 50000 AND age = 25
```

**条件向量 (conds)**：
```cpp
conds = [
    Condition(department, OP_EQ, 'IT'),      // 位置0
    Condition(salary, OP_GT, 50000),         // 位置1
    Condition(age, OP_EQ, 25)                // 位置2
]
```

**索引分析**：
- `idx_dept_salary_age`:
  - department = 'IT' (OP_EQ) ✓
  - salary > 50000 (OP_GT) ✓ (范围条件)
  - age = 25：由于salary是范围条件，age字段无法使用
  - 匹配数 = 2

**选择结果**：
```cpp
index_conds = [
    Condition(department, OP_EQ, 'IT'),
    Condition(salary, OP_GT, 50000)
]

conds = [
    Condition(age, OP_EQ, 25)  // 需要Filter操作
]
```

##### 场景3：完全匹配
```sql
WHERE department = 'IT' AND salary = 60000 AND age = 30
```

**条件向量 (conds)**：
```cpp
conds = [
    Condition(department, OP_EQ, 'IT'),      // 位置0
    Condition(salary, OP_EQ, 60000),         // 位置1
    Condition(age, OP_EQ, 30)                // 位置2
]
```

**索引分析**：
- `idx_dept_salary_age`:
  - department = 'IT' (OP_EQ) ✓
  - salary = 60000 (OP_EQ) ✓ (等值条件)
  - age = 30 (OP_EQ) ✓ (前面都是等值，可以使用)
  - 匹配数 = 3

**选择结果**：
```cpp
index_conds = [
    Condition(department, OP_EQ, 'IT'),
    Condition(salary, OP_EQ, 60000),
    Condition(age, OP_EQ, 30)
]

conds = []  // 所有条件都被索引处理，无需额外Filter
```

#### 场景4：哈希索引示例
假设有哈希索引：
```sql
CREATE HASH INDEX idx_hash_dept_salary ON employees(department, salary);
```

查询：
```sql
WHERE department = 'IT' AND salary = 60000 AND age > 25
```

**哈希索引分析**：
- 需要 department 和 salary 都有等值条件
- department = 'IT' ✓
- salary = 60000 ✓
- 可以使用哈希索引

**选择结果**：
```cpp
index_conds = [
    Condition(department, OP_EQ, 'IT'),
    Condition(salary, OP_EQ, 60000)
]

conds = [
    Condition(age, OP_GT, 25)
]
```

在Init阶段，你需要根据传入的index_conds正确地生成对应的Low Key和High Key。并且需要能够正确处理`<`和`>`的情况。 
比如，对于下面的条件：
```C++
// 选择 idx_dept_salary
index_conds = [
    Condition(department, OP_EQ, 'IT'),
    Condition(salary, OP_GT, 50000)
]

// 剩余条件
conds = [
    Condition(city, OP_EQ, 'Beijing')  // 需要额外的Filter操作
]
```
生成的Low key为`<'IT',50000>`，High key为`<String.Max(), Integer.Max()>`。加入表中刚好存在`<'IT',50000>`，由于比较符号中存在OP_GT(`>`)，所以该值应该被过滤。

### 附加题f1: 哈希索引

Hash索引是数据库系统中另一种重要的索引结构，与B+树索引相比具有不同的特点和适用场景。Hash索引基于哈希函数将键映射到存储桶中，提供O(1)的平均查找时间复杂度，但不支持范围查询和有序遍历。

#### 核心思想
Hash索引使用哈希函数将索引键映射到固定数量的存储桶（bucket）中。每个存储桶可以存储多个键值对，当发生哈希冲突时，同一个桶中的多个条目通过链式存储或开放寻址等方式处理。

#### 主要特点
- **等值查询高效**：通过哈希函数直接定位到目标桶，平均时间复杂度为O(1)
- **不支持范围查询**：由于哈希函数的随机性，相邻的键可能被映射到完全不同的桶中
- **不支持有序遍历**：键在桶中的存储顺序与原始键的顺序无关
- **冲突处理**：当多个键映射到同一个桶时，需要通过链式存储等方式解决冲突

需要完成的代码在`src/storage/index/index_hash.cpp`和`src/storage/index/index_hash.h`。

#### WSDB中的Hash索引页面组织
WSDB中的Hash索引采用三层页面结构来组织数据：

1. 头部页面（Header Page）
- **页面ID**: `FILE_HEADER_PAGE_ID` (通常为0)
- **作用**: 存储Hash索引的全局元信息
- **主要字段**:
  - `bucket_count_`: 桶的总数量
  - `total_entries_`: 索引中键值对的总数
  - `next_page_id_`: 用于分配新页面的计数器

2. 目录页面（Directory Page）
- **页面ID**: `HASH_KEY_PAGE` (固定为1)
- **作用**: 存储桶页面ID的目录，实现从桶索引到实际存储页面的映射
- **主要字段**:
  - `bucket_page_ids_[]`: 存储每个桶对应的页面ID数组
  - 如果某个桶尚未创建，对应的页面ID为`INVALID_PAGE_ID`

3. 桶页面（Bucket Page）
- **页面ID**: 动态分配（从页面2开始）
- **作用**: 实际存储键值对数据
- **主要字段**:
  - `next_page_id_`: 指向溢出页面的指针，用于处理桶容量不足的情况
  - `entry_count_`: 当前页面中存储的条目数量
  - `data_[]`: 存储序列化的键值对数据

#### 数据组织流程
1. **插入操作**: 通过哈希函数计算键的桶索引 → 在目录页面中查找对应的桶页面ID → 在桶页面中插入键值对
2. **查找操作**: 计算键的桶索引 → 定位桶页面 → 在桶页面及其溢出页面中线性搜索匹配的键
3. **溢出处理**: 当桶页面容量不足时，创建新的溢出页面并通过链表连接

#### 哈希函数设计
WSDB使用Record类内置的Hash()方法计算哈希值，然后对桶数量取模得到桶索引：
```cpp
size_t bucket_index = key.Hash() % bucket_count_;
```

#### 冲突解决
采用链式存储方式解决哈希冲突：
- 每个桶对应一个或多个页面
- 当页面容量不足时，通过`next_page_id_`字段链接到溢出页面
- 在同一个桶中线性搜索匹配的键

#### 范围查询的特殊处理
由于Hash索引不支持高效的范围查询，`SearchRange`方法需要扫描所有桶并在应用层过滤结果，这是Hash索引的固有限制。

#### 迭代器实现
Hash索引的迭代器需要按桶的顺序遍历所有键值对，由于Hash索引不保证顺序，迭代结果的顺序是不确定的。

## 开放问题f2: 索引性能分析（0 pts）
请尝试构造测试样例与测试脚本，分析不同索引在不同过滤条件下的查询性能，并在报告中写下你的结论与思考。

## 作业评分与提交

### 评分标准

1. 实验报告（10%）：实现思路，遇到的困难以及如何解决的，实验结果，对框架代码和实验的建议，以及在报告中出现的思考等，请尽量避免直接粘贴代码，建议2-4页。

2. 功能分数（90%）：
    1. t1需要通过`test/storage/bptree_test.cpp`中的所有Gtest测试。

    2. t2 需要<u>**顺序**</u>通过`wsdb/test/sql/lab04/basic`下的SQL测试并与`expected`输出比较，无差异获得该小题满分。

    3. f1需要通过`test/storage/hashindex_test.cpp`中的所有Gtest测试。<u>**顺序**</u>通过`wsdb/test/sql/lab04/bonus`下的SQL测试并与`expected`输出比较，无差异获得该小题满分。

**重要：请勿尝试抄袭代码或搬运他人实验结果，我们会严格审查，如被发现将取消大实验分数，情节严重可能会对课程总评产生影响!!!**

3. 测试方法：编译`wsdb`，`client`，`cd`到可执行文件目录下并启动两个终端分别执行：
   
   ```shell
   $ ./wsdb
   $ ./client
   ```
   
   关于client的更多用法可参考参考[开始之前](./00basic.md)或使用-h参数查看。如果`wsdb`因为端口监听异常启动失败（通常原因是已经启用了一个wsdb进程或前一次启动进程未正常退出导致端口未释放），需要手动杀死进程或者等待一段时间wsdb释放资源后再重新启动。

   * 提示：你可以cd到`wsdb/test/sql/`目录下通过脚本`evaluate.sh`进行测试。

      ```bash
      $ bash evaluate.sh <build directory> <lab directory> <sql directory>
      # e.g. bash evaluate.sh /path/to/wsdb/build lab03 bonus
      ```

### 提交材料

1. 实验报告（提交一份PDF，命名格式：lab4\_学号\_姓名.pdf）：请在报告开头写上相关信息。
   
   | 学号     | 姓名 | 邮箱                      | 完成题目 |
      | -------- | ---- | ------------------------- | -------- |
   | 12345678 | 张三 | zhangsan@smail.nju.edu.cn | t1/t2/f1/f2    |

2. 代码：`wsdb/src`文件夹

*提交示例：请将以上两部分内容打包并命名为lab4\_学号\_姓名.zip（例如lab4_123456_张三.zip）并上传至提交平台，请确保解压后目录树如下：*

```
├── lab4_123456_张三.pdf
└── src
 ├── CMakeLists.txt
 ├── common
 ├── concurrency
 ├── execution
 ├── expr
 ├── log
 ├── main.cpp
 ├── net
 ├── optimizer
 ├── parser
 ├── plan
 ├── storage
 └── system
```
