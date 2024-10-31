# 实验 1: 存储管理

## 引言

数据库管理系统（RDBMS）从诞生开始只需要解决一个问题，就是如何有效地管理结构化数据从而避免重复造轮子。类比来说，一个数据库文件就好比一个Excel文件，但数据库能高效管理海量数据，其中关键优化点在于磁盘IO的优化。本次实验我们将深入WSDB的数据存储格式以及缓冲区管理器的实现。

## 磁盘管理器

磁盘管理器主要负责管理文件标识符与文件名之间的映射，以及负责内存和磁盘之间的数据交换，提供磁盘文件创建、打开、关闭、删除，以及页面、文件、日志的读写接口。

请在开始实验之前阅读文件`storage/disk/disk_manager.h`中的相关代码，并理解函数的异常和返回值处理。

## 缓冲区管理器

我们在操作系统中学过，内存被划分成一个个页面，当出现内存数据缺失时，内存管理单元（MMU）通过某种页面淘汰策略从所有页面中选出一个最近不经常使用的页面并标记为空闲，意味着当前页面被淘汰出去，如果页面为脏还需要重新写入磁盘。缓冲区管理器拥有完全一致的功能，对于一个庞大的数据库文件，将整个数据库全部载入内存是不现实的。一个数据库文件被划分成大小相同的多个页面（Page），在系统读取数据时，会首先在内存中寻找目标页面，如果出现页面缺失，缓冲区管理器选择一个可被淘汰的页面置换回硬盘。

数据库中，缓冲区是一段连续的内存空间，被划分成与页面大小相等的物理帧（Frame）。一个物理帧需要管理三个信息：载入当前帧的页面数据、指示该页面是否被修改的脏页标记、以及当前帧被多个线程同时访问的引用计数。缓冲区管理器自身维护一个空闲帧链表，出现页面缺失时首先从空闲列表中取出空闲帧并读取硬盘。如果没有空闲帧，系统将根据置换策略获取最合适的置换帧。

## 句柄

#### 页面句柄

页面句柄（PageHandle）负责将页面中的序列化数据反序列化出来，并负责元组的插入删除和读取。页面由页头和槽数据组成，页头位于页面的开头固定字节的内存段，分别为：1、当前页面上最后一个写回硬盘的日志序列号；2、下一个拥有空闲槽位的页面ID；3、当前页面上的记录个数。

下图展示了行存模式下（NAry PageHandle）的页面组织格式：

```
|<-------------------- Page Header ------------------->|<------------------slot memory---------------->|
| page last LSN | next_free_page_id | number of record | bitmap | record 1 | record 2 | ... | record n |
```

紧跟页头的是槽数据，不同数据库对槽数据的排布方式不同，但总体上可以分为两部分：指示槽位是否空闲的Bitmap，以及元组的实际数据信息。Bitmap用于指示某个槽位的内存空间是否空闲，例如，如果需要在slot_id =8的位置插入一个元组，页面句柄会首先检查第8位是否已经有记录，如果已有记录会抛出记录已存在的异常，如果没有会首先将Bitmap的第8位置为1，然后将数据写入槽位。

#### 记录句柄

WSDB采用**定长数据**的组织形式，即在表创建时一条记录的长度就已经确定。定长记录的好处是一条记录的起始位置通过简单的偏移量计算就能获得，并且在插入删除时不会产生碎片化内存（*想想看为什么*），从而不需要额外线程清理碎片化空间。

对于变长记录，一条记录由记录头信息和记录数据组成。

```
| slot id | prev_rec_off | next_rec_off | record_lenth | nullmap | record data |
```

记录句柄（Record Handle）是对一条记录的抽象，包括记录模式（Record Schema）、记录头、以及实际数据，负责管理记录数据的解析和中间结果的计算。`slot id`为当前槽的id，用于定位记录在本页面的位置；`prev_rec_off`，`next_rec_off`分别为前一条和后一条记录起始位置相对本记录起始位置的偏移，`record_lenth`为`record data`部分的长度；`nullmap`指示该记录中的哪些列为空。对于定长数据，上述页面组织形式已经能够满足数据存储和搜索的需求，对于变长数据，其页面组织形式与定长数据略有不同，会从页尾存放第一条记录，感兴趣的同学可以自行搜索了解。

WSDB中一条记录也有记录头信息，但由于采取定长记录组织形式，因此上图的字段中只保留了`nullmap`字段。

##### 表句柄

表句柄（Table Handle）通过封装页面句柄从而向上层提供针对记录的增删改查接口以及对整个表的遍历操作。

表头（Table Header）是表的第一页，存储了表的元信息，在表句柄中Table Header格式如下：

```
|<------------------------------------------------- Table Header ------------------------------------------------>|
| page_num_ | first_free_page_ | rec_num_ | rec_size_ | rec_per_page_ | field_num_ | bitmap_size_ | nullmap_size_ |
```

其中，`page_num_ `表示表中页面的数量，`first_free_page_ `记录了表中第一个空闲页面的页面号，`rec_num_`,`rec_size_`,`rec_per_page_`分别表示表中记录的数量，一条记录的大小和每页能存储的记录数量，`field_num_`表示表中的属性数，`bitmap_size_`,`nullmap_size_`分别表示页面句柄中`bitmap`的大小和记录句柄中`nullmap`的大小。

## 实验要求

本次实验需要同学们完成缓冲区管理器和表句柄的相关内容并通过相关单元测试。假设仓库目录名为`wsdb`，服务器代码文件均在`wsdb/src/`目录下。你只需要修改或添加`src`文件夹下的文件，如果遇到不在`WSDB_ERRORS`（`wsdb/common/errors.h`）列表中的未知异常，请使用`WSDB_EXCEPTION_EMPTY`，并在报告中写下你遇到的特殊情况。请完成所有标注`WSDB_STUDENT_TODO`宏的函数，并在完成后将宏删除。

### t1. LRU (20 pts)

LRU（Least Recently Used）是最经典也是实现相对简单的淘汰策略，具体来说，当缓冲区满时，算法会淘汰最近最少被访问的页面，以便为新的页面腾出空间。

在WSDB中，页面替换策略位于`storage/buffer/replacer`文件夹下，继承自同一基类`Replacer`。

`Replacer`需要支持如下接口：

```c++
/**
* Remove the victim frame as defined by the replacement policy.
* @param[out] frame_id id of frame that was removed, nullptr if no victim was found
* @return true if a victim frame was found, false otherwise
*/
virtual auto Victim(frame_id_t *frame_id) -> bool = 0;

/**
* Pins a frame, indicating that it should not be victimized until it is unpinned.
* @param frame_id the id of the frame to pin
*/
virtual void Pin(frame_id_t frame_id) = 0;

/**
* Unpins a frame, indicating that it can now be victimized.
* @param frame_id the id of the frame to unpin
*/
virtual void Unpin(frame_id_t frame_id) = 0;

/** @return the number of elements in the replacer that can be victimized */
virtual auto Size() -> size_t = 0;
```

* `Victim`：选择一个帧中的页面淘汰，并将该帧的帧号返回给调用者。
* `Pin`：固定一帧，使得该帧中的页面不会被淘汰，直到`Unpin`被调用。
* `Unpin`：取消固定一帧，被取消的帧可以是没被`Pin`过的，此时直接返回即可。
* `Size`：返回能够被淘汰的帧的个数。

`LRUReplacer`类中定义的成员变量如下：

- `std::mutex latch_` 用于并发控制。

- `std::list<std::pair<frame_id_t, bool>> lru_list_` 存储帧的id以及记录该帧是否可被淘汰的标志位。

- `std::unordered_map<frame_id_t, std::list<std::pair<frame_id_t, bool>>::iterator> lru_hash_` 维护所有帧id到`lru_list_`上的迭代器的映射。

- `size_t cur_size_` 表示当前**可被淘汰**的帧的数量。

- `size_t max_size_` 表示帧的最大数量。

下面是你需要完成的函数，位于文件`storage/buffer/replacer/lru_replacer.cpp`中：

- `auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool;`
  根据LRU策略淘汰缓冲池中一个帧中的页面并传回`frame_id`，如果成功淘汰了一个页面返回`true`，否则返回`false`。

- `void LRUReplacer::Pin(frame_id_t frame_id);`
  固定id为`frame_id`的帧，如果该帧不在`lru_list_`中，应先将其加入。
  **注意**：固定id为`frame_id`的帧应该被视作对此页面进行访问，因此调用`Pin`函数后该页面即为最近访问的页面。

- `void LRUReplacer::Unpin(frame_id_t frame_id);`
  取消固定id为`frame_id`的帧。

- `auto LRUReplacer::Size() -> size_t;`，返回可以被淘汰的帧数量。

下面列举一些例子，对期望你实现的LRU策略进行说明：

* 例1： 假设对于一组页面{0,1,2,3,4,5,6,7}的一个访问序列为[pin(3),unpin(3),pin(2),unpin(2),pin(5),unpin(5),pin(3),unpin(3),pin(2),unpin(2),pin(6),unpin(6),pin(7)]，如果缓冲区大小为4帧，则在序列执行到最后一项pin(7)时，会将5号页面淘汰并将7号页面的数据加载到内存中，此时缓冲区中LRU优先级序列为[7,6,2,3]，序列中页面靠前表示其更近被使用。

* 例2： 假设对于一组页面{0,1,2,3,4,5,6,7}的一个访问序列为[pin(3),pin(2),unpin(2),pin(5),unpin(5),pin(6),unpin(6),pin(4)]，如果缓冲区大小为4帧，则在序列执行到最后一项pin(4)时，会将2号页面淘汰并将4号页面的数据加载到内存中，此时缓冲区中LRU优先级序列为[4,6,5,3]。

* 例3： 假设对于一组页面{0,1,2,3,4,5,6,7}的一个序列为[pin(3),pin(2),pin(5),pin(6),unpin(6),pin(7)]，如果缓冲区大小为4帧，则在序列执行到最后一项pin(7)时，会将6号页面淘汰并将7号页面的数据加载到内存中，此时缓冲区中LRU优先级序列为[7,5,2,3]。

### t2. Buffer Pool Manager (30 pts)

缓冲区管理器主要负责内存缓冲区中的页面管理。

`BufferPoolManager`类中定义了以下类成员变量：

- `std::mutex latch_` 用于并发控制。

- `DiskManager *disk_manager_` 磁盘管理器，提供磁盘和缓冲区之间的页面读写接口。

- `LogManager *log_manager_` 日志管理器。

- `std::unique_ptr<Replacer> replacer_` 页面替换策略，调用你在t1中实现的接口。

- `std::array<Frame, BUFFER_POOL_SIZE> frames_` 用于存储缓冲区中的数据页面。

- `std::list<frame_id_t> free_list_` 用于记录缓冲区中空闲数据页面的标识符。

- `std::unordered_map<fid_pid_t, frame_id_t> page_frame_lookup_` 维护磁盘页面标识符（file id和page id）到缓冲区中数据页面标识符（frame id）的映射。

下面是你需要完成的函数，位于文件`storage/buffer/buffer_pool_manager.cpp`中：

* `BufferPoolManager::BufferPoolManager(DiskManager *disk_manager, wsdb::LogManager *log_manager, size_t replacer_lru_k);`
  补充类构造函数，对成员变量进行必要的初始化。

* `auto BufferPoolManager::FetchPage(file_id_t fid, page_id_t pid) -> Page *;`
  返回缓冲区中对应的页面，如果页面不在缓冲区中，将其添加后返回。

* `auto BufferPoolManager::UnpinPage(file_id_t fid, page_id_t pid, bool is_dirty) -> bool;`
  取消固定页面，并正确设置页面的脏位。

* `auto BufferPoolManager::DeletePage(file_id_t fid, page_id_t pid) -> bool;`
  将页面从缓冲区中删除，注意需要将脏页写回磁盘。
- `auto BufferPoolManager::DeleteAllPages(file_id_t fid) -> bool;`
  删除缓冲区中指定文件对应的所有页面。

- `auto BufferPoolManager::FlushPage(file_id_t fid, page_id_t pid) -> bool;`
  将缓冲区中的页面写到硬盘中。

- `auto BufferPoolManager::FlushAllPages(file_id_t fid) -> bool;`
  将缓冲区中指定文件的所有页面写回磁盘。

- `auto BufferPoolManager::GetAvailableFrame() -> frame_id_t;`
  获取一个缓冲区中的空闲数据页面。如果没有空闲页面，执行页面替换策略，替换失败需要抛出`WSDB_NO_FREE_FRAME`异常。

- `void BufferPoolManager::UpdateFrame(frame_id_t frame_id, file_id_t fid, page_id_t pid);`
  更新缓冲区中指定的页面。

更具体的实现步骤说明可以查看文件`storage/buffer/buffer_pool_manager.h`中的函数注释。

在你开始实现以上函数前，**建议首先阅读以下文件内容**，其中包含你可能会用到的函数接口：

- `storage/disk/disk_manager.h`

- `storage/buffer/frame.h`

- `common/page.h`

### t3. Table Handle (40 pts)

实现表句柄对记录的“增删改查”接口。

`TableHandle`类中主要定义了以下类成员变量：

- `tab_hdr_`为表的元数据。

- `table_id_`为表的id。

- `disk_manager_`为磁盘管理器。

- `buffer_pool_manager_`为缓冲区管理器。

- `schema_` 为表中存储的记录的模式

你需要完成以下几个函数：

* `auto GetRecord(const RID &rid) -> RecordUptr;`
  给定RID，返回对应位置的记录数据。

* `auto InsertRecord(const Record &record) -> RID;`
  两个Insert函数，分别是在指定位置插入一条记录，以及在任意空闲位置插入一条数据。

* `void DeleteRecord(const RID &rid);`
  给定RID，删除对应位置的记录数据。

* `void UpdateRecord(const RID &rid, const Record &record);`
  给定RID，用新记录数据覆盖旧记录。

具体实现步骤和辅助函数请参考`system/handle/table_handle.cpp`和`system/handle/table_handle.h`，建议在开始实现前阅读以下文件：

* `system/handle/page_handle.h`
* `system/handle/record_handle.h`
* `common/bitmap.h`
* `common/meta.h`

### 附加实验 f1：LRU K Replacer (5 pts)

LRU替换算法可能导致页面震荡，具体来说，对于一批周期性出现的页面访问序列[5,2,3,1,5,2,3,1,5,2,3,1,5]，如果缓冲区只有3帧，则从第四次页面访问开始（此时访问1号页面），每一次都会出现缺失。

LRU K就是为解决这一问题提出的改进版LRU算法，其规则如下：

**定义**：数列中，相对当前访问的索引位置，一个数字$e$最后一次出现记为$backward_1^e$，倒数第k次出现记为$backward_k^e$。

例如，上述访问序列中，索引指向序列最后一位（第13位，索引从1起）页面1的$backward_1^1= 12$ ，$backward_2^1 = 8$，$backward_3^1 = 4$，$backward_4^1 = na$。

**定义**：数列具有时间访问顺序，对于一组顺序访问的序列，在当前访问的索引位置为$i_c$定义某个数$e$的backward k distance为如下函数

$$
d_k^e = \left\{\begin{aligned}& +\text{inf} &\text{if }backward_k^e = na \\& i_c - backward_k^e &\text{other}\end{aligned}\right.
$$

索引指向序列第13位，上述索引序列中

$d_3^1 = 13 - 4 = 9$，$d_3^2 = 13 - 2 = 11$，$d_4^2 = +inf$。LRU K算法选择具有最大$d_k^e$的页面$e$作为被淘汰页面，如果有多个页面满足$d_k^e =+inf$，选择第一次出现时索引最小的那个页面。例如，$d_4^2 = d_4^3 = d_4^1 = +inf$，由于2号页面出现的最早，索引最小，所以2号页面被淘汰。

请完成`storage/buffer/replacer/lru_k_replacer.cpp`，`storage/buffer/replacer/lru_k_replacer.h`中的公有函数，实现方式和相关数据结构定义仅供参考，除公有函数的声明之外，其余数据结构可以随意修改。

### 附加实验 f2: PAX Page Handle (5 pts)

请确保已经了解并掌握了WSDB中页面的组织形式，以更好上手本实验的内容

PAX存储格式是一种行列混存的格式，其优势在于能够快速访问和抽取一页中的部分列数据。在OLAP任务中，列式存储利于数据分析算子进行有效的聚合运算和向量化加速。而PAX存储相比列室存储既能快速取出某一记录，也能做到读取整列数据。在WSDB中，PAX页面格式如下（与NAry模式存储的主要区别在slot memory部分）：

```
|<-------------------- Page Header ------------------->|
| page last LSN | next_free_page_id | number of record |
|<-------------------- Slot Memory ------------------->|
| bitmap |     nullmap_1, nullmap_2, ..., nullmap_n    |
|    col_1_1, col_1_2,      ...            , col_1_n   |
|    col_2_1, col_2_2,      ...            , col_2_n   |
|                           ...                        |
|    col_m_1, col_m_2,      ...            , col_m_n   |
```

Page Header和bitmap部分与NAry相同，指示当前页面上哪些位置的记录是有效的。紧跟bitmap的是一串nullmap区域，与NAry模式存储下的nullmap定义相同，一共有n个连续存储的nullmap。nullmap区域之后是以列为单位组织的存储空间，每一列的n个数据被连续存放。对于某个槽位的记录访问，PageHandle需要正确定位到每一列，并拼接成一条完整的记录返回到调用者；对于槽位写入需要定位到单元在文件中的准确位置并写入当前列上的值。每一列的起始位置由`TableHandle`计算并存放在`field_offset_`成员变量中。

PAX Page Handle支持整列的读取（ReadChunk），给定一个记录模式（RecordSchema），需要返回当前页上请求的所有列，将一列数据组织成数组值（ArrayValue）并将所有列打包为一个Chunk返回给调用者。

更多实现细节参考`system/handle/page_handle.cpp`，`system/handle/page_handle.h`中`PAXPageHandle`相关的内容。

## 作业评分与提交

### 评分标准

1. 实验报告（10%）：实现思路，优化技巧，实验结果，对框架代码和实验的建议，以及在报告中出现的思考等，请尽量避免直接粘贴代码，建议2-4页。
2. 功能分数（90%）：需要通过`wsdb/test`目录下的单元测试，请将测试结果写在实验报告中。
   * t1：你需要通过`wsdb/test/storage/replacer_test.cpp`中的LRU测试以获得本题目的功能分数。
   * t2：你需要通过`wsdb/test/storage/buffer_pool_manager_test.cpp`中的测试以获得本题目的功能分数。
   * t3：你需要通过`wsdb/test/system/table_handle_test.cpp`中Simple和MultiThread测试来获得本题目的功能分数。
   * f1：你需要通过`wsdb/test/storage/replacer_test.cpp`中的LRUK测试以获得该题目的功能分数。
   * f2：你需要通过`wsdb/test/system/table_handle_test.cpp`中的PAX_MultiThread测试以获得本实验的功能分数。

**重要：请勿尝试抄袭代码或搬运他人实验结果，我们会严格审查，如被发现将取消大实验分数，情节严重可能会对课程总评产生影响!!!**

### 提交材料

1. 实验报告（提交一份PDF，命名格式：lab1\_学号\_姓名.pdf）：请在报告开头写上相关信息。
   
   | 学号     | 姓名 | 邮箱                      | 完成题目    |
   | -------- | ---- | ------------------------- | ----------- |
   | 12345678 | 张三 | zhangsan@smail.nju.edu.cn | 1/2/3/f1/f2 |

2. 代码：`wsdb/src`文件夹

*提交示例：请将以上两部分内容打包并命名为lab1\_学号\_姓名.zip（例如lab1_123456_张三.zip）并上传至提交平台，请确保解压后目录树如下：*

```
├── lab1_123456_张三.pdf
└── src
    ├── CMakeLists.txt
    ├── analyse
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
