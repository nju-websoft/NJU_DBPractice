# 实验 2：执行引擎（上）

## 引言

复杂的SQL语句是怎样解析并修改和查询数据库中的数据的？本次实验同学们将完成几个基本的执行器并能够执行如下SQL语句：

```sql
select name, score, remark from nju_db where group_id = 2 and l1_score > 90;  
```

## 数据流

在WSDB中，一条SQL语句的前处理执行流程如下：

```mermaid
flowchart LR
	SQL(sql)--客户端发送到服务端-->sys(Server)--Parser-->ast(语法树)
	ast--Analyser-->plan(查询计划)--Optimizer-->opt(优化后的查询计划)
```

后处理执行流程如下：

```mermaid
flowchart LR
	opt(优化后的查询计划)--Executor-->exec(根据查询计划生成的算子树)--Executor-->res(执行结果)--Net-->cl(返回客户端)
```

本次实验需要同学们实现后处理流程中Executor的内容，理解和掌握火山模型的计算流程。

## 火山模型与算子

对于引言中的例子，其算子树如下图所示：

<img src="./02lab2-executor1.assets/valcano.png" alt="valcano" style="zoom:50%;" />

在WSDB中，每个算子都由`Init`，`Next`，`IsEnd`接口组成，分别用于算子资源的初始化，获取下一条记录，以及判断算子计算是否结束。

火山模型的优点是逻辑简单，可以通过简单的算子实现复杂的查询功能。但是缺点也显而易见，即每次获取下一条记录时都需要调用一次`Next`函数，大幅降低了计算速度。因此，向量执行引擎被提出，该模型与列存模型能够很好地兼容，感兴趣的同学可以自行查阅资料了解。

## 实验要求

本次实验需要完成基本算子并通过SQL测试。假设仓库目录名为`wsdb`，服务器代码文件均在`wsdb/src/`目录下。你只需要修改或添加`src`文件夹下的文件，如果遇到不在`WSDB_ERRORS`（`wsdb/common/errors.h`）列表中的未知异常，请使用`WSDB_EXCEPTION_EMPTY`，并在报告中写下你遇到的特殊情况。请完成所有标注`WSDB_STUDENT_TODO`宏的函数，并在完成后将宏删除。

### t1: 基本执行算子（90 pts）

本次实验需要完成除DDL语句（数据描述语句），表连接算子，聚合算子，索引扫描算子以外的其余所有算子的`Init`, `Next`, `IsEnd`函数。包括基本算子和用于增删改的DML语句（数据操作语句），并通过SQL测试。请仔细阅读`execution/executor_ddl.cpp`中`ShowTablesExecutor`的逻辑，它能够帮你快速理解火山模型的执行过程。关于`Init`，`Next`，和`IsEnd`的调用顺序，请参考`execution/executor.cpp`中的`Execute`函数。

具体来说，你需要确保以下列表中的接口都已被正确实现，才能够执行如下SQL：

```sql
select name, score, remark from nju_db where group_id = 2 and l1_score > 90 order by desc score limit 10; 
```

* `execution/executor_insert.cpp`：插入记录，需要同时插入表格和索引
* `execution/executor_seqscan.cpp`：实现全表扫描
* `execution/executor_limit.cpp`：限制结果集大小
* `execution/executor_projection.cpp`：投影操作
* `execution/executor_delete.cpp`：删除记录，需要同时在表格和索引中删除
* `execution/executor_update.cpp`：更新记录，需要同时更新表格和索引
* `execution/executor_filter.cpp`：过滤掉不符合条件的记录
* `execution/executor_sort.cpp`：内排序，中间结果能全部载入内存，根据给定列模式进行升序或降序排序，目前只需要支持全列降序或全列升序，即不需要支持单列顺序，语法为order by (asc,desc,_) \<column list\>。

建议阅读代码列表：

* `execution/executor_ddl.cpp`
* `system/handle/record_handle.h`
* `system/handle/index_handle.h`
* `common/value.h`

### 附加实验 f1: 嵌套循环内连接与归并排序（10pts）

`t1`的executor_sort假定排序的中间结果能够全部载入内存。然而在大多数应用场景中，可能需要对大量数据进行排序，并且往往没有足够的内存空间支持内排序。在本次实验中，你需要在前一次实验实现的内排序基础上完成外排序，并额外实现嵌套循环连接的内连接算子，能够执行以下SQL，该SQL会对两张含有1000条记录的表做一次笛卡尔积，并对得到的1000*1000条数据根据i_id和s_i_id排序：

```sql
select i_id, s_i_id from item, stock order by i_id, s_i_id;
```

归并排序是最经典的外部排序算法之一。具体实现上又被称为k路归并排序。具体来说，一个文件为一路，共两组文件。一组文件作为读入文件，另一组文件负责接收当前归并之后的结果。比如有如下一组数列[5,3,4,6,2,6,7,3,0,5,3,1,7,8,2]按照升序进行3路归并排序且内存中可用于排序的缓存大小为2，首先进行准备工作进行初始文件划分。按照读入顺序将排序缓存的数据依次写入3个文件，写完后更换下一个文件继续写。完成后三个文件中数据如下

```
|3,5|3,7|7,8|
```

```
|4,6|0,5|2|
```

```
|2,6|1,3|
```

首先准备下一组文件中的第一个文件，该文件中记录了上述三个文件中的第一列数据的排序结果。维护一个大小等于k的小根堆，从三个文件中将第一个数读出，分别为3,4,2。此时堆中最小数为2，在输出文件中写入2，并在2所在的第三个文件中取出下一个数字6，加入到堆中。此时堆内数字为3，4，6，最小数字为3，在输出文件中再写入数字3，从3所在的第一个文件中读取下一个数字5，加入到堆中。依此类推，直到第一个文件准备完成。

```
|2,3,4,5,6,6|
```

同理，准备第二个文件

```
|0,1,3,3,5,7|
```

第三个文件

```
|2,7,8|
```

这时我们已经将上一组的所有数据做了归并。需要注意，该归并结果没有出现第二列，如果在上一组文件中出现更多列，需要将剩余数据再次从第一个文件开始写入直到上一组文件中的数据全部做了归并。由于本组文件只有一列，因此通过一次归并就得到最终结果：

```
|0,1,2,2,3,3,3,4,5,5,6,6,7,7,8|
```

WSDB中`k=SORT_WAY_NUM`默认为10，一个buffer能够支持的最大记录数为`max_rec_num_=SORT_BUFFER_SIZE/record_length`。相关常量定义见`common/config.h`。

嵌套循环内连接的python伪代码如下：

```python
def nestedloop_join(left, right, condition):
    res = []
    for t1 in left:
        for t2 in right:
            j = t1.join(t2)
            if(condition.eval(j)):
                res.append(j)
    return res
```

## 作业评分与提交

### 评分标准

1. 实验报告（10%）：实现思路，优化技巧，实验结果，对框架代码和实验的建议，以及在报告中出现的思考等，请尽量避免直接粘贴代码，建议2-4页。

2. 功能分数（90%）：需要通过`wsdb/test/sql`目录下的SQL语句测试。

   * t1: 完成`wsdb/test/sql/lab02/t1`下的SQL测试并与`expected`输出比较，无差异获得该小题满分，测试文件分值分别为

     * `01_prepare_table_dbcourse.sql`: 15 pts
     * `02_seqscan_limit_projection.sql`: 30 pts
     * `03_filter_update_delete.sql`: 30 pts
     * `04_sort_final.sql`: 15 pts

   * f1: 完成`wsdb/test/sql/lab02/f1`下的SQL测试，由于排序结果集较大，仓库并未包含排序测试的预期输出，请同学们确保测试充分后再提交。

     * `04_merge_sort.sql`: 10pts

   * 提示：你可以cd到`wsdb/test/sql/lab02`目录下通过脚本`evaluate.sh`进行测试，也可以使用终端的命令行工具逐个文件测试或使用交互模式逐个命令测试。

     ```bash
     $ bash evaluate.sh <bin directory> <test sql directory>
     # e.g. bash evaluate.sh /path/to/wsdb/cmake-build-debug/bin t1
     ```

**请勿抄袭，如被发现将取消大实验分数**!!!

### 提交材料

1. 实验报告（提交一份PDF，命名格式：lab2\_学号\_姓名.pdf）：请在报告开头写上相关信息。

   | 学号     | 姓名 | 邮箱                      | 完成题目 |
   | -------- | ---- | ------------------------- | -------- |
   | 12345678 | 张三 | zhangsan@smail.nju.edu.cn | t1/f1    |

2. 代码：`wsdb/src`文件夹

*提交示例：请将以上两部分内容打包并命名为lab2\_学号\_姓名.zip（例如lab2_123456_张三.zip）并上传至教学立方，请确保解压后目录树如下：*

   ```
├── lab2_123456_张三.pdf
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

   



