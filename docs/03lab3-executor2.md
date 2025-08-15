# 实验 3：执行引擎（下）

## 引言

数据库需要具有一定的数据分析能力，其中，表连接和列聚合是最常见的数据分析操作。本次实验同学们将继续完善执行引擎并完成JOIN和AGGREGATE算子以支持更复杂的查询。例如

```sql
select id, count(score) as cnt_sc, max(subject.name) from (
    select * 
    from grade outer join subject 
    where subject.name=grade.name) 
order by desc id 
group by id 
having id=3 and avg(score) > 90.0;
```

在数据库系统中，连接（Join）和聚合（Aggregate）是实现复杂数据分析和查询的基础操作。理解这些概念有助于后续实验中相关算子的实现和调试。

## 内连接（Inner Join）
内连接是最常见的连接操作之一。它用于将两个表中满足连接条件的记录组合在一起。只有当两个表中的记录在连接字段上匹配时，结果中才会包含这些记录。换句话说，内连接返回的是两个表中“交集”部分的数据。常见的应用场景包括多表联合查询、数据筛选等。

## 外连接（Outer Join）
外连接分为左外连接（Left Outer Join）、右外连接（Right Outer Join）和全外连接（Full Outer Join）。与内连接不同，外连接不仅返回满足连接条件的记录，还会保留一张表中未匹配到的记录，并为另一张表中缺失的部分补充空值（NULL）。例如，左外连接会保留左表的所有记录，即使右表中没有匹配项。外连接常用于需要保留某一方全部数据的场景。

## 聚合（Aggregate）
聚合操作是对一组数据进行统计计算的过程，常见的聚合函数包括计数（COUNT）、求和（SUM）、平均值（AVG）、最大值（MAX）、最小值（MIN）等。聚合通常与分组（GROUP BY）结合使用，用于对数据进行分类统计。例如，可以统计每个学生的总成绩、每门课程的最高分等。聚合操作是数据分析和报表生成的重要基础。

## 实验要求
本次实验需要完成Join和Aggregate算子并通过SQL测试。假设仓库目录名为wsdb，服务器代码文件均在wsdb/src/目录下。你只需要修改或添加src文件夹下的文件，如果遇到不在WSDB_ERRORS（wsdb/common/errors.h）列表中的未知异常，请使用WSDB_EXCEPTION_EMPTY，并在报告中写下你遇到的特殊情况。请完成所有标注WSDB_STUDENT_TODO宏的函数，并在完成后将宏删除。

### t1： 嵌套循环内连接（Nested Loop Inner Join）（20 pts）
嵌套循环连接是最朴素，支持的连接条件最丰富的方法。嵌套循环内连接用伪代码表示如下：
```python
def nested_loop_inner_join(left, right, condition):
    res = []
    for t1 in left:
        for t2 in right:
            j = t1.join(t2)
            if condition.eval(j):
                res.append(j)
    return res
```
本题需要你完成`src/execution/executor_join_nestedloop.cpp`中的`Inner Join`部分。

### t2：循环嵌套外连接（30 pts）
本次实验只需要完成<**左外连接**>，如果当前检查的左表中的记录在右表中无法找到满足连接条件的记录，那么需要生成一个值全为NULL的空记录并与左表的当前记录连接。否则如果连接的记录满足连接条件，这不需要生成空记录，只需要返回所有满足条件的连接记录即可。伪代码表示如下：

```python
def nested_loop_outer_join(left, right, condition):
    res = []
    for t1 in left:
        has_record = false
        for t2 in right:
            j = t1.join(t2)
            if condition.eval(j):
                has_record = true
                res.append(j)
        if not has_record:
            j = t1.join(t2.null_record())
            res.append(j)
```
本题需要你完成`src/execution/executor_join_nestedloop.cpp`中的`Outer Join`部分。

### t3：聚合算子（40 pts）
聚合是将表中多个记录通过聚合函数映射到单个记录的操作。例如最简单的聚合查询`select count(*) from table`，就是将table中的所有记录映射到一条记录，该记录仅有一个值表示表中的所有记录的个数。本次实验需要完成五个聚合函数：
* `COUNT(*)`：统计底层算子传入的记录数量，若均为空或底层算子传入记录数为0则返回0。
* `COUNT(column)`：统计底层算子传入的记录中，column非空值的记录数量，若均为空或底层算子传入记录数为0则返回0。
* `MAX, MIN(column)`：统计底层算子传入的记录中，column非空的值的最大/小值。若均为空或底层算子传入记录数为0则返回空。
* `SUM, AVG(column)`：统计底层算子传入的记录中，comlumn非空的值之和/平均。若均为空或底层算子传入记录数为0则返回空。

聚合函数通常会和`group by`以及`having`子句一起出现用来分组与过滤。例如[item表](../test/sql/lab03/basic/02_prepare_table_item.sql)，
```sql
select i_im_id, avg(i_im_id), count(i_im_id) from item where i_im_id > 25 group by i_im_id having i_im_id > 20 and sum(i_im_id) > 100
```
其语义为“对于表中所有`i_im_id>25`的列，根据`i_im_id`值分组，计算每组中`i_im_id`的平均值以及记录个数，并仅输出`i_im_id`之和大于100以及`i_im_id`值大于20的结果”。
可以看出，`where`和`having`的区别在于：1. where在聚合前对表进行过滤，having则在聚合后过滤；2. where中不允许出现聚合函数而having中允许；3. having中如果出现列名（例如上例中的having i_im_id > 20，其必须出现在group by中）。

注意事项：
1. COUNT不会返回空值，而其他函数可能返回空值。
2. 假如聚合算子记录中至少有一个有效值，则需要输出，否则如果聚合的所有结果均为空，则输出0条记录，请看下面的例子，item表中所有i_im_id均小于等于50；
```sql
wsdb> select count(*), sum(i_im_id) from item where i_im_id > 50;

+--------------+--------------+
| COUNT(*)     | SUM(i_im_id) | 
|              |              | 
+--------------+--------------+
| 0            | (null)       | 
+--------------+--------------+
Total tuple(s): 1
wsdb> select sum(i_im_id) from item where i_im_id > 50;

+--------------+
| SUM(i_im_id) | 
|              | 
+--------------+
Total tuple(s): 0
```
本题需要完成`src/execution/executor_aggregate.cpp`部分。

## 附加题 f1: 哈希连接（Hash Join）（5 pts）
Nested loop join的优点是适用于任意复杂的连接条件，但时间复杂度为O(n*m)，其中n,m分别为左右表的大小。

Hash Join支持高效的等值连接（例如left.a=right.i and left.b=right.j），可以将平均时间复杂度降低到O(n+m)。主要由两阶段组成：1. 构建哈希表：将右表中对应列的键值（上例中键值为\<right.i, right.j\>）插入到哈希表中；2. 左表probing：对于左表中的键值\<left.a, left.b\>，检查是否在哈希表中存在，若存在则与右表连接输出。

WSDB中，Hash Join只要连接条件中有至少一个等值连接就可以使用，对于其他非等值连接条件，Hash Join需要在probing阶段另外检查这些条件是否能够满足。

本题需要完成`src/execution/executor_join_hash.cpp`部分。

## 附加题 f2: 排序合并连接（Sort Merge Join）(5 pts)
Sort merge join也可以支持高效的等值连接，其时间复杂度也为O(n+m)。并且相比哈希连接，它能够支持高效的非等值连接查询。

对于等值连接，左右表分别按照升序排序，连接时通过比较左右表的键值大小来决定调用哪个表的Next操作。
举一个例子，假如左表排好序的键值为 `[1,2,3,3,4,5,6,7,7,8]`，右表键值为 `[3,4,5]`。

连接过程如下：

1. 比较左表第一个元素 1 和右表第一个元素 3，1 < 3，因此左表向后移动。
2. 比较左表 2 和右表 3，2 < 3，左表继续向后移动。
3. 左表到达 3，右表也是 3，匹配成功。由于左表有两个连续的 3，分别与右表的 3 匹配，生成两条连接结果。
4. 左表继续向后，遇到 4，右表还是 3，4 > 3，右表向后移动到 4。
5. 左表 4 和右表 4 匹配，生成一条连接结果。
6. 左表 5，右表 4，5 > 4，右表向后移动到 5。
7. 左表 5 和右表 5 匹配，生成一条连接结果。
8. 右表已到末尾，连接结束。

最终输出的连接结果为：
- 左表的两个 3 分别与右表的 3 匹配（2 条）
- 左表的 4 与右表的 4 匹配（1 条）
- 左表的 5 与右表的 5 匹配（1 条）

总共 4 条连接结果。

这种方式能够高效地处理等值连接，尤其适用于大数据量的场景。

对于非等值连接，以`<`为例，WSDB会将左表按照升序排序，右表按照将序排序，这样的好处是左表向后遍历的同时，右表如果检查到第一个不满足条件记录就可以停止。例如：左表为[1,2,3,3,4,5,6,7,7,8]，右表为[5,4,3]。
对于非等值连接（如 `<`），假设左表已按升序排序 `[1,2,3,3,4,5,6,7,7,8]`，右表按降序排序 `[5,4,3]`。连接条件为左表值 `<` 右表值。

连接过程如下：

1. 左表第一个元素 1，右表第一个元素 5。1 < 5，匹配，记录结果。右表继续向后移动，检查 1 < 4，仍然匹配，记录结果。再检查 1 < 3，依然匹配，记录结果。右表已遍历完，左表向后移动到 2，右表重置到 5。
2. 2 < 5，匹配，记录结果。2 < 4，匹配，记录结果。2 < 3，匹配，记录结果。右表遍历完，左表向后移动到 3，右表重置到 5。
3. 3 < 5，匹配，记录结果。3 < 4，匹配，记录结果。3 < 3，不成立，右表停止遍历。左表还有一个 3，重复上述过程。
4. 4 < 5，匹配，记录结果。4 < 4，不成立，右表停止遍历。
5. 5 < 5，不成立，左右表同时停止遍历

最终输出的连接结果为：

- 1 与 5、4、3 匹配（3 条）
- 2 与 5、4、3 匹配（3 条）
- 3（第一个）与 5、4 匹配（2 条）
- 3（第二个）与 5、4 匹配（2 条）
- 4 与 5 匹配（1 条）

总共 3 + 3 + 2 + 2 + 1 = 11 条连接结果。

这种排序合并方式能高效地处理非等值连接，虽然时间复杂度仍为O(m*n)，但避免了不必要的遍历和比较。

在WSDB中，非等值连接仅支持单一列比较（`left.a>right.i`，并且没有其他连接条件），等值连接支持多列等值条件（`left.a=right.i and left.b=right.j`）。跟细节的判定标准请阅读`src/optimizer/optimizer.cpp`的`OptimizeSortMergeJoin`部分。

## 开放问题f3：连接方式比较（0 pts）
本开放问题源自Stack Overflow上的[What is the difference between a hash join and a merge join (Oracle RDBMS )?](https://stackoverflow.com/questions/1111707/what-is-the-difference-between-a-hash-join-and-a-merge-join-oracle-rdbms)。

你是否能够构建测试SQL并编写测试脚本，探究不同join方法在不同数据场景下的优劣？
提示：对于有序数据的优化，你可以：
1. 提前完成lab04的索引部分
2. 创建有序表格，修改query optimizer部分并去掉排序合并连接的sort plan。

## 作业评分与提交

### 评分标准

1. 实验报告（10%）：实现思路，优化技巧，实验结果，对框架代码和实验的建议，以及在报告中出现的思考等，请尽量避免直接粘贴代码，建议2-4页。

2. 功能分数（90%）：需要通过`wsdb/test/sql`目录下的SQL语句测试。

    * basic: <u>**顺序**</u>通过`wsdb/test/sql/lab03/basic`下的SQL测试并与`expected`输出比较，无差异获得该小题满分。

    * bonus: <u>**顺序**</u>通过`wsdb/test/sql/lab03/bonus`下的SQL测试。

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

1. 实验报告（提交一份PDF，命名格式：lab3\_学号\_姓名.pdf）：请在报告开头写上相关信息。
   
   | 学号     | 姓名 | 邮箱                      | 完成题目 |
      | -------- | ---- | ------------------------- | -------- |
   | 12345678 | 张三 | zhangsan@smail.nju.edu.cn | t1/t2/f1/f2/f3    |

2. 代码：`wsdb/src`文件夹

*提交示例：请将以上两部分内容打包并命名为lab3\_学号\_姓名.zip（例如lab3_123456_张三.zip）并上传至提交平台，请确保解压后目录树如下：*

```
├── lab3_123456_张三.pdf
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
