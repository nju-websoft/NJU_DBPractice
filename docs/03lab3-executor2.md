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

