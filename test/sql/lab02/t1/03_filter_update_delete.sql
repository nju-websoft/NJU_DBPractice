open database db2024;
select * from dbcourse where l2_score > 90;
select * from dbcourse where l2_score > 90 and l1_score <= 90;
select * from dbcourse where l2_score > 90 and l1_score <= 90 and gpa > 3.5;
select * from dbcourse where l2_score > 90 and l1_score <= 90 and gpa > 3.5 and age=45;
select * from dbcourse where l2_score > 90 and l1_score <= 90 and gpa > 3.5 and age<>45;
select * from dbcourse where l2_score > 90 and l1_score <= 90 and gpa > 3.5 and address='Nanjing';
select * from dbcourse where l2_score > 90 and address='Nanjing';
select * from dbcourse where l1_score = l2_score;
select * from dbcourse where l1_score <> l2_score;
select * from dbcourse where l1_score > l2_score;
select * from dbcourse where id=age;
select * from dbcourse where id=age and address='Cangzhou';
--- update
select * from dbcourse where  address='Nanjing' and gpa < 2.0;
update dbcourse set l2_score = 100, l1_score=100 where address='Nanjing' and gpa < 2.0;
select * from dbcourse where  address='Nanjing' and gpa < 2.0;
select * from dbcourse where  address='Nanjing';
select * from dbcourse where address='Beijing';
update dbcourse set l2_score = 100, l1_score=100, address='Beijing', name='Unknown' where address='Nanjing' and gpa < 2.0;
select * from dbcourse where address='Beijing';
select * from dbcourse where name='Unknown';
update dbcourse set l2_score = 100, l1_score=100, address='Beijing', name='Unknown', id=-1 where name='Unknown';
--- delete
select * from dbcourse where  address='Beijing';
delete from dbcourse where address='Beijing' and name='Unknown';
select * from dbcourse where  address='Beijing';
select * from dbcourse where  address='Nanjing';
delete from dbcourse where address='Nanjing';
select * from dbcourse where  address='Nanjing';
select * from dbcourse where  address='Beijing';
delete from dbcourse where address='Beijing';
select * from dbcourse where  address='Beijing';
select * from dbcourse;
exit;
