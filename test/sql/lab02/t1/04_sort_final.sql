open database db2024;
select * from dbcourse order by id;
select * from dbcourse order by l2_score, id;
select * from dbcourse order by desc l2_score, id;
select * from dbcourse order by age, id;
select * from dbcourse order by desc age, id;
select * from dbcourse order by address, id;
select * from dbcourse order by desc address, id;
select * from dbcourse order by name, address, id;
select * from dbcourse order by desc name, address, id;
select * from dbcourse order by l2_score, address, id;
select * from dbcourse order by desc l2_score, address, id;
select age, name, id from dbcourse order by age, name, id;
select age, name, id from dbcourse order by desc age, name, id;

-- complex query with all limit, order by, where, projection
select age, name, id from dbcourse where age > 20 order by age, name limit 10;
select * from dbcourse where age > 20 order by desc age, name, address limit 10;
select gpa, name, address from dbcourse where age > 20 order by age, name, gpa limit 10;
select age, name, id from dbcourse where age > 20 and id <> age order by desc age, name limit 10;
exit;
