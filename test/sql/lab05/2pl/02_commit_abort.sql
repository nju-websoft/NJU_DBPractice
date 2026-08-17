open database lab05_2pl;

begin;
update account set balance = 150 where id = 1;
insert into account values (4, 400, 'dave');
commit;

select id, balance, tag from account order by id;

begin;
update account set balance = 999 where id = 1;
delete from account where id = 2;
insert into account values (5, 500, 'erin');
abort;

select id, balance, tag from account order by id;
