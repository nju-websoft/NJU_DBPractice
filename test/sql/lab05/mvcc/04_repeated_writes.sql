open database lab05_mvcc;

begin;
update account set balance = 151 where id = 1;
update account set balance = 152 where id = 1;
commit;

select id, balance, tag from account where id = 1;

begin;
update account set balance = 998 where id = 2;
delete from account where id = 2;
abort;

select id, balance, tag from account where id = 2;

begin;
insert into account values (8, 800, 'henry');
update account set balance = 880, tag = 'henry2' where id = 8;
abort;

select id, balance, tag from account where id = 8;
select id, balance, tag from account where tag = 'henry2';
