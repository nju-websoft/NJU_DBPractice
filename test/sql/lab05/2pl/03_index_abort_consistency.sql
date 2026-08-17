open database lab05_2pl;

begin;
insert into account values (6, 600, 'frank');
abort;

select id, balance, tag from account where id = 6;
select id, balance, tag from account where tag = 'frank';

insert into account values (6, 660, 'frank2');
select id, balance, tag from account where id = 6;

begin;
update account set id = 7, tag = 'grace' where id = 6;
abort;

select id, balance, tag from account where id = 6;
select id, balance, tag from account where id = 7;
select id, balance, tag from account where tag = 'grace';

begin;
delete from account where id = 2;
abort;

select id, balance, tag from account where id = 2;
