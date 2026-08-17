open database lab05_mvcc;

create table account (id int, balance int, tag char(8));
create index account_id_idx on account(id);
create index account_tag_idx on account(tag);

insert into account values (1, 100, 'alice');
insert into account values (2, 200, 'bob');
insert into account values (3, 300, 'carol');

select id, balance, tag from account order by id;
