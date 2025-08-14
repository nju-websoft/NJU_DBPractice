open database db2025;

-- 1. Aggregate functions: COUNT, SUM, AVG, MIN, MAX.
select count(*) from stock;
select count(s_i_id) from stock;
select count(s_i_id) from stock where s_w_id = 751;
select count(s_i_id) from stock where s_w_id >=10 and s_quantity < 50;
select count(s_i_id) from stock where s_w_id >=500 and s_quantity < 25;
select sum(s_quantity) from stock;
select avg(s_quantity) from stock;
select min(s_quantity) from stock;
select max(s_quantity) from stock;
select count(i_im_id) from stock outer join item where stock.s_i_id = item.i_im_id order by s_i_id;
select sum(i_im_id) from stock outer join item where stock.s_i_id = item.i_im_id order by s_i_id;
select avg(i_im_id) from stock outer join item where stock.s_i_id = item.i_im_id order by s_i_id;
select min(i_im_id) from stock outer join item where stock.s_i_id = item.i_im_id order by s_i_id;
select max(i_im_id) from stock outer join item where stock.s_i_id = item.i_im_id order by s_i_id;

-- 2. Grouping results with GROUP BY.
select s_quantity, count(*) from stock group by s_quantity order by s_quantity;
select s_quantity, sum(s_remote_cnt) from stock group by s_quantity order by s_quantity;
select s_quantity, avg(s_remote_cnt) from stock group by s_quantity order by s_quantity;

-- 3. test having clause with aggregate functions.
select s_quantity, count(*) from stock group by s_quantity having count(*) > 10 order by s_quantity;
select s_quantity, i_im_id, count(*) from (select * from stock join item where s_i_id=i_im_id) group by s_quantity, i_im_id having count(*)>=3 order by s_quantity, i_im_id;
select s_quantity, sum(s_remote_cnt) from stock group by s_quantity having sum(s_remote_cnt) > 100 order by s_quantity;
select s_quantity, avg(s_remote_cnt), sum(s_order_cnt) from stock group by s_quantity having sum(s_order_cnt) > 200 order by s_quantity;
select s_quantity, avg(s_remote_cnt) as AVG_remote, avg(s_order_cnt), max(s_order_cnt), min(s_ytd), count(*) from stock where s_ytd > 5 group by s_quantity having sum(s_order_cnt) > 200 order by s_quantity;
select s_quantity, count(i_im_id), max(i_price), min(i_price), sum(i_price), count(i_price), count(*) from (select * from stock outer join item where s_i_id=i_im_id) where s_ytd > 3 group by s_quantity having s_quantity > 3 and avg(s_ytd) >= 6.93 order by s_quantity;