open database db2025;

-- 1. Cartesian product of stock and item tables.
select s_i_id, stock.s_w_id, i_id, item.i_im_id from stock, item;

-- 2. Natural join of stock and item tables on s_i_id = i_id.
select s_i_id, stock.s_w_id, i_id, item.i_im_id from item, stock where stock.s_i_id = item.i_id;
select s_i_id, stock.s_w_id, i_id, item.i_im_id from stock join item where stock.s_i_id = item.i_id;

-- 3. Join with additional filter conditions.

-- 4. Join with more complex conditions.
select s_i_id, stock.s_w_id, i_id, item.i_im_id from stock, item where stock.s_i_id > item.i_id order by s_i_id, i_id;
select s_i_id, stock.s_w_id, i_id, item.i_im_id from stock, item where stock.s_i_id > item.i_id and s_order_cnt > 10 and s_i_id >= 50 and s_i_id < 90;
select s_i_id, stock.s_w_id, i_id, item.i_im_id, s_dist_01, i_name from stock, item where stock.s_i_id > item.i_id and s_dist_01 < i_name order by s_i_id, i_id;
select s_i_id, stock.s_w_id, i_id, item.i_im_id, s_dist_01, i_name from stock, item where stock.s_i_id > item.i_id and s_dist_01 < i_name and s_order_cnt > 10 and s_i_id >= 50 and s_i_id < 90;

-- 5. Outer join.
select s_i_id, stock.s_w_id, i_id, item.i_im_id from stock outer join item where stock.s_i_id = item.i_id order by s_i_id;
select s_i_id, stock.s_w_id, i_id, item.i_im_id from stock outer join item where stock.s_i_id >= item.i_id order by s_i_id, i_id;
select s_i_id, item.i_im_id, stock.s_w_id, i_id from stock outer join item where stock.s_i_id = item.i_im_id order by s_i_id;
select s_i_id, item.i_im_id, stock.s_w_id, i_id from stock outer join item where stock.s_i_id >= item.i_im_id order by s_i_id, i_im_id;