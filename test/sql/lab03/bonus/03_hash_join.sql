open database db2025;

-- 1. Natural join of stock and item tables on s_i_id = i_id.
select s_i_id, stock.s_w_id, i_id, item.i_im_id from item, stock where stock.s_i_id = item.i_id order by s_i_id;
select s_i_id, stock.s_w_id, i_id, item.i_im_id from item, stock where stock.s_i_id = item.i_id using HASH order by s_i_id;

-- 2. Join with additional filter conditions.
select s_i_id, i_id, s_ytd, i_price from stock, item where stock.s_i_id = item.i_id and s_ytd > i_price order by s_i_id;
select s_i_id, i_id, s_ytd, i_price from stock, item where stock.s_i_id = item.i_id and s_ytd > i_price using HASH order by s_i_id;

-- 3. Outer join.
select s_i_id, stock.s_w_id, i_id, item.i_im_id from stock outer join item where stock.s_i_id = item.i_id using HASH order by s_i_id;
select s_i_id, item.i_im_id, stock.s_w_id, i_id from stock outer join item where stock.s_i_id = item.i_im_id using HASH order by s_i_id;