open database db2025;

-- Test DELETE performance and correctness with various index scenarios
-- Indexes available: stock_idx(s_i_id), stock_swid_idx(s_w_id, s_i_id), etc.

-- 1. Test single column equality delete (should use stock_idx)
delete from stock where s_i_id = 150;
select s_i_id, s_quantity from stock where s_i_id = 150;

-- 2. Test range delete using primary index
delete from stock where s_i_id > 250 and s_i_id < 255;
select s_i_id, s_w_id from stock where s_i_id > 249 and s_i_id < 256 order by s_i_id;

-- 3. Test delete by composite index (should use stock_swid_idx)
delete from stock where s_w_id = 5 and s_i_id = 350;
select s_i_id, s_w_id from stock where s_w_id = 5 and s_i_id = 350;

-- 4. Test composite index with range (should use stock_swid_idx)
delete from stock where s_w_id = 10 and s_i_id > 400 and s_i_id < 405;
select s_i_id, s_w_id from stock where s_w_id = 10 and s_i_id > 399 and s_i_id < 406 order by s_i_id;

-- 5. Test delete by quantity index (should use stock_sqty_idx)
delete from stock where s_quantity = 85 and s_i_id > 500 and s_i_id < 510;
select s_i_id, s_quantity from stock where s_quantity = 85 and s_i_id > 499 and s_i_id < 511 order by s_i_id;

-- 6. Test range delete on quantity (should use stock_sqty_idx)
delete from stock where s_quantity > 95 and s_quantity < 98;
select s_i_id, s_quantity from stock where s_quantity > 94 and s_quantity < 99 order by s_i_id;

-- 7. Test delete using order count index
delete from stock where s_order_cnt = 75 and s_i_id > 600 and s_i_id < 610;
select s_order_cnt, s_i_id from stock where s_order_cnt = 75 and s_i_id > 599 and s_i_id < 611 order by s_i_id;

-- 8. Test three-column composite index delete
delete from stock where s_order_cnt = 50 and s_remote_cnt = 25 and s_ytd > 5.0 and s_ytd < 6.0;
select s_order_cnt, s_remote_cnt, s_ytd from stock where s_order_cnt = 50 and s_remote_cnt = 25 and s_ytd > 4.9 and s_ytd < 6.1 order by s_i_id;

-- 9. Test delete by ytd range
delete from stock where s_ytd > 9.0 and s_ytd < 9.5;
select s_ytd, s_i_id from stock where s_ytd > 8.9 and s_ytd < 9.6 order by s_i_id;

-- 10. Test complex multi-condition delete
delete from stock where s_w_id = 15 and s_quantity > 80 and s_order_cnt > 60;
select s_w_id, s_quantity, s_order_cnt from stock where s_w_id = 15 and s_quantity > 79 and s_order_cnt > 59 order by s_i_id;

-- 11. Test boundary value deletes
delete from stock where s_i_id = 0;
delete from stock where s_i_id = 9999;
select s_i_id from stock where s_i_id = 0;
select s_i_id from stock where s_i_id = 9999;

-- 12. Test non-indexed column delete (should use table scan)
delete from stock where s_dist_01 = 'ABC123';
select s_dist_01 from stock where s_dist_01 = 'ABC123' order by s_i_id;

-- 13. Test delete affecting multiple records
delete from stock where s_w_id = 20;
select s_w_id, s_i_id from stock where s_w_id = 20 order by s_i_id;

-- 14. Test overlapping index conditions
delete from stock where s_i_id > 700 and s_i_id < 710 and s_quantity = 50;
select s_i_id, s_quantity from stock where s_i_id > 699 and s_i_id < 711 and s_quantity = 50 order by s_i_id;

-- 15. Test delete with float precision
delete from stock where s_ytd = 7.5;
select s_ytd from stock where s_ytd = 7.5 order by s_i_id;
