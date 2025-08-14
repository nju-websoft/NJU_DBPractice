open database db2025;

-- Test UPDATE performance and correctness with various index scenarios
-- Test both index usage for WHERE clause and index maintenance after updates
-- Note: Updates run after deletes, so some records may be missing

-- 1. Test single column equality update (should use stock_idx). sql_id : 1-2
update stock set s_quantity = 99 where s_i_id = 1500;
select s_i_id, s_quantity from stock where s_i_id = 1500;

-- 2. Test range update using primary index. sql_id : 3-4
update stock set s_remote_cnt = 99 where s_i_id > 1600 and s_i_id < 1605;
select s_i_id, s_w_id from stock where s_i_id > 1599 and s_i_id < 1606;

-- 3. Test updating indexed column (should trigger index maintenance). sql_id : 5-8
delete from stock where s_i_id = 1700;
update stock set s_i_id = 8000 where s_i_id = 1700;
select s_i_id from stock where s_i_id = 1700;
select s_i_id from stock where s_i_id = 8000;

-- 4. Test composite index usage in WHERE clause. sql_id : 9-10
update stock set s_dist_01 = 'UPD001' where s_w_id = 50 and s_i_id = 1800;
select s_i_id, s_w_id, s_dist_01 from stock where s_w_id = 50 and s_i_id = 1800;

-- 5. Test updating first column of composite index. sql_id : 11-13
update stock set s_w_id = 88 where s_w_id = 60 and s_i_id > 1900 and s_i_id < 1905;
select s_i_id, s_w_id from stock where s_w_id = 88;
select s_i_id, s_w_id from stock where s_w_id = 60 and s_i_id > 1899 and s_i_id < 1906;

-- 6. Test updating second column of composite index. sql_id : 14-16
update stock set s_i_id = 8001 where s_w_id = 70 and s_i_id = 2000;
select s_i_id, s_w_id from stock where s_i_id = 2000;
select s_i_id, s_w_id from stock where s_i_id = 8001;

-- 7. Test multiple column updates including indexed columns. sql_id : 17-18
update stock set s_quantity = 88, s_w_id = 77 where s_i_id = 2100;
select s_i_id, s_w_id, s_quantity from stock where s_i_id = 2100;

-- 8. Test range update on composite index. sql_id : 19-20
update stock set s_dist_02 = 'RNG001' where s_quantity = 10 and s_i_id > 2200 and s_i_id < 2210;
select s_i_id, s_quantity, s_dist_02 from stock where s_quantity = 10 and s_i_id > 2199 and s_i_id < 2211;

-- 9. Test three-column composite index usage. sql_id : 21-22
update stock set s_dist_03 = 'TRP001' where s_order_cnt = 75 and s_remote_cnt = 35 and s_ytd > 5.0 and s_ytd < 6.0;
select s_order_cnt, s_remote_cnt, s_ytd, s_dist_03 from stock where s_order_cnt = 75 and s_remote_cnt = 35 and s_ytd > 4.9 and s_ytd < 6.1;

-- 10. Test updating all columns in three-column composite index. sql_id : 23-24
update stock set s_order_cnt = 99, s_remote_cnt = 99, s_ytd = 9.99 where s_i_id = 2300;
select s_i_id, s_order_cnt, s_remote_cnt, s_ytd from stock where s_i_id = 2300;

-- 11. Test float column range update. sql_id : 25-26
update stock set s_dist_04 = 'FLT001' where s_ytd > 1.0 and s_ytd < 2.0;
select s_ytd, s_dist_04 from stock where s_ytd > 0.9 and s_ytd < 2.1;

-- 12. Test updating with complex WHERE clause. sql_id : 27-28
update stock set s_dist_05 = 'CMP001' where s_remote_cnt = 45 and s_ytd > 3.0 and s_order_cnt > 20 and s_order_cnt < 40;
select s_remote_cnt, s_ytd, s_order_cnt, s_dist_05 from stock where s_remote_cnt = 45 and s_ytd > 2.9 and s_order_cnt > 19 and s_order_cnt < 41;

-- 13. Test boundary value updates
update stock set s_quantity = 0 where s_i_id = 2400;
update stock set s_quantity = 100 where s_i_id = 2401;
select s_i_id, s_quantity from stock where s_i_id = 2400;
select s_i_id, s_quantity from stock where s_i_id = 2401;

-- 14. Test string column updates with indexed lookup
update stock set s_dist_06 = 'NEW001' where s_w_id = 80 and s_i_id > 2500 and s_i_id < 2510;
select s_i_id, s_w_id, s_dist_06 from stock where s_w_id = 80 and s_i_id > 2499 and s_i_id < 2511;

-- 15. Test updates that affect multiple indexes
update stock set s_w_id = 66, s_quantity = 77, s_order_cnt = 88 where s_i_id = 2600;
select s_i_id, s_w_id, s_quantity, s_order_cnt from stock where s_i_id = 2600;
select s_i_id, s_w_id, s_quantity, s_order_cnt from stock where s_i_id = 8002;

-- 16. Test non-indexed column in WHERE clause (should use table scan)
update stock set s_quantity = 55 where s_dist_07 = 'SCN001';
select s_quantity, s_dist_07 from stock where s_dist_07 = 'SCN001';

-- 17. Test potential index key conflicts, 2700 should throw and 2701 does not exist.
select s_i_id, s_w_id, s_quantity, s_order_cnt from stock;
select s_i_id, s_w_id, s_quantity, s_order_cnt from stock order by s_i_id;
select s_i_id, s_w_id, s_quantity, s_ytd, s_order_cnt from stock order by s_ytd;
select s_i_id, s_w_id, s_quantity, s_order_cnt from stock order by s_quantity;
select s_i_id, s_w_id, s_quantity, s_order_cnt from stock order by s_order_cnt;
select s_i_id, s_w_id, s_quantity, s_order_cnt from stock where s_i_id = 2700;
select s_i_id, s_w_id, s_quantity, s_order_cnt from stock where s_i_id = 8003;
select s_i_id, s_w_id, s_quantity, s_order_cnt from stock where s_i_id = 8004;
select s_i_id, s_w_id, s_quantity, s_order_cnt from stock where s_i_id = 2701;
update stock set s_i_id = 8003 where s_i_id = 2700;
update stock set s_i_id = 8004 where s_i_id = 2701;
select s_i_id, s_w_id, s_quantity, s_order_cnt from stock where s_i_id = 8003;
select s_i_id, s_w_id, s_quantity, s_order_cnt from stock where s_i_id = 8004;
select s_i_id, s_w_id, s_quantity, s_order_cnt from stock where s_i_id = 2701;

-- 18. Test updates with conditions that might not use optimal index
update stock set s_dist_08 = 'SUB001' where s_i_id > 2800 and s_w_id = 90;
select s_i_id, s_w_id, s_dist_08 from stock where s_i_id > 2799 and s_w_id = 90;

-- 19. Test large range update
update stock set s_dist_09 = 'LRG001' where s_i_id > 2900 and s_i_id < 2950;
select s_i_id, s_dist_09 from stock where s_i_id > 2899 and s_i_id < 2951;

-- 20. Test updating with overlapping index conditions
update stock set s_dist_10 = 'OVR001' where s_quantity > 50 and s_quantity < 70 and s_i_id > 3000 and s_i_id < 3100;
select s_i_id, s_quantity, s_dist_10 from stock where s_quantity > 49 and s_quantity < 71 and s_i_id > 2999 and s_i_id < 3101; 