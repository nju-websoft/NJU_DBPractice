open database db2025;

create index stock_idx on stock(s_i_id) using HASH;
create index stock_sdist01_idx on stock(s_dist_01) using HASH;
create index stock_swid_idx on stock(s_w_id, s_i_id) using HASH;
create index stock_sqty_idx on stock(s_quantity, s_i_id) using HASH;
create index stock_ordercnt_idx on stock(s_order_cnt, s_i_id) using HASH;
create index stock_s_rmcnt_ytd_idx on stock(s_remote_cnt, s_ytd) using HASH;
create index stock_s_ytd_rmtcnt_idx on stock(s_ytd, s_remote_cnt) using HASH;
create index stock_ordercnt_rmcnt_ytd_idx on stock(s_order_cnt, s_remote_cnt, s_ytd) using HASH;


select s_i_id, s_w_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt from stock where s_i_id = 9864;
select s_i_id, s_w_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt from stock where s_i_id > 9864 and s_i_id < 9870 order by s_i_id;
select s_i_id, s_w_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt from stock where s_w_id = 5 order by s_i_id;
select s_i_id, s_w_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt from stock where s_w_id = 5 and s_i_id = 9864;
select s_i_id, s_w_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt from stock where s_w_id = 5 and s_i_id > 9864 order by s_i_id;
select s_i_id, s_w_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt from stock where s_quantity = 10 order by s_i_id;
select s_i_id, s_w_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt from stock where s_quantity = 10 and s_i_id = 9864;
select s_i_id, s_w_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt from stock where s_quantity = 10 and s_i_id > 9864 order by s_i_id;
select s_i_id, s_w_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt from stock where s_order_cnt = 5 and s_i_id = 9864;
select s_i_id, s_w_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt from stock where s_order_cnt = 5 and s_i_id > 9864 order by s_i_id;
select s_i_id, s_w_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt from stock where s_remote_cnt = 3 and s_ytd >= 4 and s_ytd <= 6 order by s_ytd;
select s_i_id, s_w_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt from stock where s_order_cnt = 30 and s_remote_cnt = 27 and s_ytd >= 4 and s_ytd <= 6 order by s_order_cnt, s_remote_cnt, s_ytd;
select s_i_id, s_w_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt from stock where s_order_cnt = 30 and s_remote_cnt = 27 and s_ytd >= 4 and s_ytd <= 6 order by s_order_cnt, s_remote_cnt, s_ytd;
select s_i_id, s_w_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt from stock where s_order_cnt > 30 and s_remote_cnt = 27 and s_ytd >= 4 and s_ytd <= 6 order by s_order_cnt, s_remote_cnt, s_ytd;
select s_i_id, s_w_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt from stock where s_order_cnt > 30 and s_remote_cnt > 27 and s_ytd >= 4 and s_ytd <= 6 order by s_order_cnt, s_remote_cnt, s_ytd;
select s_i_id, s_w_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt from stock where s_order_cnt > 30 and s_order_cnt < 50 and s_remote_cnt >= 10 order by s_order_cnt, s_remote_cnt, s_ytd;
