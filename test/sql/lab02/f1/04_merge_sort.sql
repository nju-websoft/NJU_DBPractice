open database db2024;
select i_id, s_i_id, i_name, s_quantity from item, stock order by s_quantity, i_id, s_i_id;
exit;
