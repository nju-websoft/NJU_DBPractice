open database db2024;
select i_id, s_i_id, i_name, s_quantity  from item inner join stock where i_id = s_i_id;
select i_id, s_i_id, i_name, s_quantity from stock, item where s_i_id > i_id and i_id < 100;
exit;