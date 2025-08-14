# create table item (i_id int, i_im_id int, i_name char(24), i_price float, i_data char(50));
# create table stock (s_i_id int, s_w_id int, s_quantity int, s_dist_01 char(24), s_dist_02 char(24), s_dist_03 char(24), s_dist_04 char(24), s_dist_05 char(24), s_dist_06 char(24), s_dist_07 char(24), s_dist_08 char(24), s_dist_09 char(24), s_dist_10 char(24), s_ytd float, s_order_cnt int, s_remote_cnt int, s_data char(50));

# insert into item values (1, 6539, 'EPjQ', 140.125000, 'HIEtK');
# insert into stock values (3, 1, 35, '0oVK', 'pgGX', 'Z7JN', '6D2o', '77xX', 'kf0z', 'cuwy', 'cVac', 'J5v6', 'jBbI', 0.500000, 0, 0, 'JsfN4');

# generate data to be inserted into tables
import random
import string
import os


def gen_sql(table, num, sql_file):
    cached = []
    if table == "item":
        for i in range(num):
            i_id = i
            i_im_id = random.randint(0, 50)
            i_name = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
            i_price = random.uniform(0, 10)
            i_data = ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))
            # write to /Users/ziqi/Work/dbcomp/db2024-websoft/src/test/sql/6_sort_merge_join.sql
            # float limit to 5 decimal places
            cached.append(f"insert into item values ({i_id}, {i_im_id}, '{i_name}', {i_price:.5f}, '{i_data}');\n")

    elif table == "stock":
        for i in range(num):
            s_i_id = i
            s_w_id = random.randint(0, 1000)
            s_quantity = random.randint(0, 10)
            s_dist_01 = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
            s_dist_02 = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
            s_dist_03 = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
            s_dist_04 = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
            s_dist_05 = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
            s_dist_06 = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
            s_dist_07 = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
            s_dist_08 = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
            s_dist_09 = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
            s_dist_10 = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
            s_ytd = random.uniform(0, 10)
            s_order_cnt = random.randint(0, 100)
            s_remote_cnt = random.randint(0, 100)
            s_data = ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))
            cached.append(
                f"insert into stock values ({s_i_id}, {s_w_id}, {s_quantity}, '{s_dist_01}', '{s_dist_02}', '{s_dist_03}', '{s_dist_04}', '{s_dist_05}', '{s_dist_06}', '{s_dist_07}', '{s_dist_08}', '{s_dist_09}', '{s_dist_10}', {s_ytd}, {s_order_cnt}, {s_remote_cnt}, '{s_data}');\n")

    with open(sql_file, "a") as f:
        random.shuffle(cached)
        for line in cached:
            f.write(line)


if __name__ == "__main__":
    # create tables
    tb_name = "stock"
    sql_file = f"./lab03/t1/01_prepare_table_{tb_name}.sql"
    if os.path.exists(sql_file):
        os.remove(sql_file)
    with open(sql_file, "w") as f:
        f.write("open database db2025;\n")
        # f.write("create table item (i_id int, i_im_id int, i_name char(24), i_price float, i_data char(50));\n")
        if tb_name == "item":
            f.write(
                "create table item (i_id int, i_im_id int, i_name char(8), i_price float, i_data char(10));\n")
        elif tb_name == "stock":
            f.write(
            "create table stock (s_i_id int, s_w_id int, s_quantity int, s_dist_01 char(8), s_dist_02 char(8), s_dist_03 char(8), s_dist_04 char(8), s_dist_05 char(8), s_dist_06 char(8), s_dist_07 char(8), s_dist_08 char(8), s_dist_09 char(8), s_dist_10 char(8), s_ytd float, s_order_cnt int, s_remote_cnt int, s_data char(10));\n")
    # gen_sql("item", 1000)
    gen_sql(tb_name, 100, sql_file)
