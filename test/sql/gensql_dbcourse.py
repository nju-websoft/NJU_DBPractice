import random
import string
import os

sql_path = "/Users/ziqi/Work/course/database/wsdb/test/sql/lab02/t1/01_prepare_table_dbcourse.sql"
# sql_path = "/Users/ziqi/Work/course/database/wsdb/test/sql/lab02/t1/02_seqscan_limit_projection.sql"
table_def = "create table dbcourse (id int, name char(20), age int, address char(50), gpa float, l1_score float, l2_score float);\n"
cached = []

name_list = ["Alice", "Bob", "Cathy", "David", "Eva", "Frank", "Grace", "Helen", "Ivy", "Jack", "Kelly", "Lily", "Mike",
             "Nancy", "Oscar", "Peter", "Queen", "Rose", "Sam", "Tom", "Uma", "Vicky", "Wendy", "Xavier", "Yoyo", "Zoe",
             "Amy", "Ben", "Cindy", "Daisy", "Eric", "Fiona", "Gary", "Hank", "Iris", "Jenny", "Kevin", "Lucy", "Mandy",
             "Nina", "Olivia", "Penny", "Quincy", "Rita", "Sandy", "Tony", "Ursula", "Vivian", "Winnie", "Xander",
             "Yuri", "Zack", "Annie", "Bill", "Coco", "Derek", "Ella", "Felix", "Gina", "Hugo", "Irene", "Jade", "Karl",
             "Luna", "Mars", "Nora", "Owen", "Peggy", "Quentin", "Rex", "Sara", "Tina", "Ulysses", "Vera", "Walter",
             "Xena", "Yvonne", "Zara", "Andy", "Bella", "Chris", "Diana", "Evan", "Fanny", "George", "Holly", "Ian",
             "Jasmine", "Kyle", "Lily", "James", "Wang", "Zhang", "Li", "Zhao", "Wu", "Chen", "Yang", "Huang", "Zhou",
             "Wang", "Zhang", "Li", "Zhao", "Wu", "Chen", "Yang", "Huang", "Zhou", "Wang", "Zhang", "Li", "Zhao", "Wu",
             "Chen", "Yang", "Huang", "Zhou", "Wang", "Zhang", "Li", "Zhao", "Wu", "Chen", "Yang", "Huang", "Zhou",
             "Deng", "Xu", "Guo", "Sun", "Hu", "Shi", "Lu", "He", "Gao", "Song", "Zhu", "Qian", "Hou", "Ma", "Feng",
             "Zeng", "Xie", "Han", "Tang", "Fang", "Cao", "Qin", "Wei", "Xu", "Shen", "Song", "Zheng", "Pan", "Xiao",
             "Cheng", "Cai", "Yuan", "Dai", "Yu", "Jin", "Du", "Tan", "Feng", "Hu", "Gu", "Wei", "Xie", "Zou", "Xue",
             "Yao", "Yuan", "Xiong", "Xu", "Xiao", "Xing", "Xian", "Xia", "Wu", "Wang", "Tang", "Sun", "Su", "Song", ]

address_list = ["Beijing", "Shanghai", "Guangzhou", "Shenzhen", "Hangzhou", "Nanjing", "Wuhan", "Chengdu", "Chongqing",
                "Tianjin", "Xian", "Suzhou", "Wuxi", "Changzhou", "Nantong", "Yangzhou", "Zhenjiang", "Jiangyin",
                "Kunshan", "Taicang", "Changshu", "Yancheng", "Lianyungang", "Huaian", "Suqian", "Xuzhou",
                "Yancheng", "Dongtai", "Danyang", "Yangzhong", "Jurong", "Jintan", "Yinchuan", "Shijiazhuang",
                "Tangshan",
                "Qinhuangdao", "Handan", "Xingtai", "Baoding", "Zhangjiakou", "Chengde", "Cangzhou", "Langfang",
                "Hengshui",
                "Taiyuan", "Datong", "Yangquan", "Changzhi", "Jincheng", "Shuozhou", "Xinzhou", "Linfen", "Yuncheng",
                "Xian",
                "Tongchuan", "Baoji", "Xianyang", "Weinan", "Yanan", "Hanzhong", "Ankang", "Shangluo", "Lanzhou",
                "Jiayuguan",
                "Jinchang", "Baiyin", "Tianshui", "Wuwei", "Zhangye", "Pingliang", "Jiuquan", "Qingyang", "Dingxi",
                "Longnan",
                "Yinchuan", "Wuzhong", "Guyuan", "Zhongwei", "Shizuishan", "Urumqi", "Karamay", "Turpan", "Hami",
                "Changji", "Kunming",
                "Qujing", "Yuxi", "Baoshan", "Zhaotong", "Lijiang", "Puer", "Lincang", "Chuxiong", "Dali", "Dehong",
                "Nujiang", "Diqing"]


def gen_sql(table, num):
    if table == "dbcourse":
        for i in range(num):
            s_id = i + 1
            name = f"'{random.choice(name_list)}'"
            age = random.randint(10, 50)
            address = f"'{random.choice(address_list)}'"
            gpa = random.uniform(0, 5)
            l1_score = random.uniform(40, 100)
            l2_score = random.uniform(40, 100)
            # l1_score, l2_score and address has 5% chance to be null
            if random.random() < 0.05:
                l1_score = " "
            if random.random() < 0.05:
                l2_score = " "
            if random.random() < 0.05:
                address = " "
            cached.append(
                f"insert into dbcourse values ({s_id}, {name}, {age}, {address}, {gpa:.5f}, {l1_score}, {l2_score});\n")
    random.shuffle(cached)
    with open(sql_path, "a") as f_sql:
        for line in cached:
            f_sql.write(line)


def gen_projection_limit(table, num):
    cached.clear()
    # generate projection of different columns, and limit the number of rows
    if table == "dbcourse":
        col_list = ["id", "name", "age", "address", "gpa", "l1_score", "l2_score"]
        for i in range(num):
            k_columns = random.randint(0, len(col_list) - 1)
            columns = random.sample(col_list, k_columns)
            columns = ", ".join(columns)
            if columns == "":
                columns = "*"
            limit = f"limit {random.randint(1, 1200)}"
            # 70% chance has no limit
            if random.random() < 0.7:
                limit = ""
            cached.append(f"select {columns} from dbcourse {limit};\n")
    with open(sql_path, "a") as f_sql:
        for line in cached:
            f_sql.write(line)


if __name__ == "__main__":
    os.remove(sql_path)
    with open(sql_path, "a") as f:
        f.write("open database db2024;\n")
        f.write(table_def)
    gen_sql("dbcourse", 1000)
    # os.remove(sql_path)
    # with open(sql_path, "a") as f:
    #     f.write("open database db2024;\n")
    # gen_projection_limit("dbcourse", 20)
