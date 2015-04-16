#coding=utf8
'''
    创建数据库，并将数据导入到数据库中
'''

import sys
import sqlite3

CREATE_TABLE_SQLS = ['''CREATE TABLE if not exists user_behaviors(
                        id INTEGER PRIMARY KEY ASC autoincrement,
                        user_id INTEGER NOT NULL,
                        item_id INTEGER NOT NULL,
                        behavior_type INTEGER,
                        user_geo CHAR(50) DEFAULT "",
                        item_category INTEGER,
                        behavior_time TEXT DEFAULT "",
                        behavior_weekday INTEGER -- 记录用户行为的weekday信息，作为特征的一种
                        );
                     ''',
                     '''
                         CREATE TABLE if not exists items(
                         id INTEGER PRIMARY KEY ASC autoincrement,
                         item_id INTEGER NOT NULL,
                         user_geo CHAR(50) DEFAULT "",
                         item_category INTEGER
                         );
                     ''',
                     ]
CREATE_INDEX_SQLS = [
                  #'CREATE INDEX uid on user_behaviors (user_id);',
                  #'CREATE INDEX iid on user_behaviors (item_id);',
                  #'CREATE INDEX cat on user_behaviors (item_category);',
                  #'CREATE INDEX bti on user_behaviors (behavior_time);',
                  'CREATE INDEX uii on user_behaviors (user_id, item_id);',
                  #'CREATE INDEX bt on user_behaviors (behavior_type);',
                  #'CREATE INDEX bw on user_behaviors (behavior_weekday);',
                  #'CREATE INDEX iid2 on items (item_id);',
                  #'CREATE INDEX cat2 on user_behaviors (item_category);',
                ]


from constant import database_name, pre_users_his_path, items_info_path

def create_table():
    conn = sqlite3.connect(database_name)
    cur = conn.cursor()
    for sql in CREATE_TABLE_SQLS:
        cur.execute(sql)
        conn.commit()
    print 'creating table finished!'

def add_indexes():
    '''
        给数据库增加索引
    '''
    conn = sqlite3.connect(database_name)
    cur = conn.cursor()
    for sql in CREATE_INDEX_SQLS:
        cur.execute(sql)
        conn.commit()
        print 'add index, sql=%s' % sql
    print 'creating indexes finished!'

def load_users_data():
    '''
        从预处理后的文件中文件中导入训练数据，
        用户点击中增加column weekday，作为一个特征进行分析
    '''
    lines = open(pre_users_his_path).readlines()
    conn = sqlite3.connect(database_name)
    cur = conn.cursor()
    sql = '''
            INSERT INTO user_behaviors (user_id, item_id, behavior_type, user_geo, item_category, behavior_time, behavior_weekday) values (?, ?, ?, ?, ?, ?, ?);
          '''
    values = [l.strip().split(',') for l in lines]
    cur.executemany(sql, values)
    conn.commit()
    print 'insert %s user records' % len(lines)

def load_items_data():
    '''
        直接将csv中的数据导入数据库
    '''
    lines = open(items_info_path).readlines()
    conn = sqlite3.connect(database_name)
    cur = conn.cursor()
    sql = '''
            INSERT INTO items (item_id, user_geo, item_category) values (?, ?, ?);
          '''
    values = [l.strip().split(',') for l in lines]
    cur.executemany(sql, values)
    conn.commit()
    print 'insert %s items records' % len(lines)


def main():
    if len(sys.argv) < 2:
        print 'please specify your loading type\n0: all \n1: create table only) \n2: load users data \n3: add indexes\n4load items data'
        sys.exit(0)
    else:
        type_ = int(sys.argv[1])
        if type_ == 1:
            create_table()
        elif type_ == 2:
            load_users_data()
        elif type_ == 3:
            add_indexes()
        elif type_ == 3:
            load_items_data()
        elif type_ == 0:
            create_table()
            load_users_data()
            load_items_data()


if __name__ == '__main__':
    main()

