#coding=utf8
'''
    使用sqlite3保存数据，这里是与数据库交互的一些方法
'''

import sqlite3

class RECDAL(object):

    def __init__(self, database_name):
        self.conn = sqlite3.connect(database_name)
        self.cursor = self.conn.cursor()

    def get_records_by_uid(self, uid):
        sql = 'select * from user_behaviors where user_id = ? order by behavior_time desc'
        self.cursor.execute(sql, (uid,))
        return self.cursor.fetchall()

    def get_all_uids(self):
        sql = 'select distinct(user_id) from user_behaviors'
        self.cursor.execute(sql)
        return [r[0] for r in self.cursor.fetchall()]
