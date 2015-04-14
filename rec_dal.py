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

    def get_records_by_time(self, from_str, to_str):
        '''
            给定from_str和to_str，返回全部记录数
            前闭后开区间
        '''
        sql = 'select user_id, item_id from user_behaviors where behavior_time >= ? and behavior_time < ?'
        self.cursor.execute(sql, (from_str, to_str))
        return self.cursor.fetchall()

    def get_users_by_items(self, hot_items, beh_types):
        '''
            从给定的items和beh_types中筛选出所有的unique users
        '''
        sql = 'select distinct(user_id) from user_behaviors where item_id in %s and behavior_type in %s' % (hot_items, beh_types)
        self.cursor.execute(sql)
        return [r[0] for r in self.cursor.fetchall()]

    def get_records_by_date(self, date_str, beh_type=4):
        '''
            传入指定日期的date_str,eg. "2014-12-18", 以及动作type
            返回对应的购买记录
        '''
        sql = 'select user_id, item_id from user_behaviors where behavior_time like "%s%%" and behavior_type = %s' % (date_str, beh_type)
        self.cursor.execute(sql)
        return self.cursor.fetchall()


