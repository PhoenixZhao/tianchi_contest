#coding=utf8
'''
    从历史记录里选取用户，获取其全部记录
    按天抽样
'''
import sqlite3
import random

from constant import database_name, sample_data_path
from rec_dal import RECDAL

dal = RECDAL(database_name)

def sampling_users(N=1000):
    '''
        从数据库里随机抽样1000个用户，获取所有历史记录，存入文件
    '''
    uids = dal.get_all_uids()
    print uids
    sample_uids = random.sample(uids, N)
    print sample_uids
    for uid in sample_uids:
        records = dal.get_records_by_uid(uid)
        print records[0]
        records = [[str(t) for t in r] for r in records]
        fw = open(sample_data_path + str(uid), 'w+')
        fw.write('\n'.join([','.join(r) for r in records]))
        fw.close()


if __name__ == '__main__':
    sampling_users()


