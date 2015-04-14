#!/usr/bin/python
#coding=utf8
'''
    输入日期，将改天的数据load出，用户购买的作为测试集
'''
from datetime import date

from constant import database_name, test_dir

from rec_dal import RECDAL
dal = RECDAL(database_name)

def generate_test_data_by_date(date_str):
    '''
        输入指定的日期，返回对应天的全部购买记录作为测试集
    '''
    print 'generate test data for %s' % date_str
    beh_type = 4
    user_items = dal.get_records_by_date(date_str, beh_type)
    filename = test_dir + date_str
    fw = open(filename, 'w+')
    wlines = ['user_id,item_id']
    for uid, iid in user_items:
        wlines.append('%s,%s' % (str(uid), str(iid)))
    fw.write('\n'.join(wlines))
    fw.close()
    print 'test data saved in %s' % filename

if __name__ == '__main__':
    date_str = '2014-12-18'
    generate_test_data_by_date('2014-12-18')
