#coding=utf8
'''
    一些util工具
'''

from datetime import datetime

def date2str(date_):
    '''
        转化成和数据库存储的一致的str，即2014-12-18 8
    '''
    return datetime.strftime(date_, "%Y-%m-%d %H")

def save_standard_results(user_items, filename):
    '''
        根据预测生成的user-items列表构建最终的提交文档
    '''
    fw = open(filename, 'w+')
    wlines = ['user_id,item_id']
    for uid, iid in user_items:
        wlines.append('%s,%s' % (uid, iid))
    fw.write('\n'.join(wlines))
    fw.close()
    print '%s records saved in %s' % (len(wlines) - 1, filename)

