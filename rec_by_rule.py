#coding=utf8
'''
    使用rule进行筛选
'''

truth_sql = '''
            select l.user_id, l.item_id from split_20141218_labels as l join split_20141218_stats as s
            on l.user_id=s.user_id and l.item_id=s.item_id where l.label = 1
            '''
vali_sql = 'select user_id, item_id from split_20141218_stats as s where s.y_total > 7 and s.buys = 0 '
vali_sql = 'select user_id, item_id from split_20141218_stats as s where s.total > 20 and s.buys > 3 and s.y_buys=0'
test_sql = 'select user_id, item_id from split_20141219_stats as s where s.total > 20 and s.buys > 3 and s.y_buys=0'

import logging

from rec_dal import RECDAL
dal = RECDAL()

from constant import submission_dir

def validate():
    '''
        选出18号的数据进行验证
    '''
    records = dal.get_records_by_sql(vali_sql)
    truth = dal.get_records_by_sql(truth_sql)
    truth = set(truth)
    print 'rec_num=%s, truth_num=%s' % (len(records), len(truth))
    correct_num = len([r for r in records if r in truth])
    P = 1.0 * correct_num / len(records)
    R = 1.0 * correct_num / len(truth)
    print P, R
    F = 2. * P * R / (P + R)
    print 'F, P, R = ', F, P, R

def test():
    '''
        选出19号的数据进行测试
    '''
    test_records = dal.get_records_by_sql(test_sql)
    filename = submission_dir + 'rec_by_rule.201504232330'
    f = open(filename, 'w+')
    wlines = ['use_id,item_id'] + [','.join([str(i) for i in r]) for r in test_records]
    f.write('\n'.join(wlines))
    f.close()

def main():
    validate()
    #test()

if __name__ == '__main__':
    main()

