#coding=utf8
'''
    给定split date，也就是预测的日期，生成训练集
    1,12-18，根据12-18的记录，生成训练集;
    2,实际online预测没有，所以需要选出一堆预测集
    存入数据库中，表名为split_20141218_labels
'''

from datetime import datetime, timedelta

from rec_dal import RECDAL
from constant import POS, NEG
dal = RECDAL()

def save_samples(samples, table_name):
    samples = [(ind + 1, user_id, item_id, label) for ind, (user_id, item_id, label) in enumerate(samples)]
    dal.insert_records(table_name, samples)
    print 'insert %s records' %(len(samples))

def generate_train_data(split_date_str, table_name):
    '''
    '''
    print 'start generating training data in %s' % split_date_str
    from_str = split_date_str
    split_date = datetime.strptime(split_date_str, '%Y-%m-%d')
    to_date = split_date + timedelta(days=1)
    to_str = datetime.strftime(to_date, '%Y-%m-%d')
    columns = ('user_id', 'item_id', 'behavior_type')
    records = dal.get_records_by_time(to_str=to_str, from_str=from_str, columns=columns)
    pos_samples = [str(uid) + '-' + str(item_id) for uid, item_id, b_type in records if b_type == 4]
    pos_samples = set(pos_samples)
    neg_samples = set()
    for uid, item_id, b_type in records:
        ui_id = str(uid) + '-' + str(item_id)
        if ui_id not in pos_samples:
            neg_samples.add(ui_id)
    samples = []
    samples = [(r, POS) for r in pos_samples] + [(r, NEG) for r in neg_samples]
    samples2 = []
    for r, label in samples:
        user_id, item_id = r.split('-')
        samples2.append((int(user_id), int(item_id), label))
    print 'samples: pos=%s, neg=%s' % (len(pos_samples), len(neg_samples))
    save_samples(samples2, table_name)

if __name__ == '__main__':
    split_date_str = '2014-12-17'
    table_name = 'split_20141217_labels'
    generate_train_data(split_date_str, table_name)





