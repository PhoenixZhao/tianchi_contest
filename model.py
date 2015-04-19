#!/usr/bin/python
#coding=utf8
'''
    训练model，然后进行预测
    目前使用方法:
        1,logistic regression

    使用18号的数据作为验证集，每次跑实验，将验证集的结果输出，并自动计算F score，然后跑出online预测
'''
import time
import logging
from datetime import datetime

import numpy as np
from sklearn.linear_model import LogisticRegression

from logging_util import init_logger

from rec_dal import RECDAL
from constant import LR_log_file, vali_output_path, test_output_path

#sql for offline prediction
train_pos_sql = '''
            select l.user_id, l.item_id, s.looks, s.stores, s.carts, s.buys, s.total, s.l3d_looks, s.l3d_stores, s.l3d_carts, l3d_buys, s.l3d_total, s.lc_date_delta, l.label
            from split_20141217_labels as l join split_20141217_stats as s
                on l.user_id=s.user_id and l.item_id=s.item_id
                where l.label = 1
         '''
train_neg_sql = '''
            select l.user_id, l.item_id, s.looks, s.stores, s.carts, s.buys, s.total, s.l3d_looks, s.l3d_stores, s.l3d_carts, l3d_buys, s.l3d_total, s.lc_date_delta, l.label
            from split_20141217_labels as l join split_20141217_stats as s
                on l.user_id=s.user_id and l.item_id=s.item_id
                where l.label = 0
          '''

filter_sql = ' where s.lc_date_delta < 10'
filter_sql = ' where s.buys = 0'
#验证集的待预测候选集
validation_candidate_sql = '''
            select s.user_id, s.item_id, s.looks, s.stores, s.carts, s.buys, s.total, s.l3d_looks, s.l3d_stores, s.l3d_carts, l3d_buys, s.l3d_total, s.lc_date_delta
            from split_20141218_stats as s'''
validation_candidate_sql += filter_sql
#ground truth
validation_truth_sql = '''
           select user_id, item_id from split_20141218_labels where label = 1;
               '''
test_candidate_sql = '''
            select s.user_id, s.item_id, s.looks, s.stores, s.carts, s.buys, s.total, s.l3d_looks, s.l3d_stores, s.l3d_carts, l3d_buys, s.l3d_total, s.lc_date_delta
            from split_20141219_stats as s'''
test_candidate_sql += filter_sql
#在过去一个月的有过购买的暂时先不考虑作为预测集
on_filter_sql = '''
            select distinct user_id, item_id from user_behaviors where behavior_type = 4;
             '''
exp_sqls = [train_pos_sql, train_neg_sql, validation_candidate_sql, validation_truth_sql, test_candidate_sql]
exp_sqls_info = ['train_pos_sql', 'train_neg_sql', 'validation_candidate_sql', 'validation_truth_sql', 'test_candidate_sql']

class Model(object):

    def __init__(self, dal, vali_output_path, test_output_path):
        self.LR = LogisticRegression()
        self.dal = dal
        self.exp_id = datetime.strftime(datetime.today(), '%Y%m%d%H%M')
        self.vali_output_path = '%s.%s' % (vali_output_path, self.exp_id)
        self.test_output_path = '%s.%s' % (test_output_path, self.exp_id)
        logging.info('init LR model..., experiment sqls:\n%s', '\n'.join([':'.join(r) for r in zip(exp_sqls_info, exp_sqls)]))
        self.generate_train_data()
        self.generate_validation_data()
        self.generate_test_data()

    def generate_train_data(self):
        '''
            从split_20141217_labels表中选出所有的(user,item, label)，即为训练数据
            这个表中正反例分布非常不均匀，反例太多，需要过滤一部分
            设定一个阈值，过去3天，总活动数小于N(N=2)可以去掉,并且lc_date_delta 小于10的,这样反例有7707
            而正例限制只要lc_date_delta小于10,这样正例有990个
            极度不平衡，先用这个跑一版再说
        '''
        pos_records = self.dal.get_records_by_sql(train_pos_sql)
        neg_records = self.dal.get_records_by_sql(train_neg_sql)
        records = pos_records + neg_records
        logging.info('train_data: pos=%s,neg=%s',len(pos_records), len(neg_records))
        train_data = np.array(records, dtype=float)
        r, c = train_data.shape
        feature_indexes = range(2, c-1)
        self.train_X = train_data[:,feature_indexes]#从index为2开始为特征，到倒数第二列
        self.train_Y = train_data[:,-1]#最后一列为label

    def generate_test_data(self):
        '''
            从数据库选出待预测的user_item对，进行预测
        '''
        can_records = self.dal.get_records_by_sql(test_candidate_sql)
        logging.info('get %s candidate test records', len(can_records))

        can_predict_data = np.array(can_records, dtype=float)
        self.test_data_ind = can_predict_data[:,(0,1)]#user_item 的pair
        r, c = can_predict_data.shape
        feature_indexes = range(2, c)
        self.test_X = can_predict_data[:,feature_indexes]

    def generate_validation_data(self):
        '''
            从数据库选出待预测的user_item对，进行预测
        '''
        can_records = self.dal.get_records_by_sql(validation_candidate_sql)
        can_predict_data = np.array(can_records, dtype=float)
        self.vali_data_ind = can_predict_data[:,(0,1)]#user_item 的pair
        r, c = can_predict_data.shape
        feature_indexes = range(2, c)
        self.vali_X = can_predict_data[:,feature_indexes]

        self.vali_truth= self.dal.get_records_by_sql(validation_truth_sql)
        logging.info('candidate_validation_records=%s, validation_truth=%s', len(can_records), len(self.vali_truth))

    def validate(self):

        vali_Y = self.LR.predict(self.vali_X)
        r = vali_Y.shape
        self.vali_Y = vali_Y.reshape(r[0], 1)
        self.vali_proba = self.LR.predict_proba(self.vali_X)[:,-1]
        r = self.vali_proba.shape
        self.vali_proba = self.vali_proba.reshape(r[0], 1)
        vali_res = np.hstack((self.vali_data_ind, self.vali_proba))
        #只需要筛选出res中最后一列为1的即为预测的结果
        self.th_prob = 0.5
        rec_res = vali_res[vali_res[:,2] > self.th_prob]
        self.vali_rec_res = [(int(uid), int(item_id)) for uid, item_id, _ in rec_res]
        logging.info('[Validation]filter by prob=%s, get %s predicted buying items', self.th_prob, len(self.vali_rec_res))

        self.evaluate()

        self.save_res(self.vali_output_path, self.vali_rec_res)

    def train(self):
        self.LR.fit(self.train_X, self.train_Y)

    def predict(self):
        self.test_Y = self.LR.predict(self.test_X)
        r = self.test_Y.shape
        self.test_Y = self.test_Y.reshape(r[0], 1)
        self.test_proba = self.LR.predict_proba(self.test_X)[:,-1]
        r = self.test_proba.shape
        self.test_proba = self.test_proba.reshape(r[0], 1)
        test_res = np.hstack((self.test_data_ind, self.test_proba))
        #只需要筛选出res中最后一列为1的即为预测的结果
        rec_res = test_res[test_res[:,2] > self.th_prob]
        self.test_rec_res = [(int(uid), int(item_id)) for uid, item_id, _ in rec_res]
        logging.info('[TEST]filter by prob=%s, get %s predicted buying items', self.th_prob, len(self.test_rec_res))


        self.save_res(self.test_output_path, self.test_rec_res)

    def save_res(self, filename, rec_res):
        ''' 从pred_labels中选出为1的即为预测的购买记录
        '''
        fw = open(filename, 'w+')
        wlines = ['user_id,item_id']
        for uid, item_id in rec_res:
            line = '%s,%s' % (str(uid), str(item_id))
            wlines.append(line)
        fw.write('\n'.join(wlines))
        fw.close()
        logging.info('pred_res saved in %s', filename)

    def evaluate(self):
        correct_num = len([r for r in self.vali_rec_res if r in self.vali_truth])
        precision = 1.0 * correct_num / len(self.vali_rec_res)
        recall = 1.0 * correct_num / len(self.vali_truth)
        F = 2.0 / (1.0 / precision + 1.0 / recall)
        logging.info('**********evaluation(ev_id=%s, vali_rec_res=%s,truth=%s) of offline model(F, P, R): %s, %s, %s*********',\
                                                        self.exp_id , len(self.vali_rec_res), len(self.vali_truth), F, precision, recall)

if __name__ == '__main__':
    dal = RECDAL()
    init_logger(log_file=LR_log_file, log_level=logging.INFO, print_console=True)

    model = Model(dal, vali_output_path, test_output_path)
    model.train()
    model.validate()
    logging.info('************model validation finished*******************')
    model.predict()

