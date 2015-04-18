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
from constant import LR_log_file, LR_offline_pred_output_path, LR_online_pred_output_path

#sql for offline prediction
off_pos_sql = '''
            select l.user_id, l.item_id, s.looks, s.stores, s.carts, s.buys, s.total, s.l3d_looks, s.l3d_stores, s.l3d_carts, l3d_buys, s.l3d_total, s.lc_date_delta, l.label
            from split_20141217_labels as l join split_20141217_stats as s
                on l.user_id=s.user_id and l.item_id=s.item_id
                where l.label = 1
          '''
off_neg_sql = '''
            select l.user_id, l.item_id, s.looks, s.stores, s.carts, s.buys, s.total, s.l3d_looks, s.l3d_stores, s.l3d_carts, l3d_buys, s.l3d_total, s.lc_date_delta, l.label
            from split_20141217_labels as l join split_20141217_stats as s
                on l.user_id=s.user_id and l.item_id=s.item_id
                where l.label = 0
          '''
off_candidate_sql = '''
            select s.user_id, s.item_id, s.looks, s.stores, s.carts, s.buys, s.total, s.l3d_looks, s.l3d_stores, s.l3d_carts, l3d_buys, s.l3d_total, s.lc_date_delta
            from split_20141218_stats as s;
                '''
off_test_sql = '''
           select user_id, item_id from split_20141218_labels where label = 1;
               '''

#sql for online prediction
on_pos_sql = '''
            select l.user_id, l.item_id, s.looks, s.stores, s.carts, s.buys, s.total, s.l3d_looks, s.l3d_stores, s.l3d_carts, l3d_buys, s.l3d_total, s.lc_date_delta, l.label
            from split_20141218_labels as l join split_20141218_stats as s
                on l.user_id=s.user_id and l.item_id=s.item_id
                where l.label = 1;
          '''
on_neg_sql = '''
            select l.user_id, l.item_id, s.looks, s.stores, s.carts, s.buys, s.total, s.l3d_looks, s.l3d_stores, s.l3d_carts, l3d_buys, s.l3d_total, s.lc_date_delta, l.label
            from split_20141218_labels as l join split_20141218_stats as s
                on l.user_id=s.user_id and l.item_id=s.item_id
                where l.label = 0;
          '''
on_candidate_sql = '''
            select s.user_id, s.item_id, s.looks, s.stores, s.carts, s.buys, s.total, s.l3d_looks, s.l3d_stores, s.l3d_carts, l3d_buys, s.l3d_total, s.lc_date_delta
            from split_20141219_stats as s;
                '''
#在过去一个月的有过购买的暂时先不考虑作为预测集
on_filter_sql = '''
            select distinct user_id, item_id from user_behaviors where behavior_type = 4;
             '''

class Model(object):

    def __init__(self, dal, output_path, sqls, is_online=True):
        self.LR = LogisticRegression()
        self.dal = dal
        self.output_path = output_path
        self.pos_sql = sqls[0]
        self.neg_sql = sqls[1]
        self.candidate_sql = sqls[2]
        if not is_online:
            self.off_test_sql = sqls[3]
        self.is_online = is_online
        logging.info('init %s model...', 'online' if is_online else 'offline')
        logging.info('sql for training data %s', '\n'.join(sqls))
        self.generate_train_data()
        self.generate_test_data()

    def generate_train_data(self):
        '''
            从split_20141218_labels表中选出所有的(user,item, label)，即为训练数据
            这个表中正反例分布非常不均匀，反例太多，需要过滤一部分
            设定一个阈值，过去3天，总活动数小于N(N=2)可以去掉,并且lc_date_delta 小于10的,这样反例有7707
            而正例限制只要lc_date_delta小于10,这样正例有990个
            极度不平衡，先用这个跑一版再说
        '''
        pos_records = self.dal.get_records_by_sql(self.pos_sql)
        neg_records = self.dal.get_records_by_sql(self.neg_sql)
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
        can_records = self.dal.get_records_by_sql(self.candidate_sql)
        #filter_records = set(self.dal.get_records_by_sql(filter_sql))
        filter_records = set()
        f_can_records = []
        for r in can_records:
            user_id, item_id = r[0], r[1]
            if not (user_id, item_id) in filter_records:
                f_can_records.append(r)
        logging.info('can_records=%s,filter_records=%s,f_can_records=%s', len(can_records), len(filter_records), len(f_can_records))

        can_predict_data = np.array(can_records, dtype=float)
        self.pred_data_ind = can_predict_data[:,(0,1)]#user_item 的pair
        r, c = can_predict_data.shape
        feature_indexes = range(2, c)
        self.pred_data_X = can_predict_data[:,feature_indexes]

    def train(self):
        self.LR.fit(self.train_X, self.train_Y)

    def predict(self):
        self.LR.fit(self.train_X, self.train_Y)
        self.pred_Y = self.LR.predict(self.pred_data_X)
        r = self.pred_Y.shape
        self.pred_Y = self.pred_Y.reshape(r[0], 1)
        self.pred_proba = self.LR.predict_proba(self.pred_data_X)[:,-1]
        r = self.pred_proba.shape
        self.pred_proba = self.pred_proba.reshape(r[0], 1)
        pred_res = np.hstack((self.pred_data_ind, self.pred_proba))
        #只需要筛选出res中最后一列为1的即为预测的结果
        th_prob = 0.5
        rec_res = pred_res[pred_res[:,2] > th_prob]
        self.rec_res = [(int(uid), int(item_id)) for uid, item_id, _ in rec_res]
        logging.info('filter by prob=%s, get %s predicted buying items', th_prob, len(rec_res))

        if not self.is_online:
            self.test_res = self.dal.get_records_by_sql(self.off_test_sql)
            self.eval_id = datetime.strftime(datetime.today(), '%Y%m%d%H%M')
            self.evaluate()
            self.output_path += '.%s' % (self.eval_id)

        self.save_res()

    def save_res(self):
        ''' 从pred_labels中选出为1的即为预测的购买记录
        '''
        fw = open(self.output_path, 'w+')
        wlines = ['user_id,item_id']
        for uid, item_id in self.rec_res:
            line = '%s,%s' % (str(uid), str(item_id))
            wlines.append(line)
        fw.write('\n'.join(wlines))
        fw.close()
        logging.info('pred_res saved in %s', self.output_path)

    def evaluate(self):
        correct_num = len([r for r in self.rec_res if r in self.test_res])
        precision = 1.0 * correct_num / len(self.rec_res)
        recall = 1.0 * correct_num / len(self.test_res)
        F = 2.0 / (1.0 / precision + 1.0 / recall)
        logging.info('**********evaluation(ev_id=%s, pred_res=%s,test_res=%s) of offline model(F, P, R): %s, %s, %s*********',\
                                                        self.eval_id , len(self.rec_res), len(self.test_res), F, precision, recall)

if __name__ == '__main__':
    dal = RECDAL()
    init_logger(log_file=LR_log_file, log_level=logging.INFO, print_console=True)

    off_sqls = [off_pos_sql, off_neg_sql, off_candidate_sql, off_test_sql]
    off_model = Model(dal, LR_offline_pred_output_path, off_sqls, is_online=False)
    off_model.train()
    off_model.predict()
    logging.info('************offline model finished*******************')

    on_sqls = [on_pos_sql, on_neg_sql, on_candidate_sql]
    on_model = Model(dal, LR_online_pred_output_path, on_sqls)
    on_model.train()
    on_model.predict()
    logging.info('************online model finished*******************')

