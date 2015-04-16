#!/usr/bin/python
#coding=utf8
'''
    训练model，然后进行预测
    目前使用方法:
        1,logistic regression
'''
import numpy as np
from sklearn.linear_model import LogisticRegression

from rec_dal import RECDAL
from constant import LR_pred_output_path

pos_sql = '''
            select l.user_id, l.item_id, s.looks, s.stores, s.carts, s.buys, s.total, s.l3d_looks, s.l3d_stores, s.l3d_carts, l3d_buys, s.l3d_total, s.lc_date_delta, l.label
            from split_20141218_labels as l join split_20141218_stats as s
                on l.user_id=s.user_id and l.item_id=s.item_id
                where l.label = 1 and s.lc_date_delta < 10;
          '''

neg_sql = '''
            select l.user_id, l.item_id, s.looks, s.stores, s.carts, s.buys, s.total, s.l3d_looks, s.l3d_stores, s.l3d_carts, l3d_buys, s.l3d_total, s.lc_date_delta, l.label
            from split_20141218_labels as l join split_20141218_stats as s
                on l.user_id=s.user_id and l.item_id=s.item_id
                where l.label = 0 and s.lc_date_delta < 10 and s.l3d_total > 2;
          '''
candidate_sql = '''
            select s.user_id, s.item_id, s.looks, s.stores, s.carts, s.buys, s.total, s.l3d_looks, s.l3d_stores, s.l3d_carts, l3d_buys, s.l3d_total, s.lc_date_delta
            from split_20141219_stats as s where s.lc_date_delta < 10;
                '''

class Model(object):

    def __init__(self, dal, output_path):
        self.LR = LogisticRegression()
        self.dal = dal
        self.output_path = output_path
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
        pos_records = self.dal.get_records_by_sql(pos_sql)
        neg_records = self.dal.get_records_by_sql(neg_sql)
        records = pos_records + neg_records
        print 'train_data: pos=%s,neg=%s' % (len(pos_records), len(neg_records))
        train_data = np.array(records, dtype=float)
        r, c = train_data.shape
        feature_indexes = range(2, c-1)
        self.train_X = train_data[:,feature_indexes]#从index为2开始为特征，到倒数第二列
        self.train_Y = train_data[:,-1]#最后一列为label

    def generate_test_data(self):
        '''
            从数据库选出待预测的user_item对，进行预测
        '''
        can_records = self.dal.get_records_by_sql(candidate_sql)
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
        self.save_res()

    def save_res(self):
        '''
            从pred_labels中选出为1的即为预测的购买记录
        '''
        print self.pred_data_ind.shape, self.pred_proba.shape
        pred_res = np.hstack((self.pred_data_ind, self.pred_proba))
        #只需要筛选出res中最后一列为1的即为预测的结果
        rec_res = pred_res[pred_res[:,2] < 0.1]
        print rec_res[0], len(rec_res)
        fw = open(self.output_path, 'w+')
        wlines = ['user_id,item_id']
        for uid, item_id, lable in rec_res:
            line = '%s,%s' % (str(int(uid)), str(int(item_id)))
            wlines.append(line)
        fw.write('\n'.join(wlines))
        fw.close()


if __name__ == '__main__':
    dal = RECDAL()
    model = Model(dal, LR_pred_output_path)
    model.train()
    model.predict()
    print 'finish, saved in %s' % LR_pred_output_path
