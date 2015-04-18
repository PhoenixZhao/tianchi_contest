#!/usr/bin/python
#coding=utf8
'''
    这个文件主要完成特征构建相关的方法
    下面划分点的意义即为训练数据和预测数据的分割点，如比赛要求预测19号的购买记录，那么12-19就是分割点；
    而自己线下训练，则用12-18作为训练集，也就是分割点
    4-16想到的特征:
        划分点一个月前的动作1，2，3，4的各自数量和总数；(5)
        划分点3天前的动作1，2，3，4的各自数量和总数(5)
        最后一次动作距离划分点的间隔(1)
        为保证正反例的比例，考虑将动作数少于2(或者3,使得正负样本比较均衡一点)的去掉，需要考虑是否为新品，比如最后一天出现的商品，动作数少于2是有可能的
        在split_date购买的label为1，未购买为0
'''

import logging
import time

from datetime import datetime, timedelta

from constant import features_log_file, features_output_path
from rec_dal import RECDAL


logging.basicConfig(filename=features_log_file,level=logging.INFO, format='%(asctime)s-%(message)s')

class FeatureGenerator(object):

    def __init__(self, dal, split_date_str, table_name):
        self.dal = dal
        self.split_date_str = split_date_str
        self.split_date = datetime.strptime(split_date_str, "%Y-%m-%d")
        self.l3d_date = self.split_date - timedelta(days=3)
        self.l3d_date_str = datetime.strftime(self.l3d_date, "%Y-%m-%d")
        self.table_name = table_name
        logging.info('init Generator: split_date=%s, l3d_date=%s, table_name=%s', self.split_date_str, self.l3d_date_str, self.table_name)

    def generate_features(self):
        '''
            划分点一个月前的动作1，2，3，4的各自数量和总数；(5)
        '''
        logging.info('starting generating features...')
        time1 = time.time()
        columns = ('user_id', 'item_id', 'behavior_type', 'behavior_time')
        records = self.dal.get_records_by_time(to_str=self.split_date_str, columns=columns)
        time2 = time.time()
        logging.info('finish dumping data from db time=%ds', time2 - time1)
        mon_nums, l3d_nums = {}, {}#{ui:[1,2,3,4,5]}
        item_l3d_clicks = {} #每个item最后3天的总动作数
        labels = {}
        lc_date_str = {} #最后一次活动的时间
        for uid, item_id, bt, b_time_str in records:
            item_id = str(item_id)
            ui = str(uid) + '-' + item_id
            #生成label
            labels[ui] = 0
            if bt == 4:
                labels[ui] = 1

            #生成过去一个月的动作数统计
            mon_nums.setdefault(ui,[0,0,0,0,0])[bt-1] += 1
            mon_nums.setdefault(ui,[0,0,0,0,0])[4] += 1#第五个位置即为总数

            #记录最后一次动作的时间串
            if b_time_str > lc_date_str.get(ui, ''):
                lc_date_str[ui] = b_time_str

            #生成划分点前3天的动作数统计
            if b_time_str >= self.l3d_date_str:
                l3d_nums.setdefault(ui,[0,0,0,0,0])[bt-1] += 1
                l3d_nums.setdefault(ui,[0,0,0,0,0])[4] += 1#第五个位置即为总数
                item_l3d_clicks[item_id] = item_l3d_clicks.get(item_id, 0) + 1

        time3 = time.time()
        logging.info('finish counting features, time=%ds', time3 - time2)
        #将所有的数据
        states_data = self.merge_features(mon_nums, l3d_nums, item_l3d_clicks, lc_date_str, labels)
        time4 = time.time()
        logging.info('finish merging features, time=%ds', time4 - time3)

        #将生成的特征存入数据库对应的表中
        self.save_states_data(states_data)

    def merge_features(self, mon_nums, l3d_nums, item_l3d_clicks, lc_date_str, labels):
        '''
            将生成不同维度的feature合并到一起，并与label进行合并，生成最后的训练数据
        '''
        logging.info('starting merging features...')
        states_data = []
        pos, neg, feature_num = 0, 0, 0
        for ui, m_features in mon_nums.items():
            uid, item_id = ui.split('-')
            one_states_data = [int(uid), int(item_id)]
            one_states_data.extend(m_features)
            one_states_data.extend(l3d_nums.get(ui,[0,0,0,0,0]))

            #item_id = ui.split('-')[1]
            #one_states_data.append(item_l3d_clicks.get(item_id, 0))

            ui_lc_date = datetime.strptime(lc_date_str[ui], "%Y-%m-%d %H")#数据库的时间类型为"2014-12-18 8"
            #datetime的类型计算天数差值不准确，必须转换为date类型
            lc_delta = self.split_date.date() - ui_lc_date.date()
            if lc_delta.days == 0:
                logging.info('lc_delta days is 0, ui=%s, split_date=%s, ui_lc_date=%s', ui, self.split_date_str, lc_date_str[ui])
            one_states_data.append(lc_delta.days)

            #one_states_data.append(labels[ui])

            if labels[ui] == 1:
                pos += 1
            else:
                neg += 1
            if not feature_num:
                feature_num = len(one_states_data) - 2
            states_data.append(one_states_data)

        logging.info('merged all features,feature_num=%s; samples: pos=%s neg=%s', feature_num, pos, neg)

        return states_data

    def save_states_data(self, states_data):
        '''
            将生成的feature全部存入数据库对应的表中
        '''
        logging.info('start saving states_data...')
        delta = 10000
        for ind in range(len(states_data)):
            states_data[ind].insert(0, ind+1)#增加插入数据库的id
            if ind and ind % delta == 0:
                self.dal.insert_records(self.table_name, states_data[ind-delta:ind])
        #最后会剩的一部分需要再插入数据库
        self.dal.insert_records(self.table_name, states_data[(ind - ind % delta):])

        logging.info('%s states_data saved in %s', len(states_data), self.table_name)

if __name__ == '__main__':
    dal = RECDAL()
    split_date_str = '2014-12-17'
    table_name = 'split_20141217_stats'
    generator = FeatureGenerator(dal, split_date_str, table_name)
    generator.generate_features()
