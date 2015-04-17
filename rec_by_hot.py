#!/usr/bin/python
#coding=utf8
'''
    使用热门推荐:
        1，找出最近一周的热门商品，给下面两种用户推荐:
            1) 对热门item有过收藏和加购物车行为的用户直接推荐热门的试试看：
            2) 对过去一个月有购买行为的用户推荐
        2,最近一周，也可以考虑自然周(也就是每次只从周一开始算起)
'''

from datetime import timedelta, datetime
from itertools import product

from rec_dal import RECDAL
from constant import database_name
dal = RECDAL(database_name)

from util import date2str, save_standard_results

from constant import submission_dir

def get_candidate_users_from_hot_items(hot_items, beh_types):
    '''
        选出对热门item有过收藏和加购物车行为的用户,这部分用户最有可能有购买行为
    '''
    return dal.get_users_by_items(hot_items, beh_types)

def get_recent_hot_items(current_day, delta_days=7, topK=10):
    '''
        往current_day往前推delta_days的时间，然后选出这段时间topK的热门商品
    '''
    from_date = current_day - timedelta(days=delta_days)
    user_items = dal.get_records_by_time(date2str(from_date), date2str(current_day))
    print 'get %s user_items from %s to %s' % (len(user_items), date2str(from_date), date2str(current_day))
    item_clicks = {}
    for uid, item_id in user_items:
        item_clicks[item_id] = item_clicks.get(item_id, 0) + 1
    item_clicks = sorted(item_clicks.items(), key=lambda d:d[1], reverse=True)
    return [iid for iid, _ in item_clicks[:topK]]

def rec_by_hot_items():
    '''
        根据热门item进行预测，预测的用户对该热门items有过收藏或者加购物车行为
        2015-04-14-exp1:
            1, 只要历史中有过收藏或者加购物车行为的用户，全部算candidate用户
            2, 每个用户都推荐topK，并未对每个用户只预测它加入购物车的item
        2015-04-14-exp2:
            1, 只要历史中有过收藏或者加购物车行为的用户，全部算candidate用户
            2, 对每个用户只推荐它加入购物车的item,即需要过滤一下product生成的items
        2015-04-14-exp3:
            1，给每个用户推荐过去7天它加入收藏或者购物车但还未购买的item
    '''
    date_str = '2014-12-12'
    predicted_day = datetime.strptime(date_str, "%Y-%m-%d")
    topK = 20
    delta_days = 4
    beh_types = (2,3)
    filename = '2015-04-14-exp1.csv'
    print 'expriments infor:\npredicted_date=%s\ntopK=%s\nbeh_types=%s' % (date_str, topK, beh_types)
    hot_items = get_recent_hot_items(predicted_day, delta_days=delta_days, topK=topK)
    users = get_candidate_users_from_hot_items(tuple(hot_items), beh_types)
    user_items = [r for r in product(users, hot_items)]
    print 'rec by hot items, %s candidate_users, %s hot items' % (len(users), len(hot_items))
    save_standard_results(user_items, submission_dir + filename)

if __name__ == '__main__':
    rec_by_hot_items()
