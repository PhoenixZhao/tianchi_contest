#!/usr/bin/python
#coding=utf8
'''
    根据自己的需求筛选出数据观察
'''

from rec_dal import RECDAL
dal = RECDAL()
sql = 'select * from user_behaviors where user_behaviors.behavior_weekday = 5 and user_behaviors.behavior_type = 4'
records = dal.get_records_by_sql(sql)
nums = {}
w_records = {}
for r in records:
    ui_id = str(r[1]) + str(r[2])
    nums[ui_id] = nums.get(ui_id, 0) + 1
    w_records.setdefault(ui_id, []).append(r)

for k, v in nums.items():
    if v ==3:
        for r in w_records[k]:
            print r


