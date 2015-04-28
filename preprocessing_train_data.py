#!/usr/bin/python
#coding=utf=8
'''
    1，增加周一周二这样的显示；
    2，按时间降序排列
'''

import sys
from datetime import datetime

def main():
    if len(sys.argv) == 2:
        filename = sys.argv[1]
        lines = open(filename, 'r').readlines()
        wlines = []
        if 'user_id' in lines[0]:
            #如果第一行是注释说明，添加一个weekday的标识
            #wlines.append(lines[0].strip().split(',').append('weekday'))
            del lines[0]
        for l in lines:
            parts = l.strip().split(',')
            r_date = datetime.strptime(parts[-1], "%Y-%m-%d %H")
            parts.append(str(r_date.isoweekday()))
            wlines.append(parts)
        #wlines = sorted(wlines, key=lambda d: d[-2], reverse=True)
        fw = open(filename+".pre", "w+")
        fw.write('\n'.join([",".join(r) for r in wlines]))
        fw.close()


if __name__ == '__main__':
    main()

