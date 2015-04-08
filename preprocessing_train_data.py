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
        for l in lines:
            parts = l.strip().split(',')
            r_date = datetime.strptime(parts[-1], "%Y-%m-%d %H")
            parts.append(str(r_date.isoweekday()))
            wlines.append(parts)
        wlines = sorted(wlines, key=lambda d: d[-2], reverse=True)
        fw = open(filename+".pre", "w+")
        fw.write('\n'.join([",".join(r) for r in wlines]))
        fw.close()


if __name__ == '__main__':
    main()

