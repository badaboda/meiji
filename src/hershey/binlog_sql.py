import MySQLdb
import MySQLdb.cursors

import re, datetime
re_timestamp=re.compile('^SET TIMESTAMP=(\d+);')
re_use=re.compile('use\s(.*);')

def sql_to(f, to_dt):
    for line in f:
        if line.startswith('#'):
            continue

        m=re_timestamp.search(line)
        if not m:
            sql=line
            m=re_use.search(sql)
            if m:
                db_name=m.group(1)
                yield 'use %s_test;' % db_name
            else:
                yield sql
        else:
            timestamp=int(m.group(1))
            cur_dt=datetime.datetime.fromtimestamp(timestamp)
            if cur_dt > to_dt:
                raise StopIteration()
         
def sql_from_to(f, start, end, interval):
    cur=start
    while cur < end:
        cur += interval
        yield sql_to(f, cur)

if __name__=='__main__':
    f=open("./mysql_binlog_sql.sample")
    start=datetime.datetime(2009, 7, 21, 18, 30, 0)
    end=datetime.datetime(2009, 7, 21, 18, 40, 0)
    for sql_within_interval in sql_from_to(f, start, end, datetime.timedelta(seconds=10)):
        print list(sql_within_interval)

