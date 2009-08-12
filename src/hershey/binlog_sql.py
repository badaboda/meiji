import MySQLdb
import MySQLdb.cursors

import re, datetime
re_timestamp=re.compile('^SET TIMESTAMP=(\d+);')
re_use=re.compile('use\s(.*);')

def db_and_sql_until(f, current_db_name, to_dt):
    for line in f:
        if line.startswith('#'):
            continue

        m=re_timestamp.search(line)
        if not m:
            sql=line
            m=re_use.search(sql)
            if m:
                db_name=m.group(1)
                current_db_name=db_name
                yield current_db_name, None
            else:
                yield current_db_name, sql
        else:
            timestamp=int(m.group(1))
            cur_dt=datetime.datetime.fromtimestamp(timestamp)
            if cur_dt > to_dt:
                raise StopIteration()
         
def sql_from_to(f, default_db_name, interested_db_name, start, end, interval):
    _cur_db=default_db_name
    cur=start
    while cur < end:
        cur += interval

        filtered=[]
        for db, sql in db_and_sql_until(f, _cur_db, cur):
            _cur_db=db
            if db==interested_db_name and sql is not None:
                filtered.append(sql)
        yield filtered

if __name__=='__main__':
    f=open("./mysql_binlog_sql.sample")
    start=datetime.datetime(2009, 7, 21, 18, 30, 0)
    end=datetime.datetime(2009, 7, 21, 18, 40, 0)
    for sql in sql_from_to(f, 'npb', 'kbo', start, end, datetime.timedelta(seconds=10)):
        print sql

