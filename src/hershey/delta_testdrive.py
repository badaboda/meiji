import datetime 

import feed, config
from binlog_sql import *

if __name__=='__main__':
    f=open("./mysql_binlog_sql.sample")
    
    consumer = feed.JavascriptSysoutConsumer()
    delta=feed.DeltaGenerator(consumer)
    game_code ='20090721LGHT0'

    db = feed.SportsDatabase(db='kbo_test', **config.enex_credential)

    start=datetime.datetime(2009, 7, 21, 18, 30, 0)
    end=datetime.datetime(2009, 7, 21, 18, 40, 0)
    for sqls_within_interval in sql_from_to(f, start, end, datetime.timedelta(seconds=10)):
        for sql in sqls_within_interval:
            print sql
            db.execute(sql)

        print '-' * 30
        import kbo_delta
        kbo_delta.loop(db, delta, game_code)
