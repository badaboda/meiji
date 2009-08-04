import MySQLdb
import sys, pprint

import merge,feed
from feed import kl


if __name__=='__main__':
    game_code ='20091114'

    db = feed.SportsDatabase(host='sports-livedb1',
                        user='root', passwd='damman#2',
                        db='kl', charset='utf8',
                        cursorclass=MySQLdb.cursors.DictCursor)

    bootstrap_dicts=[]
    for klass in kl.datums:
        datum=klass(db, game_code)
        bootstrap_dicts.append(datum.as_bootstrap_dict())


    merged=reduce(lambda x,y: merge._merge_insert(x, y), bootstrap_dicts)
    feed.mypprint(merged)

