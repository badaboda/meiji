import MySQLdb
import sys, pprint

import merge,feed
from feed import npb

def npb_bootstrap_dict(db, game_code):
    bootstrap_dicts=[]
    for klass in npb.datums:
        datum=klass(db, game_code)
        bootstrap_dicts.append(datum.as_bootstrap_dict())


    for klass in npb.scoreboard_datums:
        bootstrap_dicts.append(klass(db, game_code).as_bootstrap_dict())

    merged=reduce(lambda x,y: merge._merge_insert(x, y), bootstrap_dicts)
    return merged

if __name__=='__main__':
    game_code ='2009072101'

    db = feed.SportsDatabase(host='sports-livedb1',
                        user='root', passwd='damman#2',
                        db='npb', charset='utf8',
                        cursorclass=MySQLdb.cursors.DictCursor)
    merged=npb_bootstrap_dict(db, game_code)
    feed.mypprint(merged)

