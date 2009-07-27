import MySQLdb
import sys, pprint

import merge,feed
from feed import kbo


if __name__=='__main__':
    game_code ='20090701HHSK0'

    db = feed.SportsDatabase(host='sports-livedb1',
                        user='root', passwd='damman#2',
                        db='kbo', charset='utf8',
                        cursorclass=MySQLdb.cursors.DictCursor)

    bootstrap_dicts=[]
    for klass in kbo.datums:
        datum=klass(db, game_code)
        bootstrap_dicts.append(datum.as_bootstrap_dict())

    league_datums = [ kbo.LeagueTodayGames(db, game_code, feed.gamecode_to_datetime(game_code)),
                      kbo.LeaguePastVsGames(db, game_code) ]
    game_codes = [game_code]
    for datum in league_datums:
        bootstrap_dicts.append(datum.as_bootstrap_dict())
        game_codes += datum.rows

    for klass in kbo.scoreboard_datums:
        for c in game_codes:
            bootstrap_dicts.append(klass(db, c).as_bootstrap_dict())

    merged=reduce(lambda x,y: merge._merge_insert(x, y), bootstrap_dicts)
    feed.mypprint(merged)

