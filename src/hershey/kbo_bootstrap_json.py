import sys, MySQLdb

import merge, feed, config
from feed import kbo

def kbo_bootstrap_dict(db, game_code):
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
    return merged

if __name__=='__main__':
    game_code ='20090701HHSK0'
    db = feed.SportsDatabase(db='kbo', **config.sports_live_db1_credential)
    merged=kbo_bootstrap_dict(db, game_code)
    feed.mypprint(merged, encoding='unicode_escape')
