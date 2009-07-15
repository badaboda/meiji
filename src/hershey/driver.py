import MySQLdb
import sys, pprint

import feed, merge

def write(o):
    sys.stdout.write(str(o))
    #sys.stdout.write("\n")

def mypprint(o, write=write):
    if type(o)==type({}):
        write('{')
        for k in o.keys():
            write('"%s":'% k)
            mypprint(o[k])
            write(',')
        write('}')
    elif type(o)==type([]):
        write('[')
        for e in o:
            mypprint(e)
            write(',',)
        write(']')
    elif type(o)==type((None,)):
        write('[')
        for e in o:
            mypprint(e)
            write(',',)
        write(']')
    elif type(o)==type(u''):
        write('"%s"' % o.encode('unicode_escape'))
    elif type(o)==type(''):
        write('"%s"' % o)
    elif type(o) in [type(0), type(0L), type(0.3)]:
        write(o)
    elif type(o)==type(True):
        write(str(o).lower())
    elif type(o)==type(None):
        write('null')
    else:
        raise ValueError(type(o))

if __name__=='__main__':
    game_code ='20090701HHSK0'
    datums = [feed.RegistryPlayerProfile,
                    feed.RegistryPlayerBatterSeason,
                    feed.RegistryPlayerBatterToday,
                    feed.RegistryPlayerPitcherToday, 
                    feed.RegistryPlayerPitcherSeason, 
                    feed.RegistryTeamSeason, 
                    feed.RegistryTeamProfile,
                    feed.LiveText, 
                    feed.Meta,
                    feed.GameCode]

    league_datums = [ feed.LeagueTodayGames, 
                      feed.LeaguePastVsGames ]

    scoreboard_datums = [feed.ScoreBoard,
                       feed.ScoreBoardHome, 
                       feed.ScoreBoardAway,
                       feed.ScoreBoardBases, 
                       feed.ScoreBoardWatingBatters,]

    db = feed.SportsDatabase(host='sports-livedb1', 
                        user='root', passwd='damman#2',
                        db='kbo', charset='utf8',
                        cursorclass=MySQLdb.cursors.DictCursor)

    initial_dicts=[]
    for klass in datums:
        datum=klass(db, game_code)
        initial_dicts.append(datum.as_initial_dict())

    league_datums = [ feed.LeagueTodayGames(db, game_code), 
                      feed.LeaguePastVsGames(db, game_code) ]
    game_codes = [game_code]
    for datum in league_datums:
        initial_dicts.append(datum.as_initial_dict())
        game_codes += datum.rows

    for klass in scoreboard_datums:
        for c in game_codes:
            initial_dicts.append(klass(db, c).as_initial_dict())

    merged=reduce(lambda x,y: merge._merge_insert(x, y), initial_dicts)
    mypprint(merged)

