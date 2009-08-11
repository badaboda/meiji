import MySQLdb
import sys, pprint, time

import merge, feed, config
from feed import kbo


def loop(db, delta, game_code):
    for klass in kbo.datums:
        datum=klass(db, game_code)
        delta.feed(datum)

    league_datums = [ kbo.LeagueTodayGames(db, game_code),
                      kbo.LeaguePastVsGames(db, game_code) ]
    game_codes = [game_code]
    for klass in kbo.league_datums:
        delta.feed(klass(db, game_code))
        game_codes += datum.rows

    for klass in kbo.scoreboard_datums:
        for c in game_codes:
            delta.feed(klass(db, c))


if __name__=='__main__':
    INTERVAL=5
    game_code=sys.argv[1]
    consumer = feed.JavascriptSysoutConsumer()
    delta=feed.DeltaGenerator(consumer)
    while True:
        loop(delta, game_code)
        time.sleep(INTERVAL)
