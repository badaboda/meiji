import MySQLdb
import sys, pprint, time

import merge, feed, config
from feed import kl


def loop(delta, game_code):
    db = feed.SportsDatabase(db='kl', **config.sports_live_db1_credential)
    for klass in kl.datums:
        datum=klass(db, game_code)
        delta.feed(datum)


if __name__=='__main__':
    INTERVAL=5
    game_code=sys.argv[1]
    consumer = feed.JavascriptSysoutConsumer()
    delta=feed.DeltaGenerator(consumer)
    while True:
        loop(delta, game_code)
        time.sleep(INTERVAL)
