#!/usr/bin/env python2.6
# vim: et sts=4 sw=4 ts=4 ai :
import MySQLdb
from difflib import SequenceMatcher
import time
import datetime

import router_client

class ContextCursor:
    def __init__(self, cursor):
        self.cursor = cursor
    def __getattr__(self, attr):
        v=getattr(self.cursor,attr)
        if v:
            return v
    def __enter__(self):
        return self.cursor
    def __exit__(self, type, value, traceback):
        self.cursor.close()

class SportsDatabase(object):
    def __init__(self, **args):
        self.db = MySQLdb.connect(**args)
    def cursor(self):
        return ContextCursor(self.db.cursor())
    def close(self):
        self.db.close()
    def __enter__(self):
        return self
    def __exit__(self, type, value, traceback):
        self.close()

    def execute(self, sql):
        with self.cursor() as c:
            c.execute(sql)
            r = c.fetchall()
        return r

       
if __name__=='__main__':
    gameId ='20090701HHSK0'
    sql = """
        SELECT 
            kbo.IE_LiveText.gameID    AS gameCode, 
            kbo.IE_LiveText.LiveText  AS liveText, 
            kbo.IE_LiveText.SeqNO     AS seqNo, 
            kbo.IE_LiveText.Inning    AS inning, 
            kbo.IE_LiveText.bTop      AS bTop, 
            kbo.IE_LiveText.textStyle AS textStyle 
        FROM 
            kbo.IE_LiveText 
        WHERE 
            kbo.IE_LiveText.gameID = '%s'
        ORDER BY 
            seqNo 
    """ % (gameId,)

    before = ()
    #router_client.create(gameId)
    print "created"
    while True:
        db = SportsDatabase(host='sports-livedb1', user='root', passwd='damman#2',db='kbo', charset='utf8')
        with db as db:
           after = db.execute(sql)

        cruncher = SequenceMatcher(None, before, after)
        for tag, i1, i2, j1, j2 in cruncher.get_opcodes():
            a = []
            if tag == "insert":
                for t in after[j1:j2]:
                    a.append("(%s) %s" % (t[2], t[1]))
                router_client.send_as_raw(gameId, 
                    ("<br/> [%s] [%s] " % (datetime.datetime.today(),tag)).join(a).encode("utf8"))
            elif tag == "delete":
                for t in before[i1:i2]:
                    a.append("(%s) %s" % (t[2], t[1]))
                router_client.send_as_raw(gameId, 
                    ("<br/> [%s] [%s] " % (datetime.datetime.today(),tag)).join(a).encode("utf8"))
            elif tag == "replace":
                for t in after[j1:j2]:
                    a.append("(%s) %s" % (t[2], t[1]))
                router_client.send_as_raw(gameId, 
                    ("<br/> [%s] [%s] " % (datetime.datetime.today(),tag)).join(a).encode("utf8"))
            elif tag == "equal":
                print "equal"
            else:
                raise ValueError, 'unknown tag %r' % (tag,)
        before = after
        time.sleep(5)
