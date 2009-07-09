#!/usr/bin/env python2.6
# vim: et sts=4 sw=4 ts=4 ai :
import MySQLdb
import MySQLdb.cursors

from difflib import SequenceMatcher
import time
import datetime

#import router_client
import mock_router_client as router_client

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
        if self.db:
            self.db.close()
            self.db=None
    def __enter__(self):
        return self
    def __exit__(self, type, value, traceback):
        self.close()

    def execute(self, sql):
        with self.cursor() as c:
            c.execute(sql)
            r = c.fetchall()
        return r

class RelayDatum:
    def __init__(self):
        self.before = {}
        self.after = None

    def sql(self):
        raise NotImplemented()

    def list_of_dict_to_list_of_pairs(self, list_of_dict):
        return tuple(tuple(d.items()) for d in list_of_dict)

    def as_diff_strings(self):
        cruncher = SequenceMatcher(None, self.list_of_dict_to_list_of_pairs(datum.before), self.list_of_dict_to_list_of_pairs(datum.after))
        for tag, i1, i2, j1, j2 in cruncher.get_opcodes():
            a = []
            if tag == "insert":
                for t in datum.after[j1:j2]:
                    yield tag, t
                #yield "<br/> [%s] [%s] " % (datetime.datetime.today(),tag)).join(a).encode("utf8")
            elif tag == "delete":
                for t in datum.before[i1:i2]:
                    yield tag, t
                #yield "<br/> [%s] [%s] " % (datetime.datetime.today(),tag)).join(a).encode("utf8")
            elif tag == "replace":
                for t in datum.after[j1:j2]:
                    yield tag, t
                #yield "<br/> [%s] [%s] " % (datetime.datetime.today(),tag)).join(a).encode("utf8")
            elif tag == "equal":
                yield "equal"
            else:
                raise ValueError, 'unknown tag %r' % (tag,)

class LiveText(RelayDatum):
    def sql(self):
        return """
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
        """

class Score(RelayDatum):
    def sql(self):
        return  """
        SELECT 
            kbo.IE_Scoreinning.inning AS inning, 
            kbo.IE_ScoreRHEB.Run as r, 
            kbo.IE_ScoreRHEB.Hit as h, 
            kbo.IE_ScoreRHEB.Error as e, 
            kbo.IE_ScoreRHEB.BallFour as b, 
            kbo.IE_Scoreinning.Score as score, 
            kbo.IE_Scoreinning.bHome AS bhome 
        FROM 
            kbo.IE_Scoreinning 
        INNER JOIN 
            kbo.IE_ScoreRHEB 
            ON 
            (
                kbo.IE_Scoreinning.gameID = kbo.IE_ScoreRHEB.gameID
            ) 
            AND 
            (
                kbo.IE_Scoreinning.bHome = kbo.IE_ScoreRHEB.bHome
            ) 
        WHERE 
            kbo.IE_Scoreinning.gameID = '%s'
        ORDER BY 
            bhome DESC, 
            inning  
    """
    
       
if __name__=='__main__':
    gameId ='20090701HHSK0'
    datums = [LiveText(), Score()]
    #router_client.create(gameId)

    print "created"
    while True:
        for datum in datums:
            db = SportsDatabase(host='sports-livedb1', user='root', passwd='damman#2',db='kbo', charset='utf8', cursorclass=MySQLdb.cursors.DictCursor)
            with db as db:
               datum.after = db.execute(datum.sql() % gameId)

            for tag, diff in datum.as_diff_strings():
                router_client.send_as_raw(gameId, "%s, %s, %s" % (datum.__class__, tag, diff))

            datum.before = datum.after
        time.sleep(5)


