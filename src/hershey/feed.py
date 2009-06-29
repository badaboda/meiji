#!/usr/bin/env python2.6
# vim: et sts=4 sw=4 ts=4 ai :
import MySQLdb
from difflib import SequenceMatcher
import time

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
    sql = """
        SELECT 
            hershey.IE_LiveText.gameID    AS gameCode, 
            hershey.IE_LiveText.LiveText  AS liveText, 
            hershey.IE_LiveText.SeqNO     AS seqNo, 
            hershey.IE_LiveText.Inning    AS inning, 
            hershey.IE_LiveText.bTop      AS bTop, 
            hershey.IE_LiveText.textStyle AS textStyle 
        FROM 
            hershey.IE_LiveText 
        WHERE 
            hershey.IE_LiveText.gameID = '1'  
        ORDER BY 
            seqNo 
    """
    before = ()
    while True:
        db = SportsDatabase(host='sports-testdb2', user='hanadmin', passwd='damman#2',db='hershey', charset='utf8')
        with db as db:
           after = db.execute(sql)

        cruncher = SequenceMatcher(None, before, after)
        for tag, i1, i2, j1, j2 in cruncher.get_opcodes():
            #print ("%7s before[%d:%d] (%s) after[%d:%d] (%s)" % (tag, i1, i2, before[i1:i2], j1, j2, after[j1:j2])) 
            if tag == "insert":
                print "insert",after[j1:j2]
            elif tag == "delete":
                print "delete",before[i1:i2]
            elif tag == "replace":
                print "replace from ",before[i1:i2]," to ",after[j1:j2]
            elif tag == "equal":
                print "equal"
            else:
                raise ValueError, 'unknown tag %r' % (tag,)
            #print ("%7s before[%d:%d] after[%d:%d] " % (tag, i1, i2, j1, j2)) 
        before = after
        time.sleep(5)
