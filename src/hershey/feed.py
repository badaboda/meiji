#!/usr/bin/env python25
# vim: et sts=4 sw=4 ts=8 ai :
import MySQLdb

if __name__=='__main__':
    db=MySQLdb.connect(host='sports-livedb1', user='root', passwd='damman#2',db='kbo', charset='utf8')
    c=db.cursor()
    c.execute("""
           SELECT 
               * 
           FROM 
               IE_LiveText a 
           WHERE 
               a.gameID = '20090624OBLT0' 
           ORDER BY 
               a.SeqNO asc
    """)
    print c.fetchall()

