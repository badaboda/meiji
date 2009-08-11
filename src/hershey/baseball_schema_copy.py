import MySQLdb
import MySQLdb.cursors

import feed, config

def tables_for(db, league): 
    rows=db.execute("show tables")
    return [row[0] for row in rows]

def print_schema(league):
    db = feed.SportsDatabase(db=league, **{
        'host': 'sports-livedb1', 
        'user': 'root',
        'passwd': 'damman#2',
        'charset': 'utf8',
    })
    print """
        DROP DATABASE IF EXISTS %(league)s_test;
        create DATABASE %(league)s_test;
        use %(league)s_test;
    """ % {'league': league}
    for table in tables_for(db, league):
        print "DROP TABLE IF EXISTS %s;" % table
        print db.execute("show create table %s" % table)[0][1]
        print ';'

if __name__=='__main__':
    print_schema("npb")
    print_schema("mlb")
    print_schema("kbo")
