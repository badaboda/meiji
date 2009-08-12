from sqlalchemy.ext.sqlsoup import SqlSoup
from sqlalchemy.ext.sqlsoup import Session
from sqlalchemy.exc import IntegrityError

from sqlalchemy.orm import sessionmaker

def sqlsoup_insert_dict(d):
    n = {}
    for k in d.keys():
        if k.startswith('_'):
            continue
        else:
            n[str(k)]=d[k]
    return n

def copy_row(src, target, table, where_key, game_code):
    entity=src.entity(table)
    objs=entity.filter(getattr(entity, where_key)==game_code).all()

    entity=target.entity(table)
    for o in objs:
        entity.insert(**sqlsoup_insert_dict(o.__dict__))
    #print entity.filter(getattr(entity, where_key)==game_code).all()
    target.flush()


if __name__=='__main__':
    game_code='20090721LGHT0'

    session=Session()
    src=SqlSoup('mysql://root:damman#2@sports-db1/kbo?charset=utf8')
    target=SqlSoup('mysql://root@localhost/kbo_test?charset=utf8')

    for table, where_key in [('Kbo_Schedule', 'gmkey'),
                            ('IE_BatterRecord', 'gameID'),
                            ('IE_ScoreRHEB', 'gameID')]:
        try:
            copy_row(src, target, table, where_key, game_code)
        except IntegrityError:
            session.rollback()

