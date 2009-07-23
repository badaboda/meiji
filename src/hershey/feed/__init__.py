# vim: et sts=4 sw=4 ts=4 ai fileencoding=utf8 :
import MySQLdb
import MySQLdb.cursors

import types, exceptions, sys, datetime
from difflib import SequenceMatcher

def _dict_to_tuple_recursive(dict):
    result=[]
    if type(dict) != types.DictType:
        raise ValueError('DicType needed')
    for k,v in dict.items():
        if type(v)==types.DictType:
            result.append((k, _dict_to_tuple_recursive(v)))
        else:
            result.append((k, v))
    return tuple(result)

def list_of_dict_to_list_of_pairs(list_of_dict):
    return tuple(_dict_to_tuple_recursive(d) for d in list_of_dict)

def lowercase_dict_key(d):
    return dict([(k.lower(),v) for k,v in d.items()])

def hierachy_dict(hierachy_parent_names, leaf):
    orig=r={}
    for n in hierachy_parent_names[:-1]:
        r[n]={}    # cann't be shorten as r=r[n]={}
        r=r[n]
    r[hierachy_parent_names[-1]]=leaf
    return orig

def gamecode_to_datetime(gamecode):
    return datetime.datetime(int(gamecode[:4]), int(gamecode[4:6]), int(gamecode[6:8]))

class Error(exceptions.Exception):
    pass

class NoDataFoundError(exceptions.Exception):
    pass

class NoDataFoundForScoreboardError(exceptions.Exception):
    pass

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


class DeltaGenerator:
    def __init__(self, consumer):
        self.consumer = consumer
        self.old_input = {}

    def feed(self, datum):
        current = datum.as_delta_generator_input()
        json_path=datum.json_path()
        if self.old_input.has_key(json_path):
            for tag, list_of_pair in self.diff_seq_specs(self.old_input[json_path], current):
                if tag == 'equal':
                    pass
                else:
                    self.consumer.feed(tag, json_path, dict(list_of_pair))
        self.old_input[json_path] = current

    def diff_seq_specs(self, old, current):
        cruncher = SequenceMatcher(None, old, current)
        for tag, i1, i2, j1, j2 in cruncher.get_opcodes():
            a = []
            if tag == "insert":
                for t in current[j1:j2]:
                    yield tag, t
            elif tag == "delete":
                for t in old[i1:i2]:
                    yield tag, t
            elif tag == "replace":
                #for t in old[i1:i2]:
                #    yield "delete", t
                #for t in current[j1:j2]:
                #    yield "insert", t
                for t in current[j1:j2]:
                    yield tag, t
            elif tag == "equal":
                yield tag, None
            else:
                raise ValueError, 'unknown tag %s' % (tag,)

class JavascriptSysoutConsumer:
    def __init__(self):
        pass

    def emit(self, s):
        print s

    def feed(self, tag, json_path, dict):
        if tag in ['insert', 'replace']:
            self.emit(self.insert_javascript(json_path, dict))
        elif tag=='delete':
            self.emit(self.delete_javascript(json_path, dict))
        else:
            raise NotImplementedError

    def insert_javascript(self, json_path, dict):
        return "%s=%s;" % (self._js_variable("db", json_path, dict), str(dict))
        
    def delete_javascript(self, json_path, dict):
        return "delete %s;" % (self._js_variable("db", json_path, dict))

    def _js_variable(self, root, json_path, dict):
        keys=json_path.split(":")
        js_key_parts=[root]
        for i, k in enumerate(keys):
            is_placeholder=lambda s: s.startswith("**")
            if is_placeholder(k):
                js_key_parts.append("['%s']" % dict[k[2:]])    
            else:
                js_key_parts.append(".%s" % k)
        return ''.join(js_key_parts) 

# ----------

class RelayDatum(object):
    def __init__(self, db, game_code):
        self.db = db
        self.game_code = game_code

        self.rows = None
        self.extra_args = None

    def sql(self):
        raise NotImplemented()

    def ensure_rows(self):
        if not self.rows:
            self.rows=[self.postprocess(row) for row in self.fetch()]

    def as_delta_generator_input(self):
        self.ensure_rows()
        return list_of_dict_to_list_of_pairs(self.rows) 

    def __parse_json_path(self, json_path):
        path=json_path.split(':')
        is_key=lambda s: s.startswith('**')
        index_of_key=None
        try:
            index_of_key=map(is_key, path).index(1)
        except ValueError:
            pass
        return path, index_of_key, path[:index_of_key], path[index_of_key+1:]

    def as_bootstrap_dict(self):
        self.ensure_rows()
        path, index_of_key, path_before_key, path_after_key = self.__parse_json_path(self.json_path())

        if index_of_key:
            key=path[index_of_key][2:]
            result = {}
            for row in self.rows:
                path_after_key=path[index_of_key+1:]
                if path_after_key:
                    result[row[key]]=hierachy_dict(path_after_key, row)
                    del row[key]
                else:
                    result[row[key]]=row
                    
            return hierachy_dict(path_before_key, result)
        else:
            return hierachy_dict(path, self.rows)

    def postprocess(self, row):
        if type(row) == types.DictType:
            if row.has_key('INPUTTIME'):
                del row['INPUTTIME']
            return lowercase_dict_key(row)
        else:
            return row

class RelayDatumAsList(RelayDatum):
    def as_bootstrap_dict(self):
        self.ensure_rows()
        path=self.json_path().split(':')
        return hierachy_dict(path, self.rows)

class RelayDatumAsAtom(RelayDatum):
    def as_bootstrap_dict(self):
        self.ensure_rows()
        path=self.json_path().split(':')
        return hierachy_dict(path, self.rows[0])

# ----------

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

