# vim: et sts=4 sw=4 ts=4 ai fileencoding=utf8 :
import MySQLdb
import MySQLdb.cursors
import merge

import types, exceptions, sys, datetime, decimal
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

def list_of_dict_to_list_of_list_of_pair(list_of_dict):
    return tuple(_dict_to_tuple_recursive(d) for d in list_of_dict)

def dict_to_list_pair(d):
    return _dict_to_tuple_recursive(d)  

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
    def __init__(self, game_code, table):
        super(NoDataFoundForScoreboardError, self).__init__("no data for game_code(%s) at table(%s)" % (game_code, table))

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
            for tag, list_of_pair, i1, i2, j1, j2 in self.diff_seq_specs(self.old_input[json_path], current):
                if tag == 'equal':
                    pass
                else:
                    if not isinstance(datum, RelayDatumAsAtom):
                        delta=Delta(dict(list_of_pair), i1, i2, j1, j2)
                    else:
                        delta=Delta(dict((list_of_pair,)), i1, i2, j1, j2)

                    self.consumer.feed(tag, datum, delta)

        self.old_input[json_path] = current

    def diff_seq_specs(self, old, current):
        cruncher = SequenceMatcher(None, old, current)
        for tag, i1, i2, j1, j2 in cruncher.get_opcodes():
            #print tag, i1, i2, j1, j2 
            a = []
            if tag == "insert":
                for t in current[j1:j2]:
                    yield tag, t, i1, i2, j1, j2
            elif tag == "delete":
                for t in old[i1:i2]:
                    yield tag, t, i1, i2, j1, j2
            elif tag == "replace":
                #for t in old[i1:i2]:
                #    yield "delete", t
                #for t in current[j1:j2]:
                #    yield "insert", t
                for t in current[j1:j2]:
                    yield tag, t, i1, i2, j1, j2
            elif tag == "equal":
                yield tag, None, i1, i2, j1, j2
            else:
                raise ValueError, 'unknown tag %s' % (tag,)

class Delta(object):
    def __init__(self, dict, i1, i2, j1, j2):
        self.dict=dict
        self.i1, self.i2, self.j1, self.j2 = i1, i2, j1, j2

class JavascriptSysoutConsumer:
    def __init__(self):
        pass

    def emit(self, s):
        print s

    def emit_insert(self, datum, delta):
        self.emit(datum.javascript_insert_output(delta))   

    def emit_delete(self, datum, delta):
        self.emit(datum.javascript_delete_output(delta))

    def emit_replace(self, datum, delta):
        self.emit(datum.javascript_replace_output(delta))

    def feed(self, tag, datum, delta):
        f=getattr(self, 'emit_%s' % tag)
        f(datum, delta)

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
        return list_of_dict_to_list_of_list_of_pair(self.rows)

    def __parse_json_path(self, json_path):
        paths=json_path.split(':')
        is_key=lambda s: s.startswith('**')
        index_of_key=None
        try:
            index_of_key=map(is_key, paths).index(1)
        except ValueError:
            pass
        return paths, index_of_key, paths[:index_of_key], paths[index_of_key+1:]

    def as_bootstrap_dict(self):
        self.ensure_rows()
        paths, index_of_key, paths_before_key, paths_after_key = self.__parse_json_path(self.json_path())

        if index_of_key:
            key=paths[index_of_key][2:]
            result = {}
            for row in self.rows:
                paths_after_key=paths[index_of_key+1:]
                if paths_after_key:
                    result[row[key]]=hierachy_dict(paths_after_key, row)
                    del row[key]
                else:
                    result[row[key]]=row

            return hierachy_dict(paths_before_key, result)
        else:
            return hierachy_dict(paths, self.rows)

    def postprocess(self, row):
        if type(row) == types.DictType:
            if row.has_key('INPUTTIME'):
                del row['INPUTTIME']
            return lowercase_dict_key(row)
        else:
            return row

    def javascript_insert_output(self, delta):
        dict=delta.dict
        return "%s=%s;" % (self._js_variable("db", self.json_path(), dict), str(dict))

    def javascript_delete_output(self, delta):
        return "delete %s;" % (self._js_variable("db", self.json_path(), delta.dict))

    def javascript_replace_output(self, delta):
        dict=delta.dict
        return "%s=%s;" % (self._js_variable("db", self.json_path(), dict), str(dict))

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

class RelayDatumAsList(RelayDatum):
    def as_bootstrap_dict(self):
        self.ensure_rows()
        paths=self.json_path().split(':')
        return hierachy_dict(paths, self.rows)

    def javascript_insert_output(self, delta):
        return 'db.%s.push(%s);' % (self.json_path(), str(delta.dict))   # splice로 하면 좋은데 여러개가 동시에 지워졌을 때 
                                                                # index가 바뀌어 버리는 것을 처리하기가 마땅치가 않다
                                                                #   야구에서는 list를 쓰는 것이 끝에만 추가되는 것이라 
                                                                #   array.push로 써도 돌아는 간다
    def javascript_delete_output(self, delta):
        return 'db.%s.splice(%d, %d);' % (self.json_path(), delta.i1, delta.i2-delta.i1)

class RelayDatumAsAtom(RelayDatum):
    def as_bootstrap_dict(self):
        self.ensure_rows()
        paths=self.json_path().split(':')
        return hierachy_dict(paths, self.rows[0])

    def as_delta_generator_input(self):
        self.ensure_rows()
        assert len(self.rows)==1
        return dict_to_list_pair(self.rows[0])

    def javascript_replace_output(self, delta):
        d=delta.dict
        assert len(d.keys()) == 1
        k, v=d.items()[0]
        return "%s.%s=%s;" % (self._js_variable("db", self.json_path(), d), k, v)

# ----------

def write(o):
    sys.stdout.write(str(o))
    #sys.stdout.write("\n")

def mypprint(o, write=write, encoding='utf-8'):
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
        write('"%s"' % o.encode(encoding))
    elif type(o)==type(''):
        write('"%s"' % o)
    elif type(o) in [type(0), type(0L), type(0.3)]:
        write(o)
    elif type(o)==type(True):
        write(str(o).lower())
    elif type(o)==type(None):
        write('null')
    elif type(o)==decimal.Decimal:
        write(str(o))
    else:
        raise ValueError(type(o))

def dump_keynames(dict):
    import StringIO
    f=StringIO.StringIO()
    __dump_keynames(dict, [], f)
    return f.getvalue()

def __dump_keynames(dict, paths, f):
    for k in sorted(dict.keys()):
        v=dict[k]
        f.write(':'.join(paths+[str(k)]) + "\n")
        if type(v)==type({}):
            __dump_keynames(dict[k], paths+[str(k)], f)

