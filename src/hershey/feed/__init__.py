# vim: et sts=4 sw=4 ts=4 ai fileencoding=utf8 :
import MySQLdb
import MySQLdb.cursors

import types
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
    def feed(self, tag, json_path, dict):
        if tag in ['insert', 'replace']:
            print self.insert_javascript(json_path, dict)
        elif tag=='delete':
            print self.delete_javascript(json_path, dict)
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

