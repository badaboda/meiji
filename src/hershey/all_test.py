import unittest
import types
import merge


class FeedTest(unittest.TestCase):
    def new_datum(self, klass, *args):
        if args:
            return klass(self.db, *args)
        return klass(self.db, self.game_code)

    def bootstrap_dict(self, klass, *args):
        dict = self.new_datum(klass, *args).as_bootstrap_dict()
        self.assertTrue(type(dict)==types.DictType)
        return dict

    def assertHierachy(self, path, hierachy_dict):
        d=hierachy_dict
        for k in path.split(':'):
            self.assertTrue(d.has_key(unicode(k)), 'key(%s) not found in dict(%s)' % (k, repr(d)))
            d=d[k]

    def merge(self, dicts):
        return reduce(lambda x,y: merge._merge_insert(x, y), dicts)


if __name__=='__main__':
    suite=unittest.TestLoader().loadTestsFromNames([
        'kbo_test',
        'npb_test',
        'mlb_test',
        'kl_test',
        'delta_test',
    ])
    unittest.TextTestRunner(verbosity=2).run(suite)
