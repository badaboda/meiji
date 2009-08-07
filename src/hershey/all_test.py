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

    def fetchHierachy(self, path, hierachy_dict):
        d=hierachy_dict
        for k in path.split(':'):
            self.assertTrue(d.has_key(unicode(k)), 'key(%s) not found in dict(%s)' % (k, repr(d)))
            d=d[k]
        return d

    def assertHierachy(self, path, hierachy_dict):
        self.fetchHierachy(path, hierachy_dict)

    def merge(self, dicts):
        return reduce(lambda x,y: merge._merge_insert(x, y), dicts)

    def assertScoreBoardHomeOrAwayLineUp(self, klass, expected_json_path, expected_first_pcode):
        dict=self.bootstrap_dict(klass, self.game_code)
        json_path=expected_json_path % self.game_code
        self.assertHierachy(json_path, dict)
        self.assertEquals(expected_first_pcode, self.fetchHierachy(json_path, dict)[0]['pcode'])


if __name__=='__main__':
    suite=unittest.TestLoader().loadTestsFromNames([
        'kbo_test',
        'npb_test',
        'mlb_test',
        'kl_test',
        'delta_test',
    ])
    unittest.TextTestRunner(verbosity=2).run(suite)
