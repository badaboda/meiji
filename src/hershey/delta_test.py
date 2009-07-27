# vim: fileencoding=utf-8 :
import unittest
import feed
from feed import kbo
import mock
import merge
import MySQLdb

class DeltaTest(unittest.TestCase):
    def setUp(self):
        self.game_code ='20090701HHSK0'
        self.consumer = mock.MockJavascriptConsumer()
        self.delta=feed.DeltaGenerator(self.consumer)

    def testDeltaLoop(self):
        import kbo_delta
        kbo_delta.loop(self.delta, self.game_code)
        kbo_delta.loop(self.delta, self.game_code)

class DeltaGeneratorWithLiveTextFromDbTest(unittest.TestCase):
    def setUp(self):
        self.db = feed.SportsDatabase(host='localhost',
                            user='root', passwd='',
                            db='kbo_test', charset='utf8',
                            cursorclass=MySQLdb.cursors.DictCursor)
        self.game_code ='20090701HHSK0'
        self.consumer = mock.MockJavascriptConsumer()
        self.delta=feed.DeltaGenerator(self.consumer)

    def tearDown(self):
        self.db.close()

    def testLiveText(self):
        params={ 'game_code': self.game_code, 'seq': 305 }
        self.db.execute("""
            delete from IE_LiveText where gameid = '%(game_code)s'  AND SeqNO = %(seq)d;
        """ % params)

        datum=kbo.LiveText(self.db, self.game_code)
        self.delta.feed(datum)

        self.db.execute("""
            insert IE_LiveText (gameID, LiveText, SeqNO, Inning, bTop, textStyle) values('%(game_code)s','%(seq)d',%(seq)d,6,0,1);
        """ % params)

        datum=kbo.LiveText(self.db, self.game_code)
        self.delta.feed(datum)

        self.assertEquals(
            ["db.livetext.push({'inning': 6, 'textstyle': 1, 'text': u'305', 'seq': 305, 'btop': 0});"], self.consumer.lst)


class DeltaGeneratorTest(unittest.TestCase):
    def setUp(self):
        self.consumer = mock.MockJavascriptConsumer()
        self.delta=feed.DeltaGenerator(self.consumer)
        self.KEYED_JSON_PATH="registry:players:**pcode:profile"

    def tearDown(self):
        pass

    def test_insert_keyed_datum(self):
        self.delta.feed(mock.MockDatum(self.KEYED_JSON_PATH, [{ "pcode": "79260", "name": "장원삼" }]))
        self.delta.feed(mock.MockDatum(self.KEYED_JSON_PATH, [{ "pcode": "79260", "name": "장원삼" },
                                                              { "pcode": "98260", "name": "정삼흠" }]))
        self.assertEquals(
            ["db.registry.players['98260'].profile={'pcode': '98260', 'name': %s};"%repr('정삼흠')],
            self.consumer.lst)

    def test_insert_list_datum(self):
        self.delta.feed(mock.MockDatumAsList("livetext", [{ "a": 1, "b": 2 }]))
        self.delta.feed(mock.MockDatumAsList("livetext", [{ "a": 1, "b": 2 },
                                                          { "a": 2, "b" : 2}]))
        self.assertEquals(
            ["db.livetext.push({'a': 2, 'b': 2});"], self.consumer.lst)


    def test_delete(self):
        self.delta.feed(mock.MockDatum(self.KEYED_JSON_PATH, [{ "pcode": "79260", "name": "장원삼" },
                                                        { "pcode": "98260", "name": "정삼흠" }]))
        self.delta.feed(mock.MockDatum(self.KEYED_JSON_PATH, [{ "pcode": "79260", "name": "장원삼" }]))
        self.assertEquals(
            ["delete db.registry.players['98260'].profile;"],
            self.consumer.lst)

    def test_replace(self):
        self.delta.feed(mock.MockDatum(self.KEYED_JSON_PATH, [{ "pcode": "79260", "name": "장원삼" },
                                                        { "pcode": "98260", "name": "정삼흠" }]))
        self.delta.feed(mock.MockDatum(self.KEYED_JSON_PATH, [{ "pcode": "79260", "name": "장원삼2" },
                                                        { "pcode": "98260", "name": "정삼흠" }]))
        self.assertEquals(
            ["db.registry.players['79260'].profile={'pcode': '79260', 'name': %s};"%repr('장원삼2')],
            self.consumer.lst)


class FeedAsDeltaGeneratorInput(unittest.TestCase):
    def setUp(self):
        self.db = feed.SportsDatabase(host='sports-livedb1',
                            user='root', passwd='damman#2',
                            db='kbo', charset='utf8',
                            cursorclass=MySQLdb.cursors.DictCursor)
        self.game_code ='20090701HHSK0'

        self.consumer = mock.MockJavascriptConsumer()
        self.delta=feed.DeltaGenerator(self.consumer)

    def testRegistryPlayerProfile(self):
        d=kbo.RegistryPlayerProfile(self.db, self.game_code)
        self.delta.feed(d)

        d=kbo.RegistryPlayerProfile(self.db, self.game_code)
        d.ensure_rows()
        d.rows[0]['name']='xxx'
        self.delta.feed(d)

        self.assert_(len(self.consumer.lst) > 0)

    def testAnotherKindFeedShouldNotGenerateDelta(self):
        d=kbo.RegistryPlayerProfile(self.db, self.game_code)
        self.delta.feed(d)

        d=kbo.RegistryPlayerBatterSeason(self.db, self.game_code)
        d.ensure_rows()
        self.delta.feed(d)

        self.assertEquals(0, len(self.consumer.lst))

    def testAllDatumCouldCallAsDeltaGeneratorInput(self):
        for klass in kbo.datums+kbo.league_datums+kbo.scoreboard_datums:
            try:
                klass(self.db, self.game_code).as_delta_generator_input()
            except ValueError:
                raise ValueError('%s does not support as_delta_generator_input' % repr(klass))

class DictMergeTest(unittest.TestCase):
    def test_merge_insert(self):
        initial_data = \
            { "registry":
                { "players":
                    { "79260":
                        { "profile":
                            { "name": "장원삼" }}}}}
        delta_spec = \
             ('insert',
               { "registry":
                 { "players":
                   { "98260":
                     { "profile":
                        { "name": "정삼흠" }}}}})

        n=merge.merge(initial_data, delta_spec)
        self.assertEquals(
            { "registry":
                { "players":
                    { "79260": { "profile":
                                   { "name": "장원삼" }},
                      "98260": { "profile":
                                   { "name": "정삼흠" }}}}}, n)

    def test_merge_delete(self):
        initial_data = \
            { "registry":
                { "players":
                    { "79260":
                        { "profile":
                            { "name": "장원삼" }},
                      "98260":
                        { "profile":
                            { "name": "정삼흠" }}}}}
        delta_spec = \
             ('delete',
               { "registry":
                 { "players":
                   { "79260":
                     { "profile":
                        { "name": "정삼흠" }}}}})

        n=merge.merge(initial_data, delta_spec)
        self.assertEquals(
            { "registry":
                { "players":
                    { "79260": {},
                      "98260": { "profile":
                                   { "name": "정삼흠" }}}}}, n)


if __name__=='__main__':
    unittest.main()
