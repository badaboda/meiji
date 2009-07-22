# vim: fileencoding=utf-8 :
import unittest
import MySQLdb
import types
import datetime
from pprint import pprint as p

import feed
import mock
import merge

class UtilTest(unittest.TestCase):
    def test_hierachy_dict(self):
        d=feed.hierachy_dict(['a', 'b'], {})
        self.assertEquals(types.DictType, type(d))
        self.assert_(d.has_key('a'))

class FeedAsBootstrapDictTest(unittest.TestCase):
    def setUp(self):
        self.db = feed.SportsDatabase(host='sports-livedb1', 
                            user='root', passwd='damman#2',
                            db='kbo', charset='utf8',
                            cursorclass=MySQLdb.cursors.DictCursor)
        self.game_code ='20090701HHSK0'

    def tearDown(self):
        self.db.close()

    def new_datum(self, klass):
        return klass(self.db, self.game_code)

    def new_scoreboard_datum(self, klass, game_code):
        return klass(self.db, game_code)

    def assertHierachy(self, path, hierachy_dict):
        d=hierachy_dict
        for k in path.split(':'):
            self.assertTrue(d.has_key(unicode(k)), 'key(%s) not found in dict(%s)' % (k, repr(d)))
            d=d[k]

    def merge(self, dicts):
        return reduce(lambda x,y: merge._merge_insert(x, y), dicts)

    def testRegistryPlayerProfile(self):
        self.assertHierachy('registry:player:96441:profile', 
                             self.new_datum(feed.RegistryPlayerProfile).as_bootstrap_dict())
        
    def testRegistryPlayerSeason(self):
        self.assertHierachy('registry:player:72139:batter:season', 
                            self.new_datum(feed.RegistryPlayerBatterSeason).as_bootstrap_dict())

    def testScoreBoardForCurrentGame(self):
        code=self.game_code
        specs = [(feed.ScoreBoard, code),
                   (feed.ScoreBoardHome, code), 
                   (feed.ScoreBoardAway, code),
                   (feed.ScoreBoardBases, code), 
                   (feed.ScoreBoardWatingBatters, code),]
        initial_dicts=[self.new_scoreboard_datum(klass, game_code).as_bootstrap_dict() for klass, game_code in specs]
        merged=self.merge(initial_dicts)
        #p(merged)
        self.assertHierachy("registry:scoreboard:%s:home" % code, merged)
        self.assertHierachy("registry:scoreboard:%s:away" % code, merged)
        self.assertHierachy("registry:scoreboard:%s:waiting_batters" % code, merged)

    def testScoreboardForLeague(self):
        today_games=feed.LeagueTodayGames(self.db, self.game_code, datetime.datetime(2009, 07, 11))
        today_games.ensure_rows()
        past_vs_games=feed.LeaguePastVsGames(self.db, self.game_code)
        past_vs_games.ensure_rows()
        game_codes=today_games.rows+past_vs_games.rows
        
        initial_dicts=[]
        for game_code in game_codes:
            specs = [(feed.ScoreBoard, game_code),
                       (feed.ScoreBoardHome, game_code), 
                       (feed.ScoreBoardAway, game_code),
                       (feed.ScoreBoardBases, game_code), 
                       (feed.ScoreBoardWatingBatters, game_code),]
            initial_dicts+=[self.new_scoreboard_datum(klass, game_code).as_bootstrap_dict() for klass, game_code in specs]
        merged=self.merge(initial_dicts)
        #p(merged)

    def testLeague(self):
        initial_dicts=[
            feed.LeagueTodayGames(self.db, self.game_code, datetime.datetime(2009, 07, 11)).as_bootstrap_dict(), 
            feed.LeaguePastVsGames(self.db, self.game_code).as_bootstrap_dict()
        ]
        merged=self.merge(initial_dicts)
        #p(merged)
        self.assertHierachy("league:today_games", merged)
        self.assertHierachy("league:past_vs_games", merged)
        self.assertEquals(3, len(merged['league']['today_games']))

    def testRegistryPlayer(self):
        klasses = [feed.RegistryPlayerProfile,
                    feed.RegistryPlayerBatterSeason,
                    feed.RegistryPlayerBatterToday,
                    feed.RegistryPlayerPitcherToday, 
                    feed.RegistryPlayerPitcherSeason, 
                    feed.RegistryTeamSeason, 
                    feed.RegistryTeamProfile, ]
        initial_dicts=[self.new_datum(klass).as_bootstrap_dict() for klass in klasses]
        #p(self.merge(initial_dicts))

    def testLiveTextAndMeta(self):
        klasses = [feed.Meta, feed.GameCode]
        initial_dicts=[self.new_datum(klass).as_bootstrap_dict() for klass in klasses]
        #p(self.merge(initial_dicts))
        

class DeltaGeneratorTest(unittest.TestCase):
    def setUp(self):
        self.consumer = mock.MockJavascriptConsumer()
        self.delta=feed.DeltaGenerator(self.consumer)
        self.JSON_PATH="registry:players:**pcode:profile"

    def tearDown(self):
        pass

    def test_insert(self):
        self.delta.feed(mock.MockDatum(self.JSON_PATH, [{ "pcode": "79260", "name": "장원삼" }]))
        self.delta.feed(mock.MockDatum(self.JSON_PATH, [{ "pcode": "79260", "name": "장원삼" },
                                                        { "pcode": "98260", "name": "정삼흠" }]))
        self.assertEquals(
            ["db.registry.players['98260'].profile={'pcode': '98260', 'name': %s};"%repr('정삼흠')], 
            self.consumer.lst)

    def test_delete(self):
        self.delta.feed(mock.MockDatum(self.JSON_PATH, [{ "pcode": "79260", "name": "장원삼" },
                                                        { "pcode": "98260", "name": "정삼흠" }]))
        self.delta.feed(mock.MockDatum(self.JSON_PATH, [{ "pcode": "79260", "name": "장원삼" }]))
        self.assertEquals(
            ["delete db.registry.players['98260'].profile;"], 
            self.consumer.lst)

    def test_replace(self):
        self.delta.feed(mock.MockDatum(self.JSON_PATH, [{ "pcode": "79260", "name": "장원삼" },
                                                        { "pcode": "98260", "name": "정삼흠" }]))
        self.delta.feed(mock.MockDatum(self.JSON_PATH, [{ "pcode": "79260", "name": "장원삼2" },
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
        d=feed.RegistryPlayerProfile(self.db, self.game_code)
        self.delta.feed(d)

        d=feed.RegistryPlayerProfile(self.db, self.game_code)
        d.ensure_rows()
        d.rows[0]['name']='xxx'
        self.delta.feed(d)
        
        self.assert_(len(self.consumer.lst) > 0)

    def testAnotherKindFeedShouldNotGenerateDelta(self):
        d=feed.RegistryPlayerProfile(self.db, self.game_code)
        self.delta.feed(d)

        d=feed.RegistryPlayerBatterSeason(self.db, self.game_code)
        d.ensure_rows()
        self.delta.feed(d)
        
        self.assertEquals(0, len(self.consumer.lst))

    def testAllDatumCouldCallAsDeltaGeneratorInput(self):
        for klass in feed.datums+feed.league_datums+feed.scoreboard_datums:
            try:
                klass(self.db, self.game_code).as_delta_generator_input()
            except ValueError:
                raise ValueError('%s does not support as_delta_generator_input' % repr(klass))

    def testDeltaLoop(self):
        import delta 
        delta.loop(self.delta, self.game_code)
        delta.loop(self.delta, self.game_code)

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
        
