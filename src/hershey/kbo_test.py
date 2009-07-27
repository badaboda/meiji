# vim: fileencoding=utf-8 :
import unittest
import MySQLdb
import types
import datetime
from pprint import pprint as p

import feed
from feed import kbo
import mock
import merge

class UtilTest(unittest.TestCase):
    def test_hierachy_dict(self):
        d=feed.hierachy_dict(['a', 'b'], {})
        self.assertEquals(types.DictType, type(d))
        self.assert_(d.has_key('a'))

class KboFeedTest(unittest.TestCase):
    def setUp(self):
        self.db = feed.SportsDatabase(host='sports-livedb1',
                            user='root', passwd='damman#2',
                            db='kbo', charset='utf8',
                            cursorclass=MySQLdb.cursors.DictCursor)
        self.game_code ='20090701HHSK0'

    def tearDown(self):
        self.db.close()

    def new_datum(self, klass, *args):
        if args:
            return klass(self.db, *args)
        return klass(self.db, self.game_code)

    def bootstrap_dict(self, klass, *args):
        return self.new_datum(klass, *args).as_bootstrap_dict()

    def assertHierachy(self, path, hierachy_dict):
        d=hierachy_dict
        for k in path.split(':'):
            self.assertTrue(d.has_key(unicode(k)), 'key(%s) not found in dict(%s)' % (k, repr(d)))
            d=d[k]

    def merge(self, dicts):
        return reduce(lambda x,y: merge._merge_insert(x, y), dicts)

    def testRegistryPlayerProfile(self):
        self.assertHierachy('registry:player:96441:profile',
                             self.bootstrap_dict(kbo.RegistryPlayerProfile))

    def testRegistryPlayerSeason(self):
        self.assertHierachy('registry:player:72139:batter:season',
                            self.bootstrap_dict(kbo.RegistryPlayerBatterSeason))

    def testScoreBoardForCurrentGame(self):
        code=self.game_code
        specs = [(kbo.ScoreBoard, code),
                   (kbo.ScoreBoardHome, code),
                   (kbo.ScoreBoardAway, code),
                   (kbo.ScoreBoardBases, code),
                   (kbo.ScoreBoardWatingBatters, code),]
        initial_dicts=[self.bootstrap_dict(klass, game_code) for klass, game_code in specs]
        merged=self.merge(initial_dicts)
        #p(merged)
        self.assertHierachy("registry:scoreboard:%s:home" % code, merged)
        self.assertHierachy("registry:scoreboard:%s:away" % code, merged)
        self.assertHierachy("registry:scoreboard:%s:waiting_batters" % code, merged)

    def testScoreboardForLeague(self):
        today_games=kbo.LeagueTodayGames(self.db, self.game_code, datetime.datetime(2009, 07, 11))
        today_games.ensure_rows()
        past_vs_games=kbo.LeaguePastVsGames(self.db, self.game_code)
        past_vs_games.ensure_rows()
        game_codes=today_games.rows+past_vs_games.rows

        initial_dicts=[]
        for game_code in game_codes:
            specs = [(kbo.ScoreBoard, game_code),
                       (kbo.ScoreBoardHome, game_code),
                       (kbo.ScoreBoardAway, game_code),
                       (kbo.ScoreBoardBases, game_code),
                       (kbo.ScoreBoardWatingBatters, game_code),]
            initial_dicts+=[self.bootstrap_dict(klass, game_code) for klass, game_code in specs]
        merged=self.merge(initial_dicts)
        #p(merged)

    def testLeague(self):
        initial_dicts=[
            self.bootstrap_dict(kbo.LeagueTodayGames, self.game_code, datetime.datetime(2009, 07, 11)),
            self.bootstrap_dict(kbo.LeaguePastVsGames, self.game_code)
        ]
        merged=self.merge(initial_dicts)
        #p(merged)
        self.assertHierachy("league:today_games", merged)
        self.assertHierachy("league:past_vs_games", merged)
        self.assertEquals(3, len(merged['league']['today_games']))

    def testRegistryPlayer(self):
        klasses = [kbo.RegistryPlayerProfile,
                    kbo.RegistryPlayerBatterSeason,
                    kbo.RegistryPlayerBatterToday,
                    kbo.RegistryPlayerPitcherToday,
                    kbo.RegistryPlayerPitcherSeason,
                    kbo.RegistryTeamSeason,
                    kbo.RegistryTeamProfile, ]
        initial_dicts=[self.bootstrap_dict(klass) for klass in klasses]
        #p(self.merge(initial_dicts))

    def testLiveTextAndMeta(self):
        klasses = [kbo.Meta, kbo.GameCode]
        initial_dicts=[self.bootstrap_dict(klass) for klass in klasses]
        #p(self.merge(initial_dicts))

    def testScoreboardForNoDataInScheduleTable(self):
        self.assertRaises(feed.NoDataFoundForScoreboardError,
                            self.bootstrap_dict, kbo.ScoreBoard, 'not_exist_gamecode')

    def testScoreboardForNoDataInScheduleTable(self):
        # game_code를 Schedule테이블에는 있되 IE_LiveText 테이블에는 없는 것으로 설정
        self.bootstrap_dict(kbo.ScoreBoard, '20090730HTLT0')

if __name__=='__main__':
    unittest.main()

