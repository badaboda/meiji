# vim: fileencoding=utf-8 :
import unittest, datetime

import feed, mock, config
from feed import kbo
from all_test import FeedTest


class UtilTest(unittest.TestCase):
    def test_hierachy_dict(self):
        a_dict = {}
        d=feed.hierachy_dict(['a', 'b'], a_dict)
        self.assertEquals(d['a']['b'], a_dict)

    def test_dump_keynames(self):
        a_dict = { 'a': { 'b': 0, 'c': { 'd': 'string' }}}
        self.assertEquals(
"""a
a:b
a:c
a:c:d
"""
        , feed.dump_keynames(a_dict))

    def test_join_with_none(self):
        self.assertRaises(TypeError, lambda: ''.join(['a', None]), )

class KboFeedTest(FeedTest):
    def setUp(self):
        self.db = feed.SportsDatabase(db='kbo', **config.sports_live_db1_credential)
        self.game_code ='20090701HHSK0'

    def tearDown(self):
        self.db.close()

    def testRegistryPlayerProfile(self):
        self.assertHierachy('registry:player:96441:profile',
                             self.bootstrap_dict(kbo.RegistryPlayerProfile))

    def testRegistryPlayerSeason(self):
        d=self.bootstrap_dict(kbo.RegistryPlayerBatterSeason)
        self.assertHierachy('registry:player:72139:batter:season', d)
        self.assertHierachy('registry:player:72139:batter:season:bbhp', d)

        season_dict=d['registry']['player']['72139']['batter']['season']
        self.assertFalse(season_dict.has_key('bb'))
        self.assertFalse(season_dict.has_key('hp'))
        self.assertFalse(season_dict.has_key('sf'))
        self.assertFalse(season_dict.has_key('sh'))
        self.assertFalse(season_dict.has_key('err'))
        self.assertFalse(season_dict.has_key('gamenum'))
        self.assertHierachy('registry:player:72139:batter:season:bbhp', d)
        self.assertHierachy('registry:player:72139:batter:season:shf', d)
        self.assertEquals('HH', season_dict['tcode'])

    def testScoreBoardForCurrentGame(self):
        code=self.game_code
        specs = [(kbo.ScoreBoard, code),
                   (kbo.ScoreBoardHome, code),
                   (kbo.ScoreBoardAway, code),
                   (kbo.ScoreBoardBases, code),
                   (kbo.ScoreBoardWatingBatters, code),]
        initial_dicts=[self.bootstrap_dict(klass, game_code) for klass, game_code in specs]
        merged=self.merge(initial_dicts)
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

    def testLeague(self):
        initial_dicts=[
            self.bootstrap_dict(kbo.LeagueTodayGames, self.game_code, datetime.datetime(2009, 07, 11)),
            self.bootstrap_dict(kbo.LeaguePastVsGames, self.game_code)
        ]
        merged=self.merge(initial_dicts)
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

    def testMetaAndGameCode(self):
        klasses = [kbo.Meta, kbo.GameCode]
        initial_dicts=[self.bootstrap_dict(klass) for klass in klasses]

    def testScoreboardForNoDataInScheduleTable(self):
        self.assertRaises(feed.NoDataFoundForScoreboardError,
                            self.bootstrap_dict, kbo.ScoreBoard, 'not_exist_gamecode')

    def testScoreboardForNoDataInScheduleTable(self):
        # game_code를 Schedule테이블에는 있되 IE_LiveText 테이블에는 없는 것으로 설정
        self.bootstrap_dict(kbo.ScoreBoard, '20090730HTLT0')

    def testScoreBoardHomeLineupBatter(self):
        self.assertScoreBoardHomeOrAwayLineUp(
            kbo.ScoreBoardHomeLineupBatter,
            "registry:scoreboard:%s:home:lineup:batter",
            "77609")

    def testScoreBoardAwayLineupBatter(self):
        self.assertScoreBoardHomeOrAwayLineUp(
            kbo.ScoreBoardAwayLineupBatter,
            "registry:scoreboard:%s:away:lineup:batter",
            "72139")

    def testScoreBoardHomeLineupPitcher(self):
        self.assertScoreBoardHomeOrAwayLineUp(
            kbo.ScoreBoardHomeLineupPitcher,
            "registry:scoreboard:%s:home:lineup:pitcher",
            "74857")

    def testScoreBoardAwayLineupPitcher(self):
        self.assertScoreBoardHomeOrAwayLineUp(
            kbo.ScoreBoardAwayLineupPitcher,
            "registry:scoreboard:%s:away:lineup:pitcher",
            "93715")


class FeedAsDeltaGeneratorInput(unittest.TestCase):
    def setUp(self):
        self.db = feed.SportsDatabase(db='kbo', **config.sports_live_db1_credential)
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

class KboDeltaLoopTest(unittest.TestCase):
    def setUp(self):
        self.game_code ='20090701HHSK0'
        self.consumer = mock.MockJavascriptConsumer()
        self.delta=feed.DeltaGenerator(self.consumer)
        self.db = feed.SportsDatabase(db='kbo', **config.sports_live_db1_credential)

    def testDeltaLoop(self):
        import kbo_delta
        kbo_delta.loop(self.db, self.delta, self.game_code)
        kbo_delta.loop(self.db, self.delta, self.game_code)

if __name__=='__main__':
    unittest.main()

