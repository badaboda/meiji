# vim: fileencoding=utf-8 :
import unittest
import MySQLdb
import types
import datetime
from pprint import pprint as p

import feed
from feed import mlb
import mock
import merge

from all_test import FeedTest

class FeedAsBootstrapDictTest(FeedTest):
    def setUp(self):
        self.db = feed.SportsDatabase(host='sports-livedb1',
                            user='root', passwd='damman#2',
                            db='mlb', charset='utf8',
                            cursorclass=MySQLdb.cursors.DictCursor)
        self.game_code ='20090723CCPH0'

    def tearDown(self):
        self.db.close()

    def testRegistryPlayerProfile(self):
        self.assertHierachy('registry:player:10002:profile',
                             self.bootstrap_dict(mlb.RegistryPlayerProfile))

    def testRegistryPlayerSeason(self):
        self.assertHierachy('registry:player:10002:batter:season',
                            self.bootstrap_dict(mlb.RegistryPlayerBatterSeason))

    def testScoreBoardForCurrentGame(self):
        code=self.game_code
        specs = [(mlb.ScoreBoard, code),
                   (mlb.ScoreBoardHome, code),
                   (mlb.ScoreBoardAway, code),
                   (mlb.ScoreBoardBases, code),
                   (mlb.ScoreBoardWatingBatters, code),]
        initial_dicts=[self.bootstrap_dict(klass, game_code) for klass, game_code in specs]
        merged=self.merge(initial_dicts)
        #p(merged)
        self.assertHierachy("registry:scoreboard:%s:home" % code, merged)
        self.assertHierachy("registry:scoreboard:%s:away" % code, merged)
        self.assertHierachy("registry:scoreboard:%s:waiting_batters" % code, merged)

    def testScoreboardForLeague(self):
        today_games=mlb.LeagueTodayGames(self.db, self.game_code, datetime.datetime(2009, 07, 11))
        today_games.ensure_rows()
        past_vs_games=mlb.LeaguePastVsGames(self.db, self.game_code)
        past_vs_games.ensure_rows()
        game_codes=today_games.rows+past_vs_games.rows

        initial_dicts=[]
        for game_code in game_codes:
            specs = [(mlb.ScoreBoard, game_code),
                       (mlb.ScoreBoardHome, game_code),
                       (mlb.ScoreBoardAway, game_code),
                       (mlb.ScoreBoardBases, game_code),
                       (mlb.ScoreBoardWatingBatters, game_code),]
            for klass, game_code in specs:
                try:
                    initial_dicts+=self.bootstrap_dict(klass, game_code)
                except feed.NoDataFoundForScoreboardError:
                    # 한국선수가 뛰지 않는 경기는
                    #  MLB_ScoreRHEB, MLB_LiveText에 row가 존재하지 않는다
                    pass
        merged=self.merge(initial_dicts)
        #p(merged)

    def testScoreBoardHomeLineupBatter(self):
        dict=self.bootstrap_dict(mlb.ScoreBoardHomeLineupBatter, self.game_code)
        json_path="registry:scoreboard:%s:home:lineup:batter" % self.game_code
        self.assertHierachy(json_path, dict)
        #p(dict)
        self.assertEquals("1848162", self.fetchHierachy(json_path, dict)[0]['pcode'])

    def testScoreBoardAwayLineupBatter(self):
        dict=self.bootstrap_dict(mlb.ScoreBoardAwayLineupBatter, self.game_code)
        json_path="registry:scoreboard:%s:away:lineup:batter" % self.game_code
        self.assertHierachy(json_path, dict)
        self.assertEquals("10765", self.fetchHierachy(json_path, dict)[0]['pcode'])

    def testScoreBoardHomeLineupPitcher(self):
        dict=self.bootstrap_dict(mlb.ScoreBoardHomeLineupPitcher, self.game_code)
        json_path="registry:scoreboard:%s:home:lineup:pitcher" % self.game_code
        self.assertHierachy(json_path, dict)
        p(dict)
        self.assertEquals("12342", self.fetchHierachy(json_path, dict)[0]['pcode'])

    def testScoreBoardAwayLineupPitcher(self):
        dict=self.bootstrap_dict(mlb.ScoreBoardAwayLineupPitcher, self.game_code)
        json_path="registry:scoreboard:%s:away:lineup:pitcher" % self.game_code
        self.assertHierachy(json_path, dict)
        p(dict)
        self.assertEquals("10931", self.fetchHierachy(json_path, dict)[0]['pcode'])

    def testLeague(self):
        initial_dicts=[
            self.bootstrap_dict(mlb.LeagueTodayGames, self.game_code, datetime.datetime(2009, 07, 11)),
            self.bootstrap_dict(mlb.LeaguePastVsGames, self.game_code)
        ]
        merged=self.merge(initial_dicts)
        #p(merged)
        self.assertHierachy("league:today_games", merged)
        self.assertHierachy("league:past_vs_games", merged)
        self.assertEquals(15, len(merged['league']['today_games']))

    def testRegistryPlayer(self):
        klasses = [mlb.RegistryPlayerProfile,
                    mlb.RegistryPlayerBatterSeason,
                    mlb.RegistryPlayerBatterToday,
                    mlb.RegistryPlayerPitcherToday,
                    mlb.RegistryPlayerPitcherSeason,
                    mlb.RegistryTeamSeason,
                    mlb.RegistryTeamProfile, ]
        initial_dicts=[self.bootstrap_dict(klass) for klass in klasses]
        #p(self.merge(initial_dicts))

if __name__=='__main__':
    unittest.main()
