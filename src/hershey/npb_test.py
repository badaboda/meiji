# vim: fileencoding=utf-8 :
import unittest
import types
import datetime
from pprint import pprint as p
import MySQLdb

import feed
from feed import npb
import mock

from all_test import FeedTest

class NpbFeedTest(FeedTest):
    def setUp(self):
        self.db = feed.SportsDatabase(host='sports-livedb1',
                            user='root', passwd='damman#2',
                            db='npb', charset='utf8',
                            cursorclass=MySQLdb.cursors.DictCursor)
        self.game_code ='2009072101'

    def tearDown(self):
        self.db.close()


    def testRegistryPlayerProfile(self):
        self.assertHierachy('registry:player:700014:profile',
                             self.bootstrap_dict(npb.RegistryPlayerProfile))

    def testRegistryPlayerSeason(self):
        d=self.bootstrap_dict(npb.RegistryPlayerBatterSeason)
        self.assertHierachy('registry:player:11983:batter:season', d)

        season_dict=d['registry']['player']['11983']['batter']['season']
        self.assertFalse(season_dict.has_key('game_flag'))
        self.assertFalse(season_dict.has_key('pa_flag'))


    def testScoreBoardForCurrentGame(self):
        code=self.game_code
        specs = [(npb.ScoreBoard, code),
                   (npb.ScoreBoardHome, code),
                   (npb.ScoreBoardAway, code),
                   (npb.ScoreBoardBases, code),
                   (npb.ScoreBoardWatingBatters, code),]
        initial_dicts=[self.bootstrap_dict(klass, game_code) for klass, game_code in specs]
        merged=self.merge(initial_dicts)
        #p(merged)
        self.assertHierachy("registry:scoreboard:%s:home" % code, merged)
        self.assertHierachy("registry:scoreboard:%s:away" % code, merged)
        self.assertHierachy("registry:scoreboard:%s:waiting_batters" % code, merged)

    def testRegistryTeamSeason(self):
        dict=self.bootstrap_dict(npb.RegistryTeamSeason, self.game_code)
        self.assertFalse(dict['registry']['team'].has_key(None))

    def testRegistryPlayer(self):
        klasses = [npb.RegistryPlayerProfile,
                    npb.RegistryPlayerBatterSeason,
                    npb.RegistryPlayerBatterToday,
                    npb.RegistryPlayerPitcherToday,
                    npb.RegistryPlayerPitcherSeason,
                    npb.RegistryTeamSeason,
                    npb.RegistryTeamProfile, ]
        initial_dicts=[self.bootstrap_dict(klass) for klass in klasses]
        #p(self.merge(initial_dicts))

    def testMetaAndGameCode(self):
        klasses = [npb.Meta, npb.GameCode]
        initial_dicts=[self.bootstrap_dict(klass) for klass in klasses]
        #p(self.merge(initial_dicts))

    def testScoreBoardHomeLineupBatter(self):
        dict=self.bootstrap_dict(npb.ScoreBoardHomeLineupBatter, self.game_code)
        json_path="registry:scoreboard:%s:home:lineup:batter" % self.game_code
        self.assertHierachy(json_path, dict)
        #p(dict)
        self.assertEquals("600052", self.fetchHierachy(json_path, dict)[0]['pcode'])

    def testScoreBoardAwayLineupBatter(self):
        dict=self.bootstrap_dict(npb.ScoreBoardAwayLineupBatter, self.game_code)
        json_path="registry:scoreboard:%s:away:lineup:batter" % self.game_code
        self.assertHierachy(json_path, dict)
        self.assertEquals("11983", self.fetchHierachy(json_path, dict)[0]['pcode'])

    def testScoreBoardHomeLineupPitcher(self):
        dict=self.bootstrap_dict(npb.ScoreBoardHomeLineupPitcher, self.game_code)
        json_path="registry:scoreboard:%s:home:lineup:pitcher" % self.game_code
        self.assertHierachy(json_path, dict)
        p(dict)
        self.assertEquals("600051", self.fetchHierachy(json_path, dict)[0]['pcode'])

    def testScoreBoardAwayLineupPitcher(self):
        dict=self.bootstrap_dict(npb.ScoreBoardAwayLineupPitcher, self.game_code)
        json_path="registry:scoreboard:%s:away:lineup:pitcher" % self.game_code
        self.assertHierachy(json_path, dict)
        p(dict)
        self.assertEquals("12103", self.fetchHierachy(json_path, dict)[0]['pcode'])

if __name__=='__main__':
    unittest.main()

