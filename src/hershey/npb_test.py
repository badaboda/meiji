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
        self.assertHierachy('registry:player:11983:batter:season',
                            self.bootstrap_dict(npb.RegistryPlayerBatterSeason))

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

    def testRegistryPlayer(self):
        klasses = [npb.RegistryPlayerProfile,
                    npb.RegistryPlayerBatterSeason,
                    npb.RegistryPlayerBatterToday,
                    npb.RegistryPlayerPitcherToday,
                    npb.RegistryPlayerPitcherSeason,
                    npb.RegistryTeamSeason,
                    npb.RegistryTeamProfile, ]
        initial_dicts=[self.bootstrap_dict(klass) for klass in klasses]
        p(self.merge(initial_dicts))

    def testMetaAndGameCode(self):
        klasses = [npb.Meta, npb.GameCode]
        initial_dicts=[self.bootstrap_dict(klass) for klass in klasses]
        #p(self.merge(initial_dicts))

if __name__=='__main__':
    unittest.main()

