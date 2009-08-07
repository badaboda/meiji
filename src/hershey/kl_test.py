# vim: fileencoding=utf-8 :
import unittest, types
from pprint import pprint as p

import feed, mock, config
from feed import kl

from all_test import FeedTest


class KlFeedTest(FeedTest):
    def setUp(self):
        self.db=feed.SportsDatabase(db='kl', **config.sports_live_db1_credential)
        self.game_code='20091114'

    def tearDown(self):
        self.db.close()

    def testLiveText(self):
        dict=self.bootstrap_dict(kl.LiveText)
        #feed.mypprint(dict)
        self.assertHierachy("livetext", dict)
        self.assertTrue(type(dict["livetext"]) == types.ListType)

    def testMetaAndGameCode(self):
        klasses=[kl.Meta, kl.GameCode]
        initial_dicts=[self.bootstrap_dict(klass) for klass in klasses]
        #self.assertHierachy("game_code",initial_dicts)
        merged=self.merge(initial_dicts)
        #feed.mypprint(merged)
        #p(merged)
        self.assertHierachy("meta:live_feed_type_text", merged)
        self.assertEquals(merged["game_code"], self.game_code)

    def testRegistryPlayerProfile(self):
        dict = self.bootstrap_dict(kl.RegistryPlayerProfile)
        #feed.mypprint(dict)
        #p(dict)
        self.assertHierachy('registry:player:1997035:profile:backnum', dict)

    def testRegistryPlayerSeason(self):
        dict = self.bootstrap_dict(kl.RegistryPlayerSeason)
        #feed.mypprint(dict)
        self.assertHierachy('registry:player:1997035:season', dict)

    def testRegistryTeamCup(self):
        dict = self.bootstrap_dict(kl.RegistryTeamCup)
        #feed.mypprint(dict)
        #p(dict)
        self.assertHierachy('registry:team:K04:cup', dict)
        self.assertHierachy('registry:team:K04:cup:avg_goal', dict)

    def testRegistryTeamRecentFive(self):
        dict = self.bootstrap_dict(kl.RegistryTeamRecentFive)
        #feed.mypprint(dict)
        #p(dict)
        self.assertHierachy('registry:team:K01:recent5', dict)

    def testRegistryScoreBoardHome(self):
        dict = self.bootstrap_dict(kl.RegistryScoreBoardHome)
        #feed.mypprint(dict)
        #p(dict)
        self.assertHierachy('registry:scoreboard:%s:home:tcode' % self.game_code, dict)

    def testRegistryScoreBoardAway(self):
        dict = self.bootstrap_dict(kl.RegistryScoreBoardAway)
        #feed.mypprint(dict)
        #p(dict)
        self.assertHierachy('registry:scoreboard:%s:away:tcode' % self.game_code, dict)

    def testRegistryScoreBoardGoal(self):
        dict = self.bootstrap_dict(kl.RegistryScoreBoardGoals)
        #feed.mypprint(dict)
        #p(dict)
        self.assertHierachy('registry:scoreboard:%s:goals' % (self.game_code) , dict)

    def testRegistryScoreBoardGameStatus(self):
        dict=self.bootstrap_dict(kl.RegistryScoreBoardStatus)
        #feed.mypprint(dict)
        #p(dict)
        self.assertHierachy('registry:scoreboard:%s:status' % (self.game_code),dict)
    
    def testRegistryScoreBoardShotPosistion(self):
        dict=self.bootstrap_dict(kl.RegistryScoreBoardShotPosition)
        #feed.mypprint(dict)
        #p(dict)
        self.assertHierachy('registry:scoreboard:%s:shot_pos' % (self.game_code),dict)

    def testRegistryOfCurrentGame(self):
        klasses = [
                    kl.Meta,
                    kl.GameCode, 
                    kl.RegistryPlayerProfile, 
                    kl.RegistryPlayerSeason, 
                    kl.RegistryTeamCup, 
                    kl.RegistryScoreBoardHome, 
                    kl.RegistryScoreBoardAway, 
                    kl.RegistryScoreBoardGoals, 
                    kl.RegistryScoreBoardStatus,
                    kl.RegistryScoreBoardShotPosition
                    ]
        dicts=[self.bootstrap_dict(klass) for klass in klasses]
        merged=self.merge(dicts)
        #p(merged)

class klDeltaLoopTest(unittest.TestCase):
    def setUp(self):
        self.game_code = '20091114'
        self.consumer=mock.MockJavascriptConsumer()
        self.delta=feed.DeltaGenerator(self.consumer)

    def testDeltaLoop(self):
        import kl_delta
        kl_delta.loop(self.delta, self.game_code)
        kl_delta.loop(self.delta, self.game_code)

if __name__=='__main__':
    unittest.main()

