# vim: fileencoding=utf-8 :
import unittest
import MySQLdb
import types
import datetime
from pprint import pprint as p

import feed
from feed import kl
import mock
import merge

class KlFeedTest(unittest.TestCase):
    def setUp(self):
        self.db=feed.SportsDatabase(host='sports-livedb1',
                            user='root', passwd='damman#2',
                            db='kl', charset='utf8',
                            cursorclass=MySQLdb.cursors.DictCursor)
        self.game_code='20091114'

    def tearDown(self):
        self.db.close()

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

    def testLiveText(self):
        dict=self.bootstrap_dict(kl.LiveText)
        self.assertHierachy("livetext", dict)
        self.assertTrue(type(dict["livetext"]) == types.ListType)

    def testMetaAndGameCode(self):
        klasses=[kl.Meta, kl.GameCode]
        initial_dicts=[self.bootstrap_dict(klass) for klass in klasses]
        merged=self.merge(initial_dicts)
        self.assertHierachy("meta:live_feed_type_text", merged)
        self.assertEquals(merged["game_code"], self.game_code)

    def testRegistryPlayerProfile(self):
        dict = self.bootstrap_dict(kl.RegistryPlayerProfile)
        #p(dict)
        self.assertHierachy('registry:player:1997035:profile:backnum', dict)

    def testRegistryPlayerSeason(self):
        dict = self.bootstrap_dict(kl.RegistryPlayerSeason)
        #feed.mypprint(dict)
        self.assertHierachy('registry:player:1997035:season', dict)

    def testRegistryTeamCup(self):
        dict = self.bootstrap_dict(kl.RegistryTeamCup)
        #p(dict)
        self.assertHierachy('registry:team:K04:cup', dict)
        self.assertHierachy('registry:team:K04:cup:avg_goal', dict)

    def testRegistryTeamRecentFive(self):
        dict = self.bootstrap_dict(kl.RegistryTeamRecentFive)
        #p(dict)
        self.assertHierachy('registry:team:K01:recent5', dict)

    def testRegistryScoreBoardHome(self):
        dict = self.bootstrap_dict(kl.RegistryScoreBoardHome)
        #p(dict)
        self.assertHierachy('registry:scoreboard:%s:home:tcode' % self.game_code, dict)

    def testRegistryScoreBoardAway(self):
        dict = self.bootstrap_dict(kl.RegistryScoreBoardAway)
        #p(dict)
        self.assertHierachy('registry:scoreboard:%s:away:tcode' % self.game_code, dict)

    def testRegistryScoreBoardGoal(self):
        dict = self.bootstrap_dict(kl.RegistryScoreBoardGoals)
        #p(dict)
        self.assertHierachy('registry:scoreboard:%s:goals' % (self.game_code) , dict)

    def testRegistryScoreBoardGameStatus(self):
        dict=self.bootstrap_dict(kl.RegistryScoreBoardStatus)
        #p(dict)
        self.assertHierachy('registry:scoreboard:%s:status' % (self.game_code),dict)
    
    def testRegistryScoreBoardShotPosistion(self):
        dict=self.bootstrap_dict(kl.RegistryScoreBoardShotPosition)
        #p(dict)
        self.assertHierachy('registry:scoreboard:%s:shot_pos' % (self.game_code),dict)

    def testRegistryOfCurrentGame(self):
        klasses = [
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
        p(merged)


if __name__=='__main__':
    unittest.main()

