import os, sys, subprocess

import kbo_bootstrap_json
import feed, config
import feed.kbo
import feed.npb


class DumpBase(object):
    def __init__(self, credential,  klass_name, league, dump_fn=feed.dump_keynames):
        db_name, game_code=league.dbname, league.gamecode
        db=feed.SportsDatabase(db=db_name, **config.sports_live_db1_credential)
        module_name='feed.%s' % db_name
        __import__(module_name); m=sys.modules[module_name]
        bs=getattr(m, klass_name)(db, game_code).as_bootstrap_dict()
        player=self.sampling(bs)

        self._fn='%s_%s.tmp' % (db_name, klass_name)
        open(self._fn, 'w').write(dump_fn(player))

    def fn(self):
        return self._fn

class DumpPlayer(DumpBase):
    def sampling(self, dict):
        players=dict['registry']['player']
        assert(players.keys() != [])
        return players[players.keys()[0]]

class DumpTeam(DumpBase):
    def sampling(self, dict):
        players=dict['registry']['team']
        return players[players.keys()[0]]

class DumpScoreboard(DumpBase):
    def sampling(self, dict):
        players=dict['registry']['scoreboard']
        return players[players.keys()[0]]

class League(object):
    def __init__(self, dbname, gamecode):
        self.dbname=dbname
        self.gamecode=gamecode

def diff_dump(dump1, dump2):
    subprocess.Popen(['diff',  '-urN',  dump1.fn(),  dump2.fn()])

def diff_league(league1, league2, dump_fn):
    for klass_name in ['RegistryTeamSeason','RegistryTeamProfile']:
        diff_dump(DumpTeam(credential, klass_name, league1, dump_fn),
                 DumpTeam(credential, klass_name, league2, dump_fn))

    for klass_name in ['RegistryPlayerBatterSeason', 
                       'RegistryPlayerBatterToday', 
                       'RegistryPlayerPitcherToday',
                       'RegistryPlayerPitcherSeason']:
        diff_dump(DumpPlayer(credential, klass_name, league1, dump_fn), 
             DumpPlayer(credential, klass_name, league2, dump_fn))

    for klass_name in ['ScoreBoard','ScoreBoardHome', 'ScoreBoardBases' ]:
        diff_dump(DumpScoreboard(credential, klass_name, league1, dump_fn), 
             DumpScoreboard(credential, klass_name, league2, dump_fn))

def diff_league_values(dump1,dump2):
    for klass_name in ['RegistryTeamSeason','RegistryTeamProfile'][:1]:
        diff_dump(DumpTeam(credential, klass_name, league1, dump_fn=feed.dump_keyname_and_value),
                 DumpTeam(credential, klass_name, league2, dump_fn=feed.dump_keyname_and_value))
    

if __name__=='__main__':
    credential=config.sports_live_db1_credential
    dump_fn=feed.dump_keyname_and_value
    kbo=League('kbo', '20090701HHSK0')
    npb=League('npb', '2009072101')
    #diff_league(kbo, npb, dump_fn)

    kbo=League('kbo', '20090701HHSK0')
    mlb=League('mlb', '20090723CCPH0')
    diff_league(kbo, mlb, dump_fn)
    #diff_league(kbo, mlb)


    #diff_league_values(kbo,mlb)
