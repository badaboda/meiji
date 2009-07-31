#!/usr/bin/env python2.6
# vim: et sts=4 sw=4 ts=4 ai fileencoding=utf8 :
import re, time, types, datetime

import feed
hierachy_dict=feed.hierachy_dict

class RelayDatum(feed.RelayDatum):
    @property
    def meet_year(self):
        return self.game_code[:4]

    @property
    def meet_seq(self):
        return self.game_code[4:5]


class LiveText(feed.RelayDatumAsList):
    def json_path(self):
        return u"livetext"

    def fetch(self):
        return self.db.execute("""
            select
                half,
                ctime,
                title,
                remark as text,
                tcode,
                sortkey
            from
                kl.klw4000 a
            where
                a.gameidx = (select gameidx from kl.klw3000 where concat(gyear, gseq, gcode) = %s)
            order by  a.sortkey

        """ % (self.game_code))

class Meta(feed.RelayDatumAsAtom):
    def json_path(self):
        return "meta"

    def fetch(self):
        return [{
            'live_feed_type_text': True,
            'live_feed_type_video': False,
            'highlight_clips': False,
        }]

class GameCode(feed.RelayDatumAsAtom):
    def json_path(self):
        return "game_code"

    def fetch(self):
        return [self.game_code]

class RegistryPlayerProfile(RelayDatum):
    def json_path(self):
        return u"registry:player:**pcode:profile"

    def fetch(self):
        return self.db.execute("""
                        select a.player_id as pcode
                            ,back_no as backnum
                            ,captain_yn
                            ,goal_b_qty + goal_a_qty + goal_e_qty as goal
                            ,(select n.player_name from kl.p_tb_player n where n.player_id=a.player_id) as name
                            ,(select g.code_name from kl.c_tb_com_code g where g.code_type=24 and g.com_code=(select n.national from kl.p_tb_player n where n.player_id=a.player_id)) as national
                            ,(select z.home_type from kl.g_tb_game_rec z where z.team_id=a.team_id and concat(z.meet_year, z.meet_seq, z.game_id)= %s limit 1) as homeaway
                            ,(select e.code_name from kl.c_tb_com_code e where e.code_type=42 and e.com_code=a.position_code) as position
                            ,date_format((select f.birth_date from kl.p_tb_player_general f where f.player_id=a.player_id), '%%Y%%m%%d') as birth
                            ,(select f.height from kl.p_tb_player_general f where f.player_id=a.player_id) as height
                            ,(select f.weight from kl.p_tb_player_general f where f.player_id=a.player_id) as weight
                        from kl.g_tb_player_rec a
                        where concat(a.meet_year, a.meet_seq, a.game_id) = %s
                        order by homeaway asc, a.position_code asc
                """ % (self.game_code,self.game_code))

class RegistryPlayerSeason(RelayDatum):
    def json_path(self):
        return u"registry:player:**pcode:season"

    def fetch(self):
        return self.db.execute("""
               select a.player_id as pcode
                ,sum(a.goal_b_qty+a.goal_a_qty+a.goal_e_qty) as season_goal
                ,sum(assist_qty) as assist
                ,sum(st_qty) as shoot
                ,sum(fo_qty) as foul
                ,sum(ck_qty) as conerkick
                ,sum(gk_qty) as goalkick
                ,sum(pk_y_qty+pk_n_qty) as pk_qty
                ,sum(gk_qty) as gk_qty
                ,sum(os_qty) as os_qty
                ,sum(warn_qty) as warn_qty
                ,sum(exit_qty) as exit_qty
                ,sum(change_i_qty) as change_i_qty
              from kl.g_tb_player_rec a
             where concat(a.meet_year, a.meet_seq, a.game_id) = '20091114'
         group by a.player_id
        """)

class RegistryTeamCup(RelayDatum):
    def json_path(self):
        return "registry:team:**tcode:cup"

    def fetch(self):
        team_stats=self.db.execute("""
            select team_id as tcode, win_type, count(team_id) as cnt
            from kl.g_tb_game_rec
            where meet_year = '%s' and meet_seq = '%s'
            group by team_id, win_type
        """ % (self.meet_year, self.meet_seq))

        rank_stats=self.db.execute("""
            select o.team_id as tcode, o.rank as rank
            from (
                select a.team_id as team_id, meet_year, meet_seq, max(a.game_id) game_id
                from g_tb_game_rec a
                where meet_year = '%s' AND meet_seq='%s'
                group by team_id
            ) m,
               g_tb_game_rec o
            where
              m.team_id = o.team_id
              AND m.meet_year = o.meet_year
              AND m.meet_seq = o.meet_seq
              AND m.game_id = o.game_id
        """ % (self.meet_year, self.meet_seq))

        keyname_from_win_type = {
            1: 'win',
            2: 'lose',
            3: 'draw'
        }
        stats_by_tcode=t={}
        for row in team_stats:
            tcode=row['tcode']
            if not t.has_key(tcode):
                t[tcode] = {}
            t[tcode][keyname_from_win_type[row['win_type']]] = row['cnt']

            avg_stats = self.db.execute("""
               select
                    (select round(sum(gain_goal)/count(team_id),2) from kl.g_tb_game_rec where meet_year='%(meet_year)s' and meet_seq='%(meet_seq)s' and team_id='%(tcode)s' group by team_id) as avg_goal,
                    (select round(sum(loss_goal+self_goal)/count(team_id),2) from kl.g_tb_game_rec where meet_year='%(meet_year)s' and meet_seq='%(meet_seq)s' and team_id='%(tcode)s' group by team_id) as avg_goal_loss
            """ % { 'meet_year': self.meet_year, 'meet_seq': self.meet_seq, 'tcode': tcode })
            t[tcode].update(avg_stats[0])

        for row in rank_stats:
            tcode=row['tcode']
            if not t.has_key(tcode):
                t[tcode] = {}
            t[tcode]['rank'] = row['rank']

        rows = []
        for tcode, dict in stats_by_tcode.items():
            dict.update({'tcode': tcode})
            rows.append(dict)
        return rows

class RegistryTeamRecentFive(RelayDatum):
    def json_path(self):
        return "registry:team:**tcode:recent5"

    def fetch(self):
        teams = self.db.execute("""
            select team_id as tcode
            from kl.g_tb_game_rec
            where meet_year = '%(meet_year)s' AND meet_seq='%(meet_seq)s'
            group by team_id
        """ % { 'meet_year': self.meet_year, 'meet_seq': self.meet_seq })
        rows = []
        for team in teams:
            tcode = team["tcode"]
            recent5 = self.db.execute("""
                     select sum(a.w) as win, sum(a.l) as lose, sum(a.d) as draw
                     from
                        (select if(win_type=1,1,0) as w, if(win_type=2,1,0) as l, if(win_type=3,1,0) as d
                         from kl.g_tb_game_rec
                         where meet_year='%(meet_year)s'
                            and meet_seq='%(meet_seq)s'
                            and team_id='%(tcode)s'
                         order by game_date desc
                         limit 5) as a
            """ % { 'meet_year': self.meet_year, 'meet_seq': self.meet_seq, 'tcode': tcode })
            recent5[0].update({'tcode': tcode})
            rows.append(recent5[0])
        return rows

class RegistryScoreBoardMixIn(object):
    def fetch_tcode(self, bhome):
        rows=self.db.execute("""
            select home_team as home_tcode, away_team as away_tcode
            from l_tb_meet_schedule
            where meet_year='%s' and meet_seq='%s' and game_id='%s'
        """ % (self.meet_year, self.meet_seq, self.game_code[5:]))
        return rows[0][bhome and 'home_tcode' or 'away_tcode']

    def fetch(self):
        tcode=self.fetch_tcode(self.bhome)

        return self.db.execute("""
            select '%s' as game_code,
                    '%s' as tcode,
                    sum(assist_qty) as assist,
                    sum(st_qty) as shoot,
                    sum(ck_qty) as ck,
                    sum(os_qty) as offside,
                    sum(fo_qty) as foul,
                    sum(warn_qty) as yellow,
                    sum(exit_qty) as red,
                    sum(change_i_qty) as `inout`,
                    sum(pk_y_qty) as `pk`
                from kl.g_tb_player_rec
                where meet_year='%s' and meet_seq='%s' and game_id='%s' and team_id='%s'
        """ % (self.game_code, tcode, self.meet_year, self.meet_seq, self.game_code[5:], tcode))


class RegistryScoreBoardHome(RelayDatum, RegistryScoreBoardMixIn):
    def __init__(self, *args):
        super(RelayDatum, self).__init__(*args)
        self.bhome = True

    def json_path(self):
        return "registry:scoreboard:**game_code:home"

class RegistryScoreBoardAway(RelayDatum, RegistryScoreBoardMixIn):
    def __init__(self, *args):
        super(RelayDatum, self).__init__(*args)
        self.bhome=False

    def json_path(self):
        return "registry:scoreboard:**game_code:away"

class RegistryScoreBoardGoals(feed.RelayDatumAsList):
    def json_path(self):
        return "registry:scoreboard:goals"

    def fetch(self):
        return self.db.execute("""
            select
                '%(game_code)s' as game_code,
                goal_team_id as tcode,
                player_id as pcode,
                ast_player_id assist_pcode,
                if(self_goal_yn='N', 0, 1) as is_self_goal,
                concat(time_min, ':',time_sec) as goal_time,
                (select code_name from kl.c_tb_com_code where code_type='47' and com_code=a.half_type) as `when`
            from kl.g_tb_goal_state a
            where concat(a.meet_year, a.meet_seq, a.game_id) = '%(game_code)s'
        """ % {'game_code' : self.game_code})

    def postprocess(self, row):
        row=super(feed.RelayDatumAsList, self).postprocess(row)
        row['is_self_goal']=bool(row['is_self_goal'])
        return row
