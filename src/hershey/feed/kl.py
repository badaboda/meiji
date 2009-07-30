#!/usr/bin/env python2.6
# vim: et sts=4 sw=4 ts=4 ai fileencoding=utf8 :
import re, time, types, datetime

import feed
hierachy_dict=feed.hierachy_dict

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
class RegistryPlayerProfile(feed.RelayDatum):
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

class RegistryPlayerSeason(feed.RelayDatum):
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

class RegistryTeamSeason(feed.RelayDatum):
    def json_path(self):
        return "registry:team:**tcode:season"

    def fetch(self):
        return self.db.execute("""
                select a.home_team
                    , a.away_team
                    ,(select rank from kl.g_tb_game_rec where meet_year=a.meet_year and meet_seq=a.meet_seq and game_id=a.game_id and team_id=a.home_team) as home_season_rank
                    ,(select rank from kl.g_tb_game_rec where meet_year=a.meet_year and meet_seq=a.meet_seq and game_id=a.game_id and team_id=a.away_team) as away_season_rank
                    
                    ,(select count(team_id) from kl.g_tb_game_rec where meet_year=a.meet_year and meet_seq=a.meet_seq and win_type=1 and team_id=a.home_team) as home_season_win
                    ,(select count(team_id) from kl.g_tb_game_rec where meet_year=a.meet_year and meet_seq=a.meet_seq and win_type=2 and team_id=a.home_team) as home_season_lose
                    ,(select count(team_id) from kl.g_tb_game_rec where meet_year=a.meet_year and meet_seq=a.meet_seq and win_type=3 and team_id=a.home_team) as home_season_draw
                    ,(select count(team_id) from kl.g_tb_game_rec where meet_year=a.meet_year and meet_seq=a.meet_seq and win_type=1 and team_id=a.away_team) as away_season_win
                    ,(select count(team_id) from kl.g_tb_game_rec where meet_year=a.meet_year and meet_seq=a.meet_seq and win_type=2 and team_id=a.away_team) as away_season_lose
                    ,(select count(team_id) from kl.g_tb_game_rec where meet_year=a.meet_year and meet_seq=a.meet_seq and win_type=3 and team_id=a.away_team) as away_season_draw

                    ,(select sum(b.num) from (select if(win_type=1,1,0) as num from kl.g_tb_game_rec where meet_year=2009 and meet_seq='1' and team_id='K04' order by game_date desc limit 5) as b) as home_recent_win
                    ,(select sum(b.num) from (select if(win_type=2,1,0) as num from kl.g_tb_game_rec where meet_year=2009 and meet_seq='1' and team_id='K04' order by game_date desc limit 5) as b) as home_recent_lose
                    ,(select sum(b.num) from (select if(win_type=3,1,0) as num from kl.g_tb_game_rec where meet_year=2009 and meet_seq='1' and team_id='K04' order by game_date desc limit 5) as b) as home_recent_draw
                    ,(select sum(b.num) from (select if(win_type=1,1,0) as num from kl.g_tb_game_rec where meet_year=2009 and meet_seq='1' and team_id='K02' order by game_date desc limit 5) as b) as away_recent_win
                    ,(select sum(b.num) from (select if(win_type=2,1,0) as num from kl.g_tb_game_rec where meet_year=2009 and meet_seq='1' and team_id='K02' order by game_date desc limit 5) as b) as away_recent_lose
                    ,(select sum(b.num) from (select if(win_type=3,1,0) as num from kl.g_tb_game_rec where meet_year=2009 and meet_seq='1' and team_id='K02' order by game_date desc limit 5) as b) as away_recent_draw

                    ,(select sum(if(win_type=1,1,0)) as num from kl.g_tb_game_rec where meet_year=2009 and meet_seq='1' and team_id='K04' and other_team='K02' group by team_id) as home_vs_win
                    ,(select sum(if(win_type=2,1,0)) as num from kl.g_tb_game_rec where meet_year=2009 and meet_seq='1' and team_id='K04' and other_team='K02' group by team_id) as home_vs_lose
                    ,(select sum(if(win_type=3,1,0)) as num from kl.g_tb_game_rec where meet_year=2009 and meet_seq='1' and team_id='K04' and other_team='K02' group by team_id) as home_vs_draw
                    ,(select sum(if(win_type=1,1,0)) as num from kl.g_tb_game_rec where meet_year=2009 and meet_seq='1' and team_id='K02' and other_team='K04' group by team_id) as away_vs_win
                    ,(select sum(if(win_type=2,1,0)) as num from kl.g_tb_game_rec where meet_year=2009 and meet_seq='1' and team_id='K02' and other_team='K04' group by team_id) as away_vs_lose
                    ,(select sum(if(win_type=3,1,0)) as num from kl.g_tb_game_rec where meet_year=2009 and meet_seq='1' and team_id='K02' and other_team='K04' group by team_id) as away_vs_draw

                    ,(select round(sum(gain_goal)/count(team_id),2) from kl.g_tb_game_rec where meet_year=a.meet_year and meet_seq=a.meet_seq and team_id=a.home_team group by team_id) as home_avg_goal
                    ,(select round(sum(loss_goal+self_goal)/count(team_id),2) from kl.g_tb_game_rec where meet_year=a.meet_year and meet_seq=a.meet_seq and team_id=a.home_team group by team_id) as home_avg_loss
                    ,(select round(sum(gain_goal)/count(team_id),2) from kl.g_tb_game_rec where meet_year=a.meet_year and meet_seq=a.meet_seq and team_id=a.away_team group by team_id) as away_avg_goal
                    ,(select round(sum(loss_goal+self_goal)/count(team_id),2) from kl.g_tb_game_rec where meet_year=a.meet_year and meet_seq=a.meet_seq and team_id=a.away_team group by team_id) as away_avg_loss

                from kl.l_tb_meet_schedule a
                where concat(a.meet_year, a.meet_seq, a.game_id) = %s
            """ % (self.game_code))



            
