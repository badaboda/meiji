#!/usr/bin/env python2.6
# vim: et sts=4 sw=4 ts=4 ai fileencoding=utf8 :
import re, time, types, datetime

import feed

#import router_client
import mock_router_client as router_client

hierachy_dict=feed.hierachy_dict

def current_btop(db, game_code):
    # 9회초(btop=1) 에서 9회말로 넘어가는 순간
    #   홈팀이 이기고 있다면 
    #   9회말에 홈팀이 공격을 하지 않고 경기가 끝나고 
    # IE_LiveText에 inning=99,bTop=0로 '경기 종료'라는 메세지가 insert된다. 
    
    # IE_BallCount에는 9회초에 away의 타자pcode가 batter field에 들어가 업데이트가 중지되기에 
    #   경기가 끝난 이후에 batorder를 잘 얻어오려면 
    #   inning=99를 제외하 btop 값을 select해야 한다.
    rows = db.execute("""
        SELECT btop
        FROM IE_LiveText
        WHERE gameID = '%s' 
            AND inning <> 99
        ORDER BY seqNo desc
        LIMIT 1
    """ % game_code)
    assert len(rows) == 1
    return rows[0]['btop']

def fetch_current_batter_list(db, game_code):
    btop=current_btop(db, game_code)
    assert btop in [0, 1], "btop |%d|" % btop
    bhome=((btop==0) and 1) or 0

    lineup_sql="""
        SELECT br.bhome as home, 
               br.playerID as pcode,
               br.BatOrder as batorder,
               br.position as position_code
        FROM IE_BatterRecord br
        INNER JOIN 
            Kbo_Person p
            ON (br.PlayerID = p.PCODE)
        LEFT OUTER JOIN
            Kbo_BatTotal bt
            ON (br.PlayerID = bt.PCODE)
        WHERE 
            br.gameid = '%s'
            AND br.bhome = %d
        ORDER BY br.BatOrder ASC, br.SeqNo DESC
    """
    candidates=db.execute(lineup_sql % (game_code, bhome))

    old_batorder = None
    for c in candidates:
        c['is_in_batter_list'] = (old_batorder != c['batorder'])
        old_batorder=c['batorder']

    current_batter_list = [c for c in candidates if c['is_in_batter_list']]

    for b in current_batter_list:
        del b['is_in_batter_list']
    return current_batter_list


class RelayDatum:
    def __init__(self, db, game_code):
        self.db = db
        self.game_code = game_code

        self.rows = None
        self.extra_args = None

    def sql(self):
        raise NotImplemented()

    def ensure_rows(self):
        if not self.rows:
            self.rows=self.fetch(self.game_code)

    def as_delta_generator_input(self):
        self.ensure_rows()
        return feed.list_of_dict_to_list_of_pairs(self.rows) 

    def __parse_json_path(self, json_path):
        path=json_path.split(':')
        is_key=lambda s: s.startswith('**')
        index_of_key=None
        try:
            index_of_key=map(is_key, path).index(1)
        except ValueError:
            pass
        return path, index_of_key, path[:index_of_key], path[index_of_key+1:]

    def as_bootstrap_dict(self):
        self.ensure_rows()
        path, index_of_key, path_before_key, path_after_key = self.__parse_json_path(self.json_path())

        if index_of_key:
            key=path[index_of_key][2:]
            result = {}
            for row in self.rows:
                path_after_key=path[index_of_key+1:]
                if path_after_key:
                    result[row[key]]=hierachy_dict(path_after_key, row)
                    del row[key]
                else:
                    result[row[key]]=row
                    
            return hierachy_dict(path_before_key, result)
        else:
            return hierachy_dict(path, self.rows)

    def postprocess(self, row):
        return feed.lowercase_dict_key(row)

class RelayDatumAsList(RelayDatum):
    def as_bootstrap_dict(self):
        self.ensure_rows()
        path=self.json_path().split(':')
        return hierachy_dict(path, self.rows)

class RelayDatumAsAtom(RelayDatum):
    def as_bootstrap_dict(self):
        self.ensure_rows()
        path=self.json_path().split(':')
        return hierachy_dict(path, self.rows[0])

class LiveText(RelayDatumAsList):
    def json_path(self):
        return u"livetext"

    def fetch(self, game_code):
        return self.db.execute("""
            SELECT 
                kbo.IE_LiveText.gameID    AS game_code, 
                kbo.IE_LiveText.SeqNO     AS seq, 
                kbo.IE_LiveText.Inning    AS inning, 
                kbo.IE_LiveText.bTop      AS btop, 
                kbo.IE_LiveText.LiveText  AS text, 
                kbo.IE_LiveText.textStyle AS textstyle 
            FROM 
                kbo.IE_LiveText 
            WHERE 
                kbo.IE_LiveText.gameID = '%s'
            ORDER BY 
                seqNo 
        """ % (self.game_code))

class Meta(RelayDatumAsAtom):
    def json_path(self):
        return "meta"

    def fetch(self, game_code):
        return [{
            'live_feed_type_text': True,
            'live_feed_type_video': False,
            'highlight_clips': False,
        }]

class GameCode(RelayDatumAsAtom):
    def json_path(self):
        return "game_code"

    def fetch(self, game_code):
        return [game_code]

    def as_delta_generator_input(self):
        self.ensure_rows()
        return tuple(self.rows)

class RegistryPlayerProfile(RelayDatum):
    def json_path(self):
        return u"registry:player:**pcode:profile"

    def fetch(self, game_code):
        batter_rows=self.db.execute("""
                    SELECT p.*
                    FROM IE_BatterRecord br, Kbo_Person p
                    WHERE br.PlayerID = p.PCODE
                        and br.gameID = '%s' 
                """ % game_code)
        pitcher_rows=self.db.execute("""
                    SELECT p.*
                    FROM IE_PitcherRecord pr, Kbo_Person p
                    WHERE pr.PlayerID = p.PCODE
                        and pr.gameID = '%s' 
                """ % game_code)
        rows=batter_rows+pitcher_rows    
        return [self.postprocess(row) for row in rows]

class RegistryPlayerBatterSeason(RelayDatum):
    def json_path(self):
        return u"registry:player:**pcode:batter:season"

    def fetch(self, game_code):
        rows=self.db.execute("""
                    SELECT b.*
                    FROM IE_BatterRecord br, Kbo_BatTotal b
                    WHERE br.PlayerID = b.PCODE
                        AND substring(br.gameID,1,4) = b.GYEAR
                        AND br.gameID = '%s' 
                """ % game_code)
        return [self.postprocess(row) for row in rows]

class RegistryPlayerBatterToday(RelayDatum):
    def json_path(self):
        return u"registry:player:**pcode:batter:today"

    def fetch(self, game_code):
        rows=self.db.execute("""
                    SELECT br.playerId as pcode, 
                            br.OAB        AS ab, 
                            (br.H1+ br.H2 + br.H3 + br.HR)     AS h, 
                            br.HR     AS hr, 
                            br.BB     AS bb, 
                            br.RBI     AS rbi, 
                            br.SO     AS so,
                            br.Run     AS r
                    FROM IE_BatterRecord br
                    WHERE br.gameID = '%s' 
                """ % game_code)
        return [self.postprocess(row) for row in rows]

class RegistryPlayerPitcherToday(RelayDatum):
    def json_path(self):
        return u"registry:player:**pcode:pitcher:today"

    def fetch(self, game_code):
        rows=self.db.execute("""
                    SELECT  pr.PlayerID       AS pcode, 
                            pr.Inning         AS ip, 
                            pr.Run            AS r, 
                            pr.ER             AS er, 
                            pr.Hit            AS h, 
                            pr.SO             AS so, 
                            pr.PitchBallCnt   AS s, 
                            pr.PitchStrikeCnt AS b,
                            (pr.PitchBallCnt + pr.PitchStrikeCnt) AS np
                    FROM IE_PitcherRecord pr
                    WHERE pr.gameID = '%s' 
                """ % game_code)
        return [self.postprocess(row) for row in rows]

class RegistryPlayerPitcherSeason(RelayDatum):
    def json_path(self):
        return u"registry:player:**pcode:pitcher:season"

    def fetch(self, game_code):
        rows=self.db.execute("""
                    SELECT  
                        pt.pcode as pcode,
                        pt.W, 
                        pt.L,
                        pt.HIT,
                        pt.HR,
                        pt.KK,
                        pt.SV
                    FROM IE_PitcherRecord pr, Kbo_PitTotal pt
                    WHERE pr.gameID = '%s' 
                        AND pt.PCODE = pr.PlayerID
                        AND substring(pr.gameID,1,4) = pt.GYEAR
                """ % game_code)
        return [self.postprocess(row) for row in rows]

class RegistryTeamSeason(RelayDatum):
    def json_path(self):
        return u"registry:team:**tcode:season"

    def fetch(self, game_code):
        rows=self.db.execute("""
                    select (select home_key from Kbo_Schedule where home = tr.team order by GYEAR limit 1) as tcode,
                            tr.*
                    from Kbo_TeamRank tr
                    where substring('%s',1,4) = tr.GYEAR
                """ % (self.game_code))
        return [self.postprocess(row) for row in rows]

class RegistryTeamProfile(RelayDatum):
    def json_path(self):
        return u"registry:team:**tcode:profile"

    def fetch(self, game_code):
        rows=self.db.execute("""
                    SELECT team_id as tcode, 
                           teamname1,
                           teamname2,
                           stadium,
                           region 
                    FROM TEAM
                """)
        return [self.postprocess(row) for row in rows]

class LeagueTodayGames(RelayDatumAsList):
    def __init__(self, db, game_code, game_datetime=datetime.datetime.now()):
        RelayDatumAsList.__init__(self, db, game_code)
        self.game_datetime = game_datetime

    def json_path(self):
        return u"league:today_games"

    def fetch(self, game_code):
        rows=self.db.execute("""
                    SELECT gmkey as game_code
                    FROM Kbo_Schedule
                    WHERE date = '%s'
                """ % self.game_datetime.strftime("%Y%m%d"))
        return [row['game_code'] for row in rows]

    def as_delta_generator_input(self):
        self.ensure_rows()
        return tuple(self.rows)

class LeaguePastVsGames(RelayDatumAsList):
    def __init__(self, db, game_code):
        RelayDatumAsList.__init__(self, db, game_code)

    def json_path(self):
        return u"league:past_vs_games"

    def fetch(self, game_code):
        rows=self.db.execute("""
                SELECT 
                    a.gmkey as game_code
                FROM 
                    kbo.Kbo_Schedule a, (select gmkey as game_code, home, visit  from kbo.Kbo_Schedule where gmkey = '%s') b
                WHERE 
                  ((a.home = b.home and a.visit = b.visit) OR
                   (a.home = b.visit and a.visit = b.home))
                  AND
                    date < substring(b.game_code,1,8) 
                  AND
                    date > date_format(curdate(),'%%Y0401')
                ORDER BY date DESC
                LIMIT 3
                """ % (self.game_code))
        return [row['game_code'] for row in rows]
    def as_delta_generator_input(self):
        self.ensure_rows()
        return tuple(self.rows)

class ScoreBoard(RelayDatum):
    def json_path(self):
        return u"registry:scoreboard:**game_code"

    def fetch(self, game_code):
        rows=self.db.execute("""
            SELECT gmkey as game_code,
                   gyear as season_year,
                   home as home_team,
                   home_key as home_tcode,
                   visit as away_team,
                   visit_key as away_tcode,
                   stadium,
                   dheader as double_header,
                   gyear,
                   gmonth, 
                   gday,
                   gtime
            FROM Kbo_Schedule 
            WHERE gmkey = '%s'
        """ % (self.game_code))
        assert len(rows) == 1
        row1 = rows[0]
        
        row2=self.db.execute("""
            SELECT 
                kbo.IE_LiveText.inning AS inning, 
                kbo.IE_LiveText.bTop  AS bhome,
                if(( textStyle='99'),'end','ing') as status
            FROM 
                kbo.IE_LiveText
            WHERE 
                kbo.IE_LiveText.gameID = '%s'
            ORDER BY 
                inning DESC, 
                bhome 
            LIMIT 1
        """ % self.game_code)[0]

        row3=self.db.execute("""
            SELECT strike as strike,
                    ball as ball,
                    `out` as `out`,
                    pitcher as pitcher_pcode,
                    batter as batter_pcode
            FROM IE_BallCount
            WHERE gameID = '%s' 
        """ % self.game_code)[0]
        row1.update(row3)

        return [self.postprocess(row) for row in [row1]]

    def postprocess(self, row):
        row=RelayDatum.postprocess(self, row)
        row['game_datetime']="%s-%s-%sT%sZ" % (row['gyear'], row['gmonth'], row['gday'], 
                                               re.search('\d+:\d+', row['gtime']).group(0))
        for name_to_remove in ['gyear', 'gmonth', 'gday', 'gtime']:
            del row[name_to_remove]
        row['double_header'] = bool(row['double_header'])
        return row


class ScoreBoardHomeOrAwayMixIn:
    def fetch(self, game_code, bhome):
        row=self.db.execute("""
                SELECT 
                    rheb.gameID as game_code,
                    rheb.Run as r, 
                    rheb.Hit as h, 
                    rheb.Error as e, 
                    rheb.BallFour as b
                FROM 
                    IE_ScoreRHEB rheb
                WHERE 
                    rheb.gameID = '%s'
                    AND rheb.bhome = %d
        """ % (self.game_code, bhome))[0]

        innings=self.db.execute("""
                SELECT 
                    inning,
                    score
                FROM 
                    IE_Scoreinning 
                WHERE 
                    IE_Scoreinning.gameID = '%s'
                    AND IE_Scoreinning.bhome = %d
                ORDER BY inning asc
        """ % (self.game_code, bhome))
        row['inning']=','.join([str(i['score']) for i in innings])
        return [self.postprocess(row) for row in [row]]


class ScoreBoardHome(RelayDatum, ScoreBoardHomeOrAwayMixIn):
    def json_path(self):
        return u"registry:scoreboard:**game_code:home"

    def fetch(self, game_code):
        return ScoreBoardHomeOrAwayMixIn.fetch(self, game_code, 1)

class ScoreBoardAway(RelayDatum, ScoreBoardHomeOrAwayMixIn):
    def json_path(self):
        return u"registry:scoreboard:**game_code:away"

    def fetch(self, game_code):
        return ScoreBoardHomeOrAwayMixIn.fetch(self, game_code, 0)

class ScoreBoardBases(RelayDatum):
    def json_path(self):
        return u"registry:scoreboard:**game_code:bases"

    def find_pcode_of_batorder(self, batorder, current_batter_list):
        if batorder == 0:
            return None
        for b in current_batter_list:
            if b['batorder'] == batorder:
                return b['pcode']
        else:
            raise ValueError('batorder not found: %d' % batorder)

    def fetch(self, game_code):
        rows=self.db.execute("""
                SELECT gameID as game_code, 
                    base1, 
                    base2, 
                    base3
                FROM IE_BallCount
                WHERE gameID = '%s'
            """ % self.game_code)
        assert len(rows) == 1
        row = rows[0]

        current_batter_list = fetch_current_batter_list(self.db, self.game_code)
        for k in ['base1', 'base2', 'base3']:
            row[k]=self.find_pcode_of_batorder(row[k], current_batter_list)
        return rows

class ScoreBoardWatingBatters(RelayDatumAsList):
    def json_path(self):
        return u"registry:scoreboard:%s:waiting_batters" % self.game_code

    def fetch(self, game_code):
        current_batter_list=fetch_current_batter_list(self.db, self.game_code)
        batorder=self.current_batorder(current_batter_list)

        from itertools import chain, islice
        return list(islice(chain(current_batter_list, current_batter_list), batorder, batorder+3))

    def current_batter_pcode(self):
        rows = self.db.execute("""
            SELECT batter
            FROM IE_BallCount 
            WHERE gameID = '%s' 
        """ % self.game_code)
        assert len(rows) == 1
        return rows[0]['batter']

    def current_batorder(self, batter_list):
        pcode=self.current_batter_pcode()
        for batter in batter_list:
            if batter['pcode']==pcode:
                batorder=batter['batorder']
                assert type(batorder) == types.IntType
                assert 0 < batorder < 10
                return batorder
        else:
            raise ValueError('pcode not found in current_batter_list: %s' % pcode)


datums = [RegistryPlayerProfile,
            RegistryPlayerBatterSeason,
            RegistryPlayerBatterToday,
            RegistryPlayerPitcherToday, 
            RegistryPlayerPitcherSeason, 
            RegistryTeamSeason, 
            RegistryTeamProfile,
            LiveText, 
            Meta,
            GameCode]

league_datums = [ LeagueTodayGames, 
                  LeaguePastVsGames ]

scoreboard_datums = [ScoreBoard,
                   ScoreBoardHome, 
                   ScoreBoardAway,
                   ScoreBoardBases, 
                   ScoreBoardWatingBatters,]