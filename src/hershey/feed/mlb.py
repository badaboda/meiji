# vim: fileencoding=utf-8 :
import re, time, types, datetime

import feed
hierachy_dict=feed.hierachy_dict

class RegistryPlayerProfile(feed.RelayDatum):
    def json_path(self):
        return u"registry:player:**pcode:profile"

    def fetch(self):
        # MLB에는 backnum가 없음
        # KBO,NPB에는 position 정보가 Person 테이블에 있었는데
        #   MLB에는 BatterRercord 테이블에 있다.

        # position 정보가 BatterRecord 테이블에만 있고
        #  PitcherRecord에는 없어서
        # BatterRecord와 PitcherRecord를 조회할 때 KBO, NPB처럼 같은 컬럼 목록을 쓸 수 없다.

        # KBO, NPB에는 hittype 정보가 hittype column에 '우'·'좌'로 들어가 있는데
        #   MLB에는 BAT, Throw 필드에 나누어 들어가 있다.

        batter_rows=self.db.execute("""
                    SELECT
                        p.PlayerID as pcode,
                        p.name as name,
                        br.PositionName as position,
                        br.position as position_code,
                        p.bat as hittype,
                        p.weight as weight,
                        p.height as height
                    FROM MLB_BatterRecord br, MLB_Person p
                    WHERE br.PlayerID = p.PlayerID
                        and br.gameID = '%s'
                """ % self.game_code)
        pitcher_rows=self.db.execute("""
                    SELECT
                        p.PlayerID as pcode,
                        p.name as name,
                        p.bat as hittype,
                        p.weight as weight,
                        p.height as height
                    FROM MLB_PitcherRecord pr, MLB_Person p
                    WHERE pr.PlayerID = p.PlayerID
                        and pr.gameID = '%s'
                """ % (self.game_code))
        rows=batter_rows+pitcher_rows
        return rows

class RegistryPlayerBatterSeason(feed.RelayDatum):
    def json_path(self):
        return u"registry:player:**pcode:batter:season"

    def fetch(self):
        rows=self.db.execute("""
                    SELECT
                        b.playerID as pcode,
                        (select t.team_code from MLB_Teams t where t.tickername = b.teamcode) as tcode,
                        b.*
                    FROM MLB_BatterRecord br, MLB_BatTotalAll b
                    WHERE br.playerID = b.playerID
                        AND substring(br.gameID,1,4) = b.YEAR
                        AND br.gameID = '%s'
                """ % self.game_code)
        return rows

    def postprocess(self, row):
        row=super(RegistryPlayerBatterSeason, self).postprocess(row)
        del row['teamcode']
        return row

class RegistryPlayerBatterToday(feed.RelayDatum):
    def json_path(self):
        return u"registry:player:**pcode:batter:today"

    def fetch(self):
        rows=self.db.execute("""
                    SELECT br.playerId as pcode,
                            br.OAB        AS ab,
                            (br.H1+ br.H2 + br.H3 + br.HR)     AS h,
                            br.HR     AS hr,
                            br.BB     AS bb,
                            br.RBI     AS rbi,
                            br.SO     AS so,
                            br.Run     AS r
                    FROM MLB_BatterRecord br
                    WHERE br.gameID = '%s'
                """ % self.game_code)
        return rows

class RegistryPlayerPitcherToday(feed.RelayDatum):
    def json_path(self):
        return u"registry:player:**pcode:pitcher:today"

    def fetch(self):
        rows=self.db.execute("""
                    SELECT  pr.PlayerID       AS pcode,
                            pr.Inning         AS ip,
                            pr.Run            AS r,
                            pr.ER             AS er,
                            pr.Hit            AS hit,
                            pr.SO             AS so,
                            pr.PitchBallCnt   AS np,
                            pr.PitchStrikeCnt AS s,
                            (pr.PitchBallCnt - pr.PitchStrikeCnt) AS b
                    FROM MLB_PitcherRecord pr
                    WHERE pr.gameID = '%s'
                """ % self.game_code)
        return rows

class RegistryPlayerPitcherSeason(feed.RelayDatum):
    def json_path(self):
        return u"registry:player:**pcode:pitcher:season"

    def fetch(self):
        # KBO, NPB에서는 Save가 'SV'
        #  MLB에서는 'S'  TODO
        rows=self.db.execute("""
                    SELECT
                        pt.playerID as pcode,
                        pt.W,
                        pt.L,
                        pt.HIT,
                        pt.HR,
                        pt.KK,
                        pt.S
                    FROM MLB_PitcherRecord pr, MLB_PitTotalAll pt
                    WHERE pr.gameID = '%s'
                        AND pt.PlayerID = pr.PlayerID
                        AND substring(pr.gameID,1,4) = pt.YEAR
                """ % self.game_code)
        return rows

class RegistryTeamSeason(feed.RelayDatum):
    def json_path(self):
        return u"registry:team:**tcode:season"

    def fetch(self):
        rows=self.db.execute("""
            select c.team_code as tcode, c.team_name as team, (a.won+a.lost) as gameplayed, a.won, a.lost, a.percentage,
                 a.games_behind, b.last10wins, b.last10losses
                 from mlb.MLB_STANDINGS a, mlb.MLB_STANDINGEX b, mlb.MLB_Teams c
                 where a.enddate = b.enddate and a.teamcode = b.teamcode
                 and a.teamcode = c.tickername
                 and a.enddate = (select max(enddate) as lastdate from mlb.MLB_STANDINGS)
                        order by a.percentage desc, a.won desc
        """)
        return rows

class RegistryTeamProfile(feed.RelayDatum):
    def json_path(self):
        return u"registry:team:**tcode:profile"

    def fetch(self):
        rows=self.db.execute("""
                    SELECT team_code as tcode,
                           franchise as teamname1,
                           nickname as teamname2,
                           gujang as stadium,
                           SSteamname as region
                    FROM MLB_Teams
                """)
        return rows



class ScoreBoard(feed.RelayDatum):
    def json_path(self):
        return u"registry:scoreboard:**game_code"

    def fetch_kbo_schedule(self):
        rows=self.db.execute("""
            SELECT gmkey as game_code,
                   substring(k_time, 1, 4) as season_year,
                   HTeam_Name as home_team,
                   HTeam_code as home_tcode,
                   VTeam_Name as away_team,
                   VTeam_code as away_tcode,
                   park as stadium,
                   substring(k_time, 1, 4) as gyear,
                   substring(k_time, 6, 7) as gmonth,
                   substring(k_time, 9, 10) as gday,
                   substring(k_time, 12, 16) as gtime
            FROM MLB_Live_Schedule
            WHERE gmkey = '%s'
        """ % (self.game_code))
        if len(rows) == 0:
            raise feed.NoDataFoundForScoreboardError(self.game_code, "MLB_Live_Schedule")
        return rows[0]

    def fetch_ie_livetext(self):
        rows=self.db.execute("""
            SELECT
                MLB_LiveText.inning AS inning,
                MLB_LiveText.bTop  AS bhome,
                if(( textStyle='99'),'end','ing') as status
            FROM
                MLB_LiveText
            WHERE
                MLB_LiveText.gameID = '%s'
            ORDER BY
                inning DESC,
                bhome
            LIMIT 1
        """ % self.game_code)
        if len(rows) == 0:
            raise feed.NoDataFoundForScoreboardError(self.game_code, "MLB_LiveText")
        return rows[0]

    def fetch_ballcount(self):
        rows=self.db.execute("""
            SELECT strike as strike,
                    ball as ball,
                    `out` as `out`,
                    pitcher as pitcher_pcode,
                    batter as batter_pcode
            FROM MLB_BallCount
            WHERE gameID = '%s'
        """ % self.game_code)
        if len(rows) == 0:
            raise feed.NoDataFoundForScoreboardError(self.game_code, "MLB_BallCount")
        return rows[0]

    def fetch(self):
        row1=self.fetch_kbo_schedule()
        try:
            row2=self.fetch_ie_livetext()
            row3=self.fetch_ballcount()
            row1.update(row2)
            row1.update(row3)
        except feed.NoDataFoundForScoreboardError:
            # 아직 시작하지 않은 경기라면
            # Schedule 테이블에는 game_code가 있는데
            # LiveText 테이블에는 데이터가 없을 수 있다
            pass

        return [row1]

    def postprocess(self, row):
        row=super(ScoreBoard, self).postprocess(row)
        row['game_datetime']="%s-%s-%sT%sZ" % (row['gyear'], row['gmonth'], row['gday'],
                                               re.search('\d+:\d+', row['gtime']).group(0))
        for name_to_remove in ['gyear', 'gmonth', 'gday', 'gtime']:
            del row[name_to_remove]
        return row

class ScoreBoardHomeOrAwayMixIn:
    def fetch(self, bhome):
        rows=self.db.execute("""
                SELECT
                    rheb.gameID as game_code,
                    rheb.Run as r,
                    rheb.Hit as h,
                    rheb.Error as e,
                    rheb.BallFour as b
                FROM
                    MLB_ScoreRHEB rheb
                WHERE
                    rheb.gameID = '%s'
                    AND rheb.bhome = %d
        """ % (self.game_code, bhome))
        if len(rows)==0:
            raise feed.NoDataFoundForScoreboardError(self.game_code,'MLB_ScoreRHEB:%d' % bhome)
        row=rows[0]

        innings=self.db.execute("""
                SELECT
                    inning,
                    score
                FROM
                    MLB_Scoreinning
                WHERE
                    MLB_Scoreinning.gameID = '%s'
                    AND MLB_Scoreinning.bhome = %d
                ORDER BY inning asc
        """ % (self.game_code, bhome))
        row['inning']=','.join([str(i['score']) for i in innings])
        return [row]


class ScoreBoardHome(feed.RelayDatum, ScoreBoardHomeOrAwayMixIn):
    def json_path(self):
        return u"registry:scoreboard:**game_code:home"

    def fetch(self):
        return ScoreBoardHomeOrAwayMixIn.fetch(self, 1)

class ScoreBoardAway(feed.RelayDatum, ScoreBoardHomeOrAwayMixIn):
    def json_path(self):
        return u"registry:scoreboard:**game_code:away"

    def fetch(self):
        return ScoreBoardHomeOrAwayMixIn.fetch(self, 0)

class ScoreBoardBases(feed.RelayDatum):
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

    def fetch(self):
        rows=self.db.execute("""
                SELECT gameID as game_code,
                    base1,
                    base2,
                    base3
                FROM MLB_BallCount
                WHERE gameID = '%s'
            """ % self.game_code)
        if len(rows)==0:
            raise feed.NoDataFoundForScoreboardError(self.game_code, 'MLB_BallCount')
        row = rows[0]

        current_batter_list = fetch_current_batter_list(self.db, self.game_code)
        for k in ['base1', 'base2', 'base3']:
            row[k]=self.find_pcode_of_batorder(row[k], current_batter_list)
        return rows

class ScoreBoardWatingBatters(feed.RelayDatumAsList):
    def json_path(self):
        return u"registry:scoreboard:%s:waiting_batters" % self.game_code

    def fetch(self):
        current_batter_list=fetch_current_batter_list(self.db, self.game_code)
        batorder=self.current_batorder(current_batter_list)

        from itertools import chain, islice
        return list(islice(chain(current_batter_list, current_batter_list), batorder, batorder+3))

    def current_batter_pcode(self):
        rows = self.db.execute("""
            SELECT batter
            FROM MLB_BallCount
            WHERE gameID = '%s'
        """ % self.game_code)
        if len(rows) == 0:
            raise feed.NoDataFoundForScoreboardError(self.game_code, "MLB_BallCount")
        return rows[0]['batter']

    def current_batorder(self, batter_list):
        pcode=self.current_batter_pcode()
        for batter in batter_list:
            if batter['pcode']==pcode:
                batorder=batter['batorder']
                assert type(batorder) in [types.IntType, types.LongType], "batorder type should be int but %s" % type(batorder)
                assert 0 < batorder < 10
                return batorder
        else:
            raise ValueError('pcode not found in current_batter_list: %s' % pcode)

class LeagueTodayGames(feed.RelayDatumAsList):
    def __init__(self, db, game_code, game_datetime=datetime.datetime.now()):
        super(LeagueTodayGames, self).__init__(db, game_code)
        self.game_datetime = game_datetime

    def json_path(self):
        return u"league:today_games"

    def fetch(self):
        rows=self.db.execute("""
                    SELECT gmkey as game_code
                    FROM MLB_Live_Schedule
                    WHERE concat(substring(K_Time, 1, 4), substring(K_Time, 6, 2), substring(K_Time, 9, 2)) = '%s'
                """ % self.game_datetime.strftime("%Y%m%d"))
        return [row['game_code'] for row in rows]

    def as_delta_generator_input(self):
        self.ensure_rows()
        return tuple(self.rows)

class LeaguePastVsGames(feed.RelayDatumAsList):
    def __init__(self, db, game_code):
        super(LeaguePastVsGames, self).__init__(db, game_code)

    def json_path(self):
        return u"league:past_vs_games"

    def fetch(self):
        rows=self.db.execute("""
                SELECT
                    a.gmkey as game_code
                FROM
                    MLB_Live_Schedule a, (select gmkey as game_code, HTeam_Name as home, VTeam_Name as visit from MLB_Live_Schedule where gmkey = '%s') b
                WHERE
                  ((a.HTeam_Name = b.home and a.VTeam_Name = b.visit) OR
                   (a.HTeam_Name = b.visit and a.VTeam_Name = b.home))
                  AND
                    concat(substring(K_Time, 1, 4), substring(K_Time, 6, 2), substring(K_Time, 9, 2)) < substring(b.game_code,1,8)
                  AND
                    concat(substring(K_Time, 1, 4), substring(K_Time, 6, 2), substring(K_Time, 9, 2)) > date_format(curdate(),'%%Y0401')
                ORDER BY k_time DESC
                LIMIT 3
                """ % (self.game_code))
        return [row['game_code'] for row in rows]
    def as_delta_generator_input(self):
        self.ensure_rows()
        return tuple(self.rows)


# ----------

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
        FROM MLB_LiveText
        WHERE gameID = '%s'
            AND inning <> 99
        ORDER BY seqNo desc
        LIMIT 1
    """ % game_code)
    if len(rows)==0:
        raise feed.NoDataFoundForScoreboardError(game_code, 'MLB_LiveText')
    return int(rows[0]['btop'])

def fetch_current_batter_list(db, game_code):
    btop=current_btop(db, game_code)
    assert btop in [0, 1], "btop |%d|" % btop
    bhome=((btop==0) and 1) or 0

    lineup_sql="""
        SELECT br.bhome as home,
               br.playerID as pcode,
               br.BatOrder as batorder,
               br.position as position_code
        FROM MLB_BatterRecord br
        INNER JOIN
            MLB_Person p
            ON (br.PlayerID = p.PlayerID)
        LEFT OUTER JOIN
            MLB_BatTotalAll bt
            ON (br.PlayerID = bt.PlayerID)
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

# ----------
