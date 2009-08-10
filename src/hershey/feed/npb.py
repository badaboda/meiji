# vim: et sts=4 sw=4 ts=4 ai fileencoding=utf8 :
import re, types
import feed

class LiveText(feed.RelayDatumAsList):
    def json_path(self):
        return u"livetext"

    def fetch(self):
        return self.db.execute("""
            SELECT
                LIVETEXT.gmkey    AS game_code,
                LIVETEXT.SeqNO     AS seq,
                LIVETEXT.Inning    AS inning,
                LIVETEXT.bTop      AS btop,
                LIVETEXT.LiveText  AS text,
                LIVETEXT.textStyle AS textstyle
            FROM
                LIVETEXT
            WHERE
                LIVETEXT.gmkey = '%s'
            ORDER BY
                seqNo
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

    def as_delta_generator_input(self):
        self.ensure_rows()
        return tuple(self.rows)


class RegistryPlayerProfile(feed.RelayDatum):
    def json_path(self):
        return u"registry:player:**pcode:profile"

    def fetch(self):
        batter_rows=self.db.execute("""
                    SELECT p.*
                    FROM BATTERRECORD br, PERSON p
                    WHERE br.PlayerID = p.PCODE
                        and br.gamkey = '%s'
                """ % self.game_code)
        pitcher_rows=self.db.execute("""
                    SELECT p.*
                    FROM PITCHERRECORD pr, PERSON p
                    WHERE pr.PlayerID = p.PCODE
                        and pr.gmkey = '%s'
                """ % self.game_code)
        rows=batter_rows+pitcher_rows
        return rows

    def postprocess(self, row):
        row=super(RegistryPlayerProfile, self).postprocess(row)
        #print row['pos']
        for delete_key in ['gyear', 'pos', 'p_flag']:
            del row[delete_key]
        return row

class RegistryPlayerBatterSeason(feed.RelayDatum):
    def json_path(self):
        return u"registry:player:**pcode:batter:season"

    def fetch(self):
        rows=self.db.execute("""
                    SELECT b.*
                    FROM BATTERRECORD br, HITTER_P b
                    WHERE br.PlayerID = b.PCODE
                        AND substring(br.gamkey,1,4) = b.GYEAR
                        AND br.gamkey = '%s'
                """ % self.game_code)
        return rows

    def postprocess(self, row):
        row=super(RegistryPlayerBatterSeason, self).postprocess(row)
        row['avg']=row['hra']
        for cast_field_name in ['avg', 'obp', 'ops', 'slg']:
            row[cast_field_name]=feed.safe_float(row[cast_field_name])

        for name_to_remove in ['game_flag', 'pa_flag', 'gyear', 'hra', 'team']:
            del row[name_to_remove]
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
                            br.BBHP     AS bbhp,
                            br.RBI     AS rbi,
                            br.SO     AS so,
                            br.Run     AS r
                    FROM BATTERRECORD br
                    WHERE br.gamkey = '%s'
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
                            pr.PitchBallCnt   AS np
                    FROM PITCHERRECORD pr
                    WHERE pr.gmkey = '%s'
                """ % self.game_code)
        return rows

class RegistryPlayerPitcherSeason(feed.RelayDatum):
    def json_path(self):
        return u"registry:player:**pcode:pitcher:season"

    def fetch(self):
        rows=self.db.execute("""
                    SELECT
                        pt.pcode as pcode,
                        pt.WIN as win,
                        pt.LOSE as lose,
                        pt.HIT as hit,
                        pt.HR as hr,
                        pt.KK as kk,
                        pt.SV as save
                    FROM PITCHERRECORD pr, PITCHER_P pt
                    WHERE pr.gmkey = '%s'
                        AND pt.PCODE = pr.PlayerID
                        AND substring(pr.gmkey,1,4) = pt.GYEAR
                """ % self.game_code)
        return rows

class RegistryTeamSeason(feed.RelayDatum):
    def json_path(self):
        return u"registry:team:**tcode:season"

    def fetch(self):
        rows=self.db.execute("""
                    select tr.team as tcode, tr.*
                    from TEAMRANK tr
                    where substring('%s',1,4) = tr.GYEAR
                """ % (self.game_code))
        return rows

    def postprocess(self, row):
        row=super(RegistryTeamSeason, self).postprocess(row)
        row['wra']=float(row['wra'])
        row['gb']=feed.safe_int(row['gb'], 0)
        for name_to_remove in ['team']:
            del row[name_to_remove]
        return row


class RegistryTeamProfile(feed.RelayDatum):
    def json_path(self):
        return u"registry:team:**tcode:profile"

    def fetch(self):
        rows=self.db.execute("""
                    SELECT team as tcode,
                           teamname as teamname1
                    FROM TEAM
                """)
        return rows


class ScoreBoard(feed.RelayDatum):
    def json_path(self):
        return u"registry:scoreboard:**game_code"

    def fetch_schedule(self):
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
                   k_time
            FROM SCHEDULE
            WHERE gmkey = '%s'
        """ % (self.game_code))
        if len(rows) == 0:
            raise feed.NoDataFoundForScoreboardError(game_code, "SCHEDULE")
        return rows[0]

    def fetch_livetext(self):
        rows=self.db.execute("""
            SELECT
                LIVETEXT.inning AS inning,
                LIVETEXT.bTop  AS bhome,
                if(( textStyle='99'),'end','ing') as status
            FROM
                LIVETEXT
            WHERE
                LIVETEXT.gmkey = '%s'
            ORDER BY
                inning DESC,
                bhome
            LIMIT 1
        """ % self.game_code)
        if len(rows) == 0:
            raise feed.NoDataFoundForScoreboardError(game_code, "LIVETEXT")
        return rows[0]

    def fetch_ballcount(self):
        rows=self.db.execute("""
            SELECT strike as strike,
                    ball as ball,
                    `out` as `out`,
                    pitcher as pitcher_pcode,
                    batter as batter_pcode
            FROM BALLCOUNT
            WHERE gmkey = '%s'
        """ % self.game_code)
        if len(rows) == 0:
            raise feed.NoDataFoundForScoreboardError(game_code, "BALLCOUNT")
        return rows[0]

    def fetch(self):
        row1=self.fetch_schedule()
        row2=self.fetch_livetext()
        row3=self.fetch_ballcount()

        row1.update(row2)
        row1.update(row3)
        return [row1]

    def postprocess(self, row):
        row=super(ScoreBoard, self).postprocess(row)
        t = re.search('\d+', row['k_time']).group(0)
        row['game_datetime']="%s-%s-%sT%s:%sZ" % (row['gyear'], row['gmonth'], row['gday'], t[:2], t[2:4])
        
        for name_to_remove in ['gyear', 'gmonth', 'gday', 'k_time']:
            del row[name_to_remove]
        row['double_header'] = bool(row['double_header'])
        return row

class ScoreBoardHomeOrAwayMixIn:
    def fetch(self, bhome):
        row=self.db.execute("""
                SELECT
                    rheb.gmkey as game_code,
                    rheb.Run as r,
                    rheb.Hit as h,
                    rheb.Error as e,
                    rheb.BallFour as b
                FROM
                    SCORERHEB rheb
                WHERE
                    rheb.gmkey = '%s'
                    AND rheb.bhome = %d
        """ % (self.game_code, bhome))[0]

        innings=self.db.execute("""
                SELECT
                    inning,
                    score
                FROM
                    SCOREINNING
                WHERE
                    SCOREINNING.gmkey = '%s'
                    AND SCOREINNING.bhome = %d
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
        assert type(batorder)==types.IntType, "batorder should be int but %s" % type(batorder)
        if batorder == 0:
            return None
        for b in current_batter_list:
            if b['batorder'] == batorder:
                return b['pcode']
        else:
            raise ValueError('batorder not found: %d' % batorder)

    def fetch(self):
        rows=self.db.execute("""
                SELECT gmkey as game_code,
                    base1,
                    base2,
                    base3
                FROM BALLCOUNT
                WHERE gmkey = '%s'
            """ % self.game_code)
        if len(rows) == 0:
            raise feed.NoDataFoundForScoreboardError(game_code, "BALLCOUNT")
        return rows

class ScoreBoardWatingBatters(feed.RelayDatumAsList):
    def json_path(self):
        return u"registry:scoreboard:%s:waiting_batters" % self.game_code

    def fetch(self):
        current_batter_list=fetch_current_offense_batter_list(self.db, self.game_code)
        batorder=self.current_batorder(current_batter_list)

        from itertools import chain, islice
        return list(islice(chain(current_batter_list, current_batter_list), batorder, batorder+3))

    def current_batter_pcode(self):
        rows = self.db.execute("""
            SELECT batter
            FROM BALLCOUNT
            WHERE gmkey = '%s'
        """ % self.game_code)
        if len(rows) == 0:
            raise feed.NoDataFoundForScoreboardError(self.game_code, "BALLCOUNT")
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

class ScoreBoardHomeLineupBatter(feed.RelayDatumAsList):
    def json_path(self):
        return "registry:scoreboard:%s:home:lineup:batter" % self.game_code

    def fetch(self):
        return fetch_batter_list_of(self.db, self.game_code, 1)

class ScoreBoardAwayLineupBatter(feed.RelayDatumAsList):
    def json_path(self):
        return "registry:scoreboard:%s:away:lineup:batter" % self.game_code

    def fetch(self):
        return fetch_batter_list_of(self.db, self.game_code, 0)

class ScoreBoardHomeLineupPitcher(feed.RelayDatumAsList):
    def json_path(self):
        return "registry:scoreboard:%s:home:lineup:pitcher" % self.game_code

    def fetch(self):
        return fetch_pitcher_list_of(self.db, self.game_code, 1)

class ScoreBoardAwayLineupPitcher(feed.RelayDatumAsList):
    def json_path(self):
        return "registry:scoreboard:%s:away:lineup:pitcher" % self.game_code

    def fetch(self):
        return fetch_pitcher_list_of(self.db, self.game_code, 0)

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
        FROM LIVETEXT
        WHERE gmkey = '%s'
            AND inning <> 99
        ORDER BY seqNo desc
        LIMIT 1
    """ % game_code)
    if len(rows) == 0:
        raise feed.NoDataFoundForScoreboardError(self.game_code, "LIVETEXT")
    return rows[0]['btop']

def fetch_current_offense_batter_list(db, game_code):
    btop=current_btop(db, game_code)
    assert btop in [0, 1], "btop |%d|" % btop
    bhome=((btop==0) and 1) or 0
    return fetch_batter_list_of(db, game_code, bhome)

def fetch_batter_list_of(db, game_code, bhome):
    lineup_sql="""
        SELECT br.bhome as home,
               br.playerID as pcode,
               br.BatOrder as batorder,
               br.posname as position
        FROM BATTERRECORD br
        INNER JOIN
            PERSON p
            ON (br.PlayerID = p.PCODE)
        LEFT OUTER JOIN
            HITTER_P bt
            ON (br.PlayerID = bt.PCODE)
        WHERE
            br.gamkey = '%s'
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

def fetch_pitcher_list_of(db, game_code, bhome):
    lineup_sql = """
        SELECT
            PLAYERID as pcode,
            SEQNO as seq
        FROM
            PITCHERRECORD
        WHERE
            GMKEY ='%s' and BHOME='%d'
        ORDER BY
            SEQNO desc
    """
    return db.execute(lineup_sql % (game_code, bhome))

# ----------

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

scoreboard_datums = [ScoreBoard,
                   ScoreBoardHome,
                   ScoreBoardAway,
                   ScoreBoardBases,
                   ScoreBoardWatingBatters,]
