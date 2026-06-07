# -*- coding: utf-8 -*-
"""
=====================================================================
 MARKET MENTOR  |  매일 아침 시황 자동 브리핑 봇
---------------------------------------------------------------------
 보유 11종목(3개 계좌)을 기준으로 매일 아침
   1) 보유종목 전일 등락 + 종목별 뉴스
   2) 전일 미국증시 + 선물 동향
   3) 매크로(환율/금리/유가)
   4) 오늘 일정(실적·지표 발표)
 를 생성하여 이메일 / 텔레그램으로 발송한다.
 어조: 균형잡힌 애널리스트형.
=====================================================================
"""

import os                       # 환경변수(민감정보)를 코드에 직접 박지 않기 위함
import sys                      # 종료 코드 제어용
import smtplib                  # 이메일 발송(SMTP)
import logging                  # 실행 로그 기록
import datetime as dt           # 날짜/시간 처리
from email.mime.text import MIMEText            # 이메일 본문(HTML) 래핑
from email.mime.multipart import MIMEMultipart  # 제목+본문 멀티파트
from concurrent.futures import ThreadPoolExecutor, as_completed  # 데이터 병렬 수집

import yfinance as yf           # 글로벌 시세(지수/선물/환율/유가/미국주식)
import feedparser               # 구글뉴스 RSS 파싱(종목 뉴스 헤드라인)
import urllib.parse             # 뉴스 검색어 URL 인코딩

# ──────────────────────────────────────────────────────────────────
# 0. 로깅 설정 : 실행 이력과 에러를 파일로 남겨 장애 추적을 가능하게 함
# ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    filename="market_mentor.log",                 # 로그 파일 경로
    level=logging.INFO,                            # INFO 이상 기록
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)                  # 모듈 전용 로거

# ──────────────────────────────────────────────────────────────────
# 1. CONFIG : 보유종목 + 발송 채널 설정 (사용자가 수정하는 유일한 구역)
# ──────────────────────────────────────────────────────────────────

# 보유종목 정의 : (표시명, yfinance 티커, 매입금액, 뉴스검색어)
# - 국내 ETF는 yfinance 미지원이라 추종 미국 원지수 티커로 대체(뉴스용으로 활용)
# - 매입금액은 손익 집계 및 비중 계산에 사용
HOLDINGS = [
    # 계좌1: ISA (6386)
    ("KODEX 미국휴머노이드로봇", "BOTZ",  2_533_000,  "humanoid robot stocks"),
    ("1Q 미국우주항공테크",      "ITA",   6_665_620,  "aerospace defense stocks"),
    ("KODEX 미국나스닥100",      "^NDX",  19_329_555, "Nasdaq 100"),
    ("SOL 미국AI소프트웨어",     "IGV",   20_631_195, "AI software stocks"),
    # 계좌2: 위탁종합 (6362) — 미국 직접보유
    ("DELL",                     "DELL",  702_341,    "Dell Technologies"),
    ("SOXL (반도체 3배)",        "SOXL",  1_601_028,  "semiconductor ETF SOXL"),
    ("TQQQ (나스닥 3배)",        "TQQQ",  9_389_851,  "Nasdaq TQQQ"),
    ("TSLL (테슬라 2배)",        "TSLL",  3_219_439,  "Tesla stock"),
    # 계좌3: 위탁종합 (6466) — 국내주식
    ("SK하이닉스",               "000660.KS", 7_146_000, "SK하이닉스"),
    ("삼성전자",                 "005930.KS", 5_208_000, "삼성전자 반도체"),
    ("NAVER",                    "035420.KS", 5_337_000, "네이버 NAVER 실적"),
]

# 매크로 지표 : (표시명, yfinance 티커, 단위포맷)
MACRO = [
    ("S&P 500",      "^GSPC", "{:,.2f}"),
    ("나스닥",        "^IXIC", "{:,.2f}"),
    ("다우",          "^DJI",  "{:,.2f}"),
    ("필라델피아 반도체", "SOXX", "{:,.2f}"),  # ^SOX 미지원 환경 대비 추종 ETF
]
FUTURES = [
    ("S&P500 선물",   "ES=F", "{:,.2f}"),
    ("나스닥 선물",    "NQ=F", "{:,.2f}"),
]
MACRO_2 = [
    ("원/달러 환율",   "KRW=X", "{:,.2f}"),
    ("美 10년물 금리", "^TNX",  "{:.3f}%"),
    ("WTI 유가",       "CL=F",  "${:,.2f}"),
    ("금",             "GC=F",  "${:,.2f}"),
]

# 섹터 정의 : (섹터명, 대표ETF티커, 뉴스검색어, 트랙)
#   - track="A" : 보유 포트폴리오 연관 섹터(포지션 직접 영향)
#   - track="B" : 미보유 섹터(시장 전반·자금 로테이션 모니터링)
#   - 대표 ETF 등락률로 "오늘 어느 섹터로 돈이 도는가"를 포착
SECTORS = [
    # ── TRACK A : 보유 연관 ──
    ("반도체",        "SOXX", "반도체 HBM 엔비디아 메모리",       "A"),
    ("AI·소프트웨어", "IGV",  "AI 인공지능 소프트웨어 클라우드",  "A"),
    ("빅테크·나스닥", "QQQ",  "빅테크 나스닥 실적 매그니피센트7", "A"),
    ("우주항공·방산", "ITA",  "방산 우주항공 수주 미사일",        "A"),
    ("로보틱스",      "BOTZ", "휴머노이드 로봇 자동화 피지컬AI",  "A"),
    ("전기차·2차전지", "DRIV", "전기차 테슬라 2차전지 배터리",     "A"),
    # ── TRACK B : 미보유(시장 전반) ──
    ("헬스케어·바이오", "XLV", "헬스케어 제약 바이오 FDA 신약",   "B"),
    ("금융",          "XLF",  "은행 금융 금리 연준 실적",         "B"),
    ("에너지",        "XLE",  "에너지 원유 정유 OPEC",            "B"),
    ("소비재",        "XLY",  "소비 리테일 소비심리 유통",        "B"),
    ("산업재",        "XLI",  "산업재 제조 인프라 항공",          "B"),
    ("부동산·리츠",   "XLRE", "부동산 리츠 상업용부동산 금리",    "B"),
    ("원자재·소재",   "XLB",  "구리 원자재 소재 광물 리튬",       "B"),
]

# 발송 채널 설정 : 환경변수에서 읽음 (코드에 비밀번호 노출 금지)
#   export GMAIL_USER="you@gmail.com"
#   export GMAIL_APP_PW="앱비밀번호16자리"   ← Gmail 2단계인증 후 발급
#   export MAIL_TO="받는주소@gmail.com"
#   export TG_TOKEN="텔레그램봇토큰"   (선택)
#   export TG_CHAT="텔레그램chat_id"   (선택)
CFG = {
    "gmail_user": os.getenv("GMAIL_USER", ""),
    "gmail_pw":   os.getenv("GMAIL_APP_PW", ""),
    "mail_to":    os.getenv("MAIL_TO", ""),
    "tg_token":   os.getenv("TG_TOKEN", ""),
    "tg_chat":    os.getenv("TG_CHAT", ""),
    "discord_webhook": os.getenv("DISCORD_WEBHOOK", ""),  # Discord 채널 웹훅 URL
    "slack_webhook":   os.getenv("SLACK_WEBHOOK", ""),    # Slack Incoming Webhook URL
}

# ──────────────────────────────────────────────────────────────────
# 2. 데이터 수집 함수
# ──────────────────────────────────────────────────────────────────

def fetch_quote(ticker: str) -> dict:
    """
    단일 티커의 전일 종가/등락률을 안전하게 조회.
    실패 시 None 값을 담은 dict 반환(graceful degrade) → 전체 중단 방지.
    """
    try:
        # period="5d" : 직전 영업일 비교를 위해 넉넉히 5일치 확보
        hist = yf.Ticker(ticker).history(period="5d", auto_adjust=False)
        if hist.empty or len(hist) < 2:            # 데이터 유효성 검사
            raise ValueError("insufficient data")
        last = hist["Close"].iloc[-1]              # 가장 최근 종가
        prev = hist["Close"].iloc[-2]              # 직전 종가
        chg_pct = (last - prev) / prev * 100       # 등락률(%)
        return {"ticker": ticker, "last": float(last),
                "chg": float(chg_pct), "ok": True}
    except Exception as e:                         # 네트워크/티커오류 등 전부 포착
        log.warning(f"fetch_quote 실패 [{ticker}]: {e}")
        return {"ticker": ticker, "last": None, "chg": None, "ok": False}


def fetch_news(query: str, limit: int = 2) -> list:
    """
    구글뉴스 RSS에서 검색어 기반 최신 헤드라인 수집.
    한국어 결과 우선(hl=ko). 실패 시 빈 리스트 반환.
    """
    try:
        q = urllib.parse.quote(query)              # 검색어 URL 인코딩
        url = (f"https://news.google.com/rss/search?q={q}"
               f"&hl=ko&gl=KR&ceid=KR:ko")         # 한국 로케일 RSS
        feed = feedparser.parse(url)               # RSS 파싱
        # 상위 limit개 제목만 추출(중복 헤드라인 방지용 set 미사용: 순서 유지)
        return [entry.title for entry in feed.entries[:limit]]
    except Exception as e:
        log.warning(f"fetch_news 실패 [{query}]: {e}")
        return []


def collect_all() -> dict:
    """
    모든 데이터를 병렬로 수집(ThreadPoolExecutor).
    네트워크 I/O 바운드 작업이므로 스레드 병렬화로 수집시간 단축.
    """
    result = {"holdings": [], "macro": {}, "futures": {},
              "macro2": {}, "news": {}, "sectors": []}

    # --- (a) 시세류 일괄 수집 : 보유+매크로+선물 티커를 한 번에 풀에 투입 ---
    quote_tasks = {}
    with ThreadPoolExecutor(max_workers=8) as ex:  # 동시 8스레드(과도한 요청 방지)
        # 보유종목
        for name, tk, buy, q in HOLDINGS:
            quote_tasks[ex.submit(fetch_quote, tk)] = ("hold", name, tk, buy, q)
        # 매크로 지수
        for name, tk, fmt in MACRO + FUTURES + MACRO_2:
            quote_tasks[ex.submit(fetch_quote, tk)] = ("macro", name, tk, fmt, None)

        for fut in as_completed(quote_tasks):       # 완료되는 순서대로 회수
            kind, name, tk, *rest = quote_tasks[fut]
            data = fut.result()
            if kind == "hold":
                buy, q = rest
                result["holdings"].append(
                    {"name": name, "ticker": tk, "buy": buy,
                     "query": q, **data})
            else:
                # 매크로/선물/2차매크로 구분하여 저장
                if tk in [t for _, t, _ in FUTURES]:
                    result["futures"][name] = data
                elif tk in [t for _, t, _ in MACRO]:
                    result["macro"][name] = data
                else:
                    result["macro2"][name] = data

    # --- (b) 뉴스 수집 : 손익 영향이 큰 상위 4종목만(요청량 절약) ---
    #     매입금액 큰 순 = 비중 큰 순으로 정렬 후 상위 4개 뉴스 조회
    top4 = sorted(result["holdings"], key=lambda x: x["buy"], reverse=True)[:4]
    with ThreadPoolExecutor(max_workers=4) as ex:
        news_tasks = {ex.submit(fetch_news, h["query"]): h["name"] for h in top4}
        for fut in as_completed(news_tasks):
            result["news"][news_tasks[fut]] = fut.result()

    # --- (c) 섹터 수집 : 대표 ETF 등락 + 섹터 뉴스(1건씩) ---
    #     보유(A)/미보유(B) 전 섹터를 커버하여 시장 전반과 자금 로테이션 파악.
    #     ETF 시세와 뉴스를 각 섹터별로 병렬 수집한 뒤 하나의 레코드로 병합.
    sec_quote = {}   # ETF 등락 수집 태스크
    sec_news = {}    # 섹터 뉴스 수집 태스크
    with ThreadPoolExecutor(max_workers=8) as ex:
        for sname, etf, query, track in SECTORS:
            sec_quote[ex.submit(fetch_quote, etf)] = sname          # ETF 등락
            sec_news[ex.submit(fetch_news, query, 1)] = sname       # 헤드라인 1건
        quote_map, news_map = {}, {}                                # 섹터명→결과 매핑
        for fut in as_completed(sec_quote):
            quote_map[sec_quote[fut]] = fut.result()
        for fut in as_completed(sec_news):
            news_map[sec_news[fut]] = fut.result()
    # 정의 순서(SECTORS) 유지하며 등락+뉴스+트랙 병합
    for sname, etf, query, track in SECTORS:
        q = quote_map.get(sname, {"chg": None})
        result["sectors"].append({
            "name": sname, "etf": etf, "track": track,
            "chg": q.get("chg"), "ok": q.get("ok", False),
            "news": news_map.get(sname, []),
        })

    return result


# ──────────────────────────────────────────────────────────────────
# 3. 브리핑 빌드 (균형 애널리스트 어조)
# ──────────────────────────────────────────────────────────────────

def build_briefing(data: dict) -> str:
    """수집 데이터를 HTML 이메일 본문으로 가공. (와이어프레임 리디자인 버전)"""
    today = dt.datetime.now().strftime("%Y년 %m월 %d일 (%a)")

    # --- 공통 헬퍼 ---
    def color(v):
        """등락 부호별 색상 (빨강=상승, 파랑=하락, 한국식)"""
        if v is None: return "#888888"
        return "#c62828" if v >= 0 else "#1565c0"

    def fmt_chg(v):
        """등락률 포맷. None → N/A"""
        return "N/A" if v is None else f"{v:+.2f}%"

    def arrow(v):
        """등락 화살표. None → —"""
        if v is None: return "—"
        return "▲" if v >= 0 else "▼"

    def chg_cell(v):
        """색상+화살표+수치 인라인 span"""
        if v is None:
            return "<span style='color:#888;'>N/A</span>"
        c = color(v)
        return (f"<span style='color:{c};font-weight:600;'>"
                f"{arrow(v)} {abs(v):.2f}%</span>")

    # --- 보유종목 집계 ---
    total_buy = sum(h["buy"] for h in data["holdings"])
    holds = sorted(data["holdings"],
                   key=lambda x: (x["chg"] if x["ok"] else -999), reverse=True)

    # --- 포트폴리오 가중평균 등락 ---
    weighted, w_sum = 0.0, 0
    for h in data["holdings"]:
        if h["ok"] and h["chg"] is not None:
            weighted += h["chg"] * h["buy"]
            w_sum += h["buy"]
    port_chg = (weighted / w_sum) if w_sum else None

    # --- 레버리지 비중 ---
    lev_buy = sum(h["buy"] for h in data["holdings"]
                  if any(k in h["name"] for k in ["3배", "2배"]))
    lev_ratio = lev_buy / total_buy * 100

    # --- 대표 지표 추출 ---
    nasdaq  = data.get("macro",  {}).get("나스닥",       {})
    sox     = data.get("macro",  {}).get("필라델피아 반도체", {})
    fx      = data.get("macro2", {}).get("원/달러 환율",  {})
    wti     = data.get("macro2", {}).get("WTI 유가",      {})
    tnx     = data.get("macro2", {}).get("美 10년물 금리",{})
    gold    = data.get("macro2", {}).get("금",            {})

    def val_str(d, fmt="{:,.2f}"):
        """종가 값 포맷. 없으면 N/A"""
        return fmt.format(d["last"]) if d.get("ok") and d.get("last") else "N/A"

    fx_str  = f"{fx['last']:,.0f}원" if fx.get("ok") and fx.get("last") else "N/A"
    port_arrow = arrow(port_chg)
    port_color = color(port_chg)
    port_str   = f"{port_arrow} {abs(port_chg):.2f}%" if port_chg is not None else "N/A"

    # ── BLOCK A: 다크 히어로 헤더 ──────────────────────────────────────────
    hero = f"""
    <table width="100%" cellpadding="0" cellspacing="0"
           style="background:#111111;border-radius:8px 8px 0 0;margin-bottom:2px;">
      <tr>
        <td style="padding:28px 28px 10px;">
          <p style="margin:0;font-size:11px;color:#666;letter-spacing:.1em;
                    text-transform:uppercase;">Market Mentor &nbsp;·&nbsp; {today}</p>
        </td>
        <td style="padding:28px 28px 10px;text-align:right;vertical-align:top;">
          <p style="margin:0;font-size:11px;color:#444;letter-spacing:.06em;">
            포트폴리오 &nbsp;·&nbsp; 시황 &nbsp;·&nbsp; 섹터 &nbsp;·&nbsp; 매크로</p>
        </td>
      </tr>
      <tr>
        <td colspan="2" style="padding:8px 28px 24px;">
          <p style="margin:0 0 6px;font-size:13px;color:#666;">내 자산 전일 등락</p>
          <p style="margin:0;font-size:44px;font-weight:500;color:{port_color};
                    letter-spacing:-.02em;line-height:1;">{port_str}</p>
          <table style="margin-top:18px;" cellpadding="0" cellspacing="0">
            <tr>
              <td style="padding-right:24px;">
                <p style="margin:0;font-size:12px;color:#555;">나스닥</p>
                <p style="margin:2px 0 0;font-size:14px;color:{color(nasdaq.get('chg'))};
                           font-weight:500;">{chg_cell(nasdaq.get('chg'))}</p>
              </td>
              <td style="padding-right:24px;">
                <p style="margin:0;font-size:12px;color:#555;">원/달러</p>
                <p style="margin:2px 0 0;font-size:14px;color:#ccc;font-weight:500;">
                  {fx_str}</p>
              </td>
              <td>
                <p style="margin:0;font-size:12px;color:#555;">WTI</p>
                <p style="margin:2px 0 0;font-size:14px;color:{color(wti.get('chg'))};
                           font-weight:500;">{chg_cell(wti.get('chg'))}</p>
              </td>
            </tr>
          </table>
        </td>
      </tr>
    </table>"""

    # ── BLOCK B: Stat 카드 3개 ─────────────────────────────────────────────
    def stat_card(label, value_str, sub, bg="#111111"):
        return f"""
        <td width="33%" style="padding:1px;">
          <table width="100%" cellpadding="0" cellspacing="0"
                 style="background:{bg};border-radius:0;">
            <tr><td style="padding:18px 16px 16px;text-align:center;">
              <p style="margin:0 0 8px;font-size:10px;color:#555;
                        letter-spacing:.1em;text-transform:uppercase;">{label}</p>
              <p style="margin:0;font-size:26px;font-weight:500;">{value_str}</p>
              <p style="margin:6px 0 0;font-size:10px;color:#444;">{sub}</p>
            </td></tr>
          </table>
        </td>"""

    lev_color  = "#c62828" if lev_ratio >= 20 else "#888"
    lev_html   = f"<span style='color:{lev_color};'>{lev_ratio:.1f}%</span>"
    nas_chg    = nasdaq.get("chg")                    # 백슬래시 없이 변수로
    sox_chg    = sox.get("chg")
    nas_html   = f"<span style='color:{color(nas_chg)};'>{chg_cell(nas_chg)}</span>"
    sox_html   = f"<span style='color:{color(sox_chg)};'>{chg_cell(sox_chg)}</span>"

    stat_row = f"""
    <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:2px;">
      <tr>
        {stat_card("레버리지 비중", lev_html, "SOXL · TQQQ · TSLL")}
        {stat_card("나스닥", nas_html, val_str(nasdaq))}
        {stat_card("필라델피아 반도체", sox_html, val_str(sox))}
      </tr>
    </table>"""

    # ── BLOCK C: 보유종목 등락 리스트 ─────────────────────────────────────
    hold_rows = ""
    for i, h in enumerate(holds):
        bg = "#fafafa" if i % 2 == 0 else "#ffffff"
        hold_rows += (
            f"<tr style='background:{bg};'>"
            f"<td style='padding:9px 16px;font-size:14px;color:#222;'>{h['name']}</td>"
            f"<td style='padding:9px 16px;text-align:right;font-size:14px;'>"
            f"{chg_cell(h['chg'])}</td>"
            f"</tr>"
        )

    holdings_block = f"""
    <table width="100%" cellpadding="0" cellspacing="0"
           style="background:#ffffff;margin-bottom:2px;">
      <tr>
        <td style="padding:16px 16px 10px;">
          <p style="margin:0;font-size:10px;color:#999;letter-spacing:.1em;
                    text-transform:uppercase;">1 — 보유종목 전일 등락</p>
        </td>
      </tr>
      <tr><td style="padding:0;">
        <table width="100%" cellpadding="0" cellspacing="0">{hold_rows}</table>
      </td></tr>
      <tr><td style="padding:10px 16px 16px;">
        <p style="margin:0;font-size:11px;color:#bbb;">등락순 정렬</p>
      </td></tr>
    </table>"""

    # ── BLOCK D: 핵심 뉴스 카드 2×2 ──────────────────────────────────────
    news_items = list(data.get("news", {}).items())
    # 최대 4종목, 2열로 배치
    def news_card(name, items):
        lines = ""
        for t in (items or [])[:2]:
            lines += (f"<p style='margin:0 0 8px;font-size:13px;color:#333;"
                      f"line-height:1.55;border-left:2px solid #111;"
                      f"padding-left:10px;'>{t}</p>")
        if not lines:
            lines = "<p style='font-size:13px;color:#bbb;'>관련 뉴스 없음</p>"
        return f"""
        <td width="50%" style="padding:1px;vertical-align:top;">
          <table width="100%" cellpadding="0" cellspacing="0"
                 style="background:#ffffff;height:100%;">
            <tr><td style="padding:16px;">
              <p style="margin:0 0 10px;font-size:10px;color:#999;
                        letter-spacing:.1em;text-transform:uppercase;">{name}</p>
              {lines}
            </td></tr>
          </table>
        </td>"""

    news_cells = ""
    for idx, (nm, items) in enumerate(news_items[:4]):
        news_cells += news_card(nm, items)
        if idx == 1:   # 2열 후 줄 바꿈
            news_cells += "</tr><tr>"

    news_block = f"""
    <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:2px;">
      <tr>
        <td colspan="2" style="padding:16px 16px 8px;background:#ffffff;">
          <p style="margin:0;font-size:10px;color:#999;letter-spacing:.1em;
                    text-transform:uppercase;">2 — 핵심 종목 뉴스</p>
        </td>
      </tr>
      <tr>{news_cells}</tr>
    </table>"""

    # ── BLOCK E: 미국증시 + 매크로 (2열) ─────────────────────────────────
    def idx_row(name, d):
        return (f"<tr style='border-bottom:1px solid #f0f0f0;'>"
                f"<td style='padding:8px 16px;font-size:13px;color:#444;'>{name}</td>"
                f"<td style='padding:8px 4px;font-size:13px;text-align:right;"
                f"color:#666;'>{val_str(d)}</td>"
                f"<td style='padding:8px 16px;font-size:13px;text-align:right;'>"
                f"{chg_cell(d.get('chg'))}</td></tr>")

    us_tbl = ""
    for name, tk, fmt in MACRO:
        us_tbl += idx_row(name, data["macro"].get(name, {}))
    for name, tk, fmt in FUTURES:
        us_tbl += idx_row(name, data["futures"].get(name, {}))

    macro_tbl = ""
    for name, tk, fmt in MACRO_2:
        macro_tbl += idx_row(name, data["macro2"].get(name, {}))

    market_block = f"""
    <table width="100%" cellpadding="0" cellspacing="0"
           style="background:#ffffff;margin-bottom:2px;">
      <tr>
        <td width="50%" style="vertical-align:top;padding:1px;">
          <table width="100%" cellpadding="0" cellspacing="0"
                 style="background:#ffffff;height:100%;">
            <tr><td style="padding:16px 16px 8px;">
              <p style="margin:0;font-size:10px;color:#999;letter-spacing:.1em;
                        text-transform:uppercase;">3 — 미국증시 &amp; 선물</p>
            </td></tr>
            <tr><td><table width="100%" cellpadding="0" cellspacing="0">
              {us_tbl}
            </table></td></tr>
          </table>
        </td>
        <td width="50%" style="vertical-align:top;padding:1px;">
          <table width="100%" cellpadding="0" cellspacing="0"
                 style="background:#ffffff;height:100%;">
            <tr><td style="padding:16px 16px 8px;">
              <p style="margin:0;font-size:10px;color:#999;letter-spacing:.1em;
                        text-transform:uppercase;">4 — 매크로 지표</p>
            </td></tr>
            <tr><td><table width="100%" cellpadding="0" cellspacing="0">
              {macro_tbl}
            </table></td></tr>
          </table>
        </td>
      </tr>
    </table>"""

    # ── BLOCK F: 섹터 자금 로테이션 카드 그리드 ───────────────────────────
    secs = data.get("sectors", [])
    secs_sorted = sorted(secs,
                         key=lambda x: (x["chg"] if x["ok"] else -999), reverse=True)

    def sec_card(s):
        tag      = "◆" if s["track"] == "A" else "◇"
        hl       = s["news"][0][:40] + "…" if s.get("news") else "—"
        sec_col  = color(s["chg"])                    # 백슬래시 회피
        sec_chg  = fmt_chg(s["chg"])
        return (f"<td width='25%' style='padding:1px;vertical-align:top;'>"
                f"<table width='100%' cellpadding='0' cellspacing='0'"
                f" style='background:#ffffff;'>"
                f"<tr><td style='padding:14px 12px;'>"
                f"<p style='margin:0 0 4px;font-size:10px;color:#aaa;'>"
                f"{tag} {s['name']}</p>"
                f"<p style='margin:0 0 8px;font-size:20px;font-weight:500;"
                f"color:{sec_col};'>{sec_chg}</p>"
                f"<p style='margin:0;font-size:11px;color:#888;line-height:1.5;'>{hl}</p>"
                f"</td></tr></table></td>")

    sec_cells = ""
    for i, s in enumerate(secs_sorted):
        sec_cells += sec_card(s)
        if (i + 1) % 4 == 0 and i + 1 < len(secs_sorted):
            sec_cells += "</tr><tr>"

    # 자금 로테이션 코멘트
    valid = [s for s in secs_sorted if s["ok"]]
    if len(valid) >= 2:
        top_s, bot_s = valid[0], valid[-1]
        rotation = (f"<b>{top_s['name']}</b>({fmt_chg(top_s['chg'])}) 최강 &nbsp;/&nbsp; "
                    f"<b>{bot_s['name']}</b>({fmt_chg(bot_s['chg'])}) 최약. ")
        b_top = [s for s in valid[:3] if s["track"] == "B"]
        if b_top:
            names = ", ".join(s["name"] for s in b_top)
            rotation += (f"방어/가치 섹터({names})로 자금 유입 감지 → "
                         "레버리지 비중 점검 권고.")
        else:
            rotation += "기술·성장 섹터 주도 흐름 — 현 포트폴리오 방향 부합."
    else:
        rotation = "섹터 데이터 수집 제한 (네트워크 확인 필요)."

    sector_block = f"""
    <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:2px;">
      <tr>
        <td colspan="4" style="padding:16px 16px 8px;background:#ffffff;">
          <p style="margin:0;font-size:10px;color:#999;letter-spacing:.1em;
                    text-transform:uppercase;">5 — 섹터 자금 로테이션</p>
        </td>
      </tr>
      <tr>{sec_cells}</tr>
      <tr>
        <td colspan="4" style="padding:12px 16px;background:#f5f5f5;">
          <p style="margin:0;font-size:12px;color:#555;line-height:1.6;">
            {rotation}</p>
        </td>
      </tr>
    </table>"""

    # ── BLOCK G: 다크 멘토 코멘트 ─────────────────────────────────────────
    comment = (
        f"현재 레버리지 ETF 비중 <b style='color:#ff6b6b;'>{lev_ratio:.1f}%</b>. "
        "변동성 확대 구간에서는 손익 진폭이 원지수 대비 2~3배로 증폭됩니다. "
        "전일 종가 + 금일 선물 방향을 함께 확인한 뒤 비중을 점검하시기 바랍니다. "
        "국내 반도체(SK하이닉스·삼성전자)는 필라델피아 반도체 지수와 동조하므로 "
        "상단 SOX 등락률을 선행지표로 활용하세요."
    )
    mentor_block = f"""
    <table width="100%" cellpadding="0" cellspacing="0"
           style="background:#111111;margin-bottom:2px;">
      <tr><td style="padding:20px 24px;">
        <p style="margin:0 0 8px;font-size:10px;color:#555;
                  letter-spacing:.1em;text-transform:uppercase;">멘토 코멘트</p>
        <p style="margin:0;font-size:14px;color:#cccccc;line-height:1.75;">
          {comment}</p>
      </td></tr>
    </table>"""

    # ── BLOCK H: 3단 푸터 ─────────────────────────────────────────────────
    footer = """
    <table width="100%" cellpadding="0" cellspacing="0"
           style="background:#ffffff;border-radius:0 0 8px 8px;">
      <tr>
        <td width="33%" style="padding:16px 18px;vertical-align:top;
                               border-top:1px solid #eeeeee;">
          <p style="margin:0 0 8px;font-size:11px;font-weight:500;color:#222;">포트폴리오</p>
          <p style="margin:0;font-size:12px;color:#888;line-height:1.9;">
            ISA (6386)<br>위탁 (6362)<br>위탁 (6466)</p>
        </td>
        <td width="33%" style="padding:16px 18px;vertical-align:top;
                               border-top:1px solid #eeeeee;">
          <p style="margin:0 0 8px;font-size:11px;font-weight:500;color:#222;">데이터 소스</p>
          <p style="margin:0;font-size:12px;color:#888;line-height:1.9;">
            yfinance<br>Google News RSS<br>실시간 선물</p>
        </td>
        <td width="33%" style="padding:16px 18px;vertical-align:top;
                               border-top:1px solid #eeeeee;">
          <p style="margin:0 0 8px;font-size:11px;font-weight:500;color:#222;">발송</p>
          <p style="margin:0;font-size:12px;color:#888;line-height:1.9;">
            매일 오전 06:30<br>Gmail · 텔레그램<br>투자 권유 아님</p>
        </td>
      </tr>
    </table>"""

    # ── 전체 조립 (외부 wrapper : 620px 고정, 배경 #f2f2f2) ──────────────
    html = f"""
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
      @media only screen and (max-width:480px) {{
        .mm td {{ font-size:15px !important; padding:10px 12px !important; }}
        .mm p  {{ font-size:13px !important; }}
      }}
    </style>
    <div class="mm" style="background:#f2f2f2;padding:8px;">
      <table width="100%" cellpadding="0" cellspacing="0"
             style="max-width:620px;margin:0 auto;border-collapse:collapse;">
        <tr><td>
          {hero}
          {stat_row}
          {holdings_block}
          {news_block}
          {market_block}
          {sector_block}
          {mentor_block}
          {footer}
        </td></tr>
      </table>
    </div>"""
    return html


def build_text(data: dict) -> str:
    """
    메신저(텔레그램/Discord/Slack)용 모바일 친화 평문 브리핑.
    HTML 태그 제거 방식 대신 수집 데이터에서 직접 재구성 →
    종목당 한 줄, 정렬·구분선으로 좁은 화면에서도 구조가 유지됨.
    """
    import datetime as _dt
    today = _dt.datetime.now().strftime("%Y-%m-%d (%a)")

    def chg_str(v, ok):
        """등락률을 부호+화살표로. 모바일에서 색 대신 기호로 방향 표시."""
        if not ok or v is None:
            return "  N/A"
        arrow = "🔺" if v >= 0 else "🔻"
        return f"{arrow}{v:+.2f}%"

    L = []                                  # 줄 단위 누적(라인 리스트)
    L.append(f"☕ Market Mentor  {today}")

    # ── 한 줄 요약 (맨 위, 알림 프리뷰 대응) ──
    weighted, w_sum = 0.0, 0
    for h in data["holdings"]:
        if h["ok"] and h["chg"] is not None:
            weighted += h["chg"] * h["buy"]
            w_sum += h["buy"]
    port_chg = (weighted / w_sum) if w_sum else None
    nas = data.get("macro", {}).get("나스닥", {})
    fx = data.get("macro2", {}).get("원/달러 환율", {})
    p = f"{'🔺' if (port_chg or 0) >= 0 else '🔻'}{port_chg:+.2f}%" if port_chg is not None else "N/A"
    n = f"{nas['chg']:+.2f}%" if nas.get("ok") else "N/A"
    f_ = f"{fx['last']:,.0f}원" if fx.get("ok") and fx.get("last") else "N/A"
    L.append(f"내 자산 {p} | 나스닥 {n} | 환율 {f_}")
    L.append("━━━━━━━━━━━━━━━━")

    # ── 1. 보유종목 등락 (등락순) ──
    L.append("📊 내 보유종목")
    holds = sorted(data["holdings"],
                   key=lambda x: (x["chg"] if x["ok"] else -999), reverse=True)
    for h in holds:
        # 종목명 12자로 고정 정렬 → 등락률 열이 세로로 가지런
        name = (h["name"][:11] + "…") if len(h["name"]) > 12 else h["name"].ljust(12)
        L.append(f"{name} {chg_str(h['chg'], h['ok'])}")

    # ── 2. 핵심 종목 뉴스 ──
    news_lines = []
    for nm, items in data.get("news", {}).items():
        if items:
            news_lines.append(f"• {nm}")
            for t in items:
                news_lines.append(f"  - {t[:60]}")   # 헤드라인 60자 컷
    if news_lines:
        L.append("")
        L.append("📰 핵심 뉴스")
        L.extend(news_lines)

    # ── 3. 미국증시 + 매크로 ──
    def macro_block(title, dct, names):
        out = [title]
        for n in names:
            d = dct.get(n, {"chg": None, "ok": False})
            out.append(f"{n.ljust(10)} {chg_str(d.get('chg'), d.get('ok', False))}")
        return out
    L.append("")
    L.extend(macro_block("🇺🇸 전일 미국증시",
                         data.get("macro", {}), [m[0] for m in MACRO]))
    L.append("")
    L.extend(macro_block("🌐 매크로",
                         data.get("macro2", {}), [m[0] for m in MACRO_2]))

    # ── 4. 섹터 (등락순, 트랙 구분) ──
    secs = sorted(data.get("sectors", []),
                  key=lambda x: (x["chg"] if x["ok"] else -999), reverse=True)
    if secs:
        L.append("")
        L.append("🏭 섹터 (강세순)")
        for s in secs:
            tag = "◆" if s["track"] == "A" else "◇"   # ◆보유연관 ◇시장전반
            nm = s["name"][:9].ljust(9)
            L.append(f"{tag}{nm} {chg_str(s['chg'], s['ok'])}")

    L.append("━━━━━━━━━━━━━━━━")
    L.append("※ 참고자료이며 투자권유 아님")
    return "\n".join(L)


# ──────────────────────────────────────────────────────────────────
# 4. 발송 함수
# ──────────────────────────────────────────────────────────────────

def send_email(html: str) -> bool:
    """Gmail SMTP로 HTML 메일 발송. 설정 미비 시 건너뜀."""
    if not (CFG["gmail_user"] and CFG["gmail_pw"] and CFG["mail_to"]):
        log.info("이메일 설정 미비 → 발송 생략")
        return False
    try:
        msg = MIMEMultipart("alternative")
        today = dt.datetime.now().strftime("%m/%d")
        msg["Subject"] = f"☕ [{today}] Market Mentor 모닝 브리핑"
        msg["From"] = CFG["gmail_user"]
        msg["To"] = CFG["mail_to"]
        msg.attach(MIMEText(html, "html", "utf-8"))   # HTML 파트 첨부
        # Gmail SSL 포트 465 사용
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=30) as s:
            s.login(CFG["gmail_user"], CFG["gmail_pw"])
            s.send_message(msg)
        log.info("이메일 발송 성공")
        return True
    except Exception as e:
        log.error(f"이메일 발송 실패: {e}")
        return False


def send_telegram(html: str) -> bool:
    """텔레그램 봇으로 발송(HTML 일부 태그만 지원되므로 요약 텍스트 변환)."""
    if not (CFG["tg_token"] and CFG["tg_chat"]):
        log.info("텔레그램 설정 미비 → 발송 생략")
        return False
    try:
        import requests
        # 모바일 친화 평문(text)을 그대로 전송. 4096자 한도 → 안전하게 컷
        resp = requests.post(
            f"https://api.telegram.org/bot{CFG['tg_token']}/sendMessage",
            data={"chat_id": CFG["tg_chat"], "text": text[:4000]},
            timeout=20,
        )
        resp.raise_for_status()
        log.info("텔레그램 발송 성공")
        return True
    except Exception as e:
        log.error(f"텔레그램 발송 실패: {e}")
        return False


def send_discord(text: str) -> bool:
    """Discord 채널 웹훅으로 발송. 웹훅 URL 하나만 있으면 됨(토큰 불필요)."""
    if not CFG["discord_webhook"]:
        log.info("Discord 설정 미비 → 발송 생략")
        return False
    try:
        import requests
        # Discord 본문 한도 2000자. 코드블록으로 감싸면 등간격 폰트라 정렬 유지
        body = "```\n" + text[:1900] + "\n```"
        resp = requests.post(
            CFG["discord_webhook"],
            json={"content": body},
            timeout=20,
        )
        resp.raise_for_status()
        log.info("Discord 발송 성공")
        return True
    except Exception as e:
        log.error(f"Discord 발송 실패: {e}")
        return False


def send_slack(text: str) -> bool:
    """Slack Incoming Webhook으로 발송. 웹훅 URL 하나만 있으면 됨."""
    if not CFG["slack_webhook"]:
        log.info("Slack 설정 미비 → 발송 생략")
        return False
    try:
        import requests
        # Slack도 코드블록(```)으로 감싸 등폭 정렬 유지
        body = "```\n" + text[:3500] + "\n```"
        resp = requests.post(
            CFG["slack_webhook"],
            json={"text": body},
            timeout=20,
        )
        resp.raise_for_status()
        log.info("Slack 발송 성공")
        return True
    except Exception as e:
        log.error(f"Slack 발송 실패: {e}")
        return False


# ──────────────────────────────────────────────────────────────────
# 5. 메인 실행
# ──────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 50)
    log.info("Market Mentor 실행 시작")
    try:
        data = collect_all()                # 데이터 수집
        html = build_briefing(data)         # 이메일용 HTML 브리핑
        text = build_text(data)             # 메신저용 모바일 친화 평문
        # 디버그/백업용 로컬 저장(발송 실패해도 결과 보존)
        with open("latest_briefing.html", "w", encoding="utf-8") as f:
            f.write(html)
        with open("latest_briefing.txt", "w", encoding="utf-8") as f:
            f.write(text)                   # 메신저 출력 미리보기용
        sent_mail = send_email(html)        # 메일: HTML
        sent_tg = send_telegram(text)       # 텔레그램: 평문
        sent_dc = send_discord(text)        # Discord: 평문(코드블록)
        sent_sl = send_slack(text)          # Slack: 평문(코드블록)
        if not (sent_mail or sent_tg or sent_dc or sent_sl):  # 전 채널 실패 시
            log.warning("발송 채널 없음 → latest_briefing.html 만 생성됨")
            print("발송 설정이 없습니다. latest_briefing.html 파일을 확인하세요.")
        else:
            print("브리핑 발송 완료.")
        log.info("Market Mentor 실행 종료(정상)")
    except Exception as e:                  # 최상위 예외 포착(스케줄러 중단 방지)
        log.critical(f"치명적 오류: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
