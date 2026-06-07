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

def build_column(data: dict) -> str:
    """
    에디 스타일 투자 칼럼 자동 생성.
    짧고 강렬한 문장, 컬러 수치, 스토리 흐름: 사건 → 원인 → 종목 명암 → 액션.
    """
    # ── 스타일 헬퍼 ───────────────────────────────────────────────────────
    def neg(t):  return f"<strong style='color:#e53935;'>{t}</strong>"
    def pos(t):  return f"<strong style='color:#43a047;'>{t}</strong>"
    def em(t):   return f"<strong>{t}</strong>"
    def p(t, size=15, mb=14, col="#222"):
        return (f"<p style='margin:0 0 {mb}px;font-size:{size}px;"
                f"color:{col};line-height:1.9;'>{t}</p>")
    def hr():
        return "<hr style='border:none;border-top:1px solid #eee;margin:18px 0;'>"
    def badge(items):
        return "".join(
            f"<p style='margin:0 0 8px;font-size:14px;color:#222;line-height:1.75;'>"
            f"<span style='color:#777;margin-right:5px;'>▍</span>{it}</p>"
            for it in items)
    def sc(v):
        """에디 스타일 수치: 음수=빨강, 양수=초록"""
        if v is None: return "N/A"
        s = f"{v:+.2f}%"
        return neg(s) if v < 0 else pos(s)

    # ── 핵심 지표 추출 ─────────────────────────────────────────────────────
    nasdaq  = data.get("macro",  {}).get("나스닥",            {})
    sp500   = data.get("macro",  {}).get("S&P 500",           {})
    sox     = data.get("macro",  {}).get("필라델피아 반도체",   {})
    tnx     = data.get("macro2", {}).get("美 10년물 금리",     {})
    fx      = data.get("macro2", {}).get("원/달러 환율",       {})
    wti     = data.get("macro2", {}).get("WTI 유가",           {})
    nq_fut  = data.get("futures",{}).get("나스닥 선물",        {})
    sp_fut  = data.get("futures",{}).get("S&P500 선물",        {})

    nas_chg  = nasdaq.get("chg")
    sox_chg  = sox.get("chg")
    tnx_chg  = tnx.get("chg")
    tnx_last = tnx.get("last")
    fx_chg   = fx.get("chg")
    fx_last  = fx.get("last")
    nq_chg   = nq_fut.get("chg")

    holdings = data.get("holdings", [])
    secs     = data.get("sectors",  [])

    # 포트폴리오 가중평균 등락
    weighted, w_sum = 0.0, 0
    for h in holdings:
        if h.get("ok") and h.get("chg") is not None:
            weighted += h["chg"] * h["buy"]
            w_sum    += h["buy"]
    port_chg  = (weighted / w_sum) if w_sum else None
    total_buy = sum(h["buy"] for h in holdings) or 1

    # 레버리지 비중
    lev_buy   = sum(h["buy"] for h in holdings
                    if any(k in h["name"] for k in ["3배", "2배"]))
    lev_ratio = lev_buy / total_buy * 100

    # 섹터 정렬
    valid_secs = sorted([s for s in secs if s.get("ok") and s.get("chg") is not None],
                        key=lambda x: x["chg"], reverse=True)
    top_sec = valid_secs[0]  if valid_secs else None
    bot_sec = valid_secs[-1] if valid_secs else None
    a_chgs  = [s["chg"] for s in secs if s.get("track") == "A"
               and s.get("ok") and s.get("chg") is not None]
    b_chgs  = [s["chg"] for s in secs if s.get("track") == "B"
               and s.get("ok") and s.get("chg") is not None]
    a_avg   = sum(a_chgs) / len(a_chgs) if a_chgs else None
    b_avg   = sum(b_chgs) / len(b_chgs) if b_chgs else None

    # 보유종목 등락 정렬
    h_valid  = [h for h in holdings if h.get("ok") and h.get("chg") is not None]
    h_sorted = sorted(h_valid, key=lambda x: x["chg"], reverse=True)
    h_up   = [h for h in h_sorted if h["chg"] >= 0]
    h_down = [h for h in h_sorted if h["chg"] <  0]

    # 뉴스 데이터 (보유종목 헤드라인)
    news_data = data.get("news", {})

    # ── 칼럼 본문 구성 ──────────────────────────────────────────────────────
    parts = []

    # ══════════════════════════════════════════════════
    # ① 리드 — 어제 시장, 한 줄 요약 + 3대 지수 종합
    # ══════════════════════════════════════════════════
    sp_chg  = sp500.get("chg")
    dow_d   = data.get("macro", {})
    dow_chg = None
    for _k, _v in dow_d.items():
        if "다우" in _k or "DOW" in _k.upper():
            dow_chg = _v.get("chg") if isinstance(_v, dict) else None
            break

    if nas_chg is not None:
        if nas_chg <= -3.0:
            parts += [
                p(f"나스닥이 {neg(f'{nas_chg:+.2f}%')} 폭락했습니다."),
                p("장 초반부터 마감까지, 전형적인 패닉셀 흐름이었습니다.", col="#555"),
            ]
        elif nas_chg <= -1.5:
            parts += [
                p(f"나스닥이 {neg(f'{nas_chg:+.2f}%')} 하락 마감했습니다."),
                p("기술주 전반에 매도 압력이 거셌습니다.", col="#555"),
            ]
        elif nas_chg <= -0.3:
            parts += [
                p(f"나스닥은 {neg(f'{nas_chg:+.2f}%')}, 소폭 하락 마감했습니다."),
                p("방향성은 아직 결정되지 않은 구간입니다.", col="#555"),
            ]
        elif nas_chg < 0.3:
            parts += [
                p(f"나스닥은 {sc(nas_chg)}, 보합 마감했습니다."),
                p("뚜렷한 모멘텀 없이 관망세가 지배한 하루였습니다.", col="#555"),
            ]
        elif nas_chg < 2.0:
            parts += [
                p(f"나스닥이 {pos(f'{nas_chg:+.2f}%')} 상승 마감했습니다."),
                p("기술주 중심으로 매수세가 이어졌습니다.", col="#555"),
            ]
        else:
            parts += [
                p(f"나스닥이 {pos(f'{nas_chg:+.2f}%')} 강하게 반등했습니다."),
                p("위험자산 선호 심리가 살아난 하루였습니다.", col="#555"),
            ]

    # 3대 지수 종합 코멘트
    index_lines = []
    if sp_chg is not None:
        sp_s = f"{sp_chg:+.2f}%"
        index_lines.append(
            f"{em('S&P 500')} {neg(sp_s) if sp_chg < 0 else pos(sp_s)}")
    if dow_chg is not None:
        dow_s = f"{dow_chg:+.2f}%"
        index_lines.append(
            f"{em('다우존스')} {neg(dow_s) if dow_chg < 0 else pos(dow_s)}")
    if sox_chg is not None:
        sox_s = f"{sox_chg:+.2f}%"
        index_lines.append(
            f"{em('필라델피아 반도체(SOX)')} {neg(sox_s) if sox_chg < 0 else pos(sox_s)}")

    if index_lines:
        parts.append(badge(index_lines))

    all_chgs = [c for c in [nas_chg, sp_chg, dow_chg] if c is not None]
    if all_chgs:
        if all(c < -1.0 for c in all_chgs):
            parts.append(p(
                "세 지수 모두 장 초반부터 마감까지 우하향이었습니다. "
                "단순 섹터 이슈가 아닌, 시장 전반의 위험회피가 작동했습니다.",
                col="#555"))
        elif all(c > 0.5 for c in all_chgs):
            parts.append(p(
                "세 지수가 모두 상승 마감했습니다. "
                "위험자산 전반에 매수세가 고르게 유입됐습니다.", col="#555"))

    parts.append(hr())

    # ══════════════════════════════════════════════════
    # ② 원인 분석 — 금리 / 환율 / 유가 3각 분석
    # ══════════════════════════════════════════════════
    cause_added = False
    tnx_str = f"{tnx_last:.2f}%" if tnx_last else "?"

    if tnx_chg is not None and abs(tnx_chg) >= 0.04:
        cause_added = True
        if tnx_chg > 0:
            parts += [
                p(f"원인의 중심에는 {em('금리')}가 있었습니다."),
                p(f"미국 10년물 국채 금리가 {neg(f'+{tnx_chg:.3f}%p')} 급등하며 "
                  f"{em(tnx_str)}까지 올랐습니다."),
                p("금리 상승은 성장주의 미래 수익을 현재 가치로 환산할 때 "
                  "할인율을 높입니다.", col="#555"),
                p("주가 계산식에서 분모가 커지면 주가가 내려갑니다. "
                  "기술주에 직격탄인 이유입니다.", col="#555"),
            ]
            if tnx_last is not None and tnx_last >= 4.5:
                parts.append(p(
                    f"특히 금리 {tnx_str} 구간은 역사적으로 "
                    "나스닥 멀티플 압축이 본격화되는 임계치입니다.",
                    col="#e53935"))
        else:
            parts += [
                p(f"10년물 국채 금리가 {pos(f'{tnx_chg:.3f}%p')} 하락, "
                  f"{em(tnx_str)}로 안정됐습니다."),
                p("금리 부담이 완화되며 성장주 밸류에이션에 숨통이 트였습니다.",
                  col="#555"),
                p("금리가 내려가면 성장주의 미래 수익 할인율이 낮아져 "
                  "현재 주가를 더 높게 평가할 수 있게 됩니다.", col="#555"),
            ]

    if fx_chg is not None and abs(fx_chg) >= 0.3:
        cause_added = True
        fx_str = f"{fx_last:,.0f}원" if fx_last else "?"
        if fx_chg > 0:
            parts += [
                p(f"원/달러 환율은 {neg(f'+{fx_chg:.1f}원')} 올라 {em(fx_str)}입니다."),
                p("원화 약세 구간입니다.", col="#555"),
                p("달러 자산 보유자 입장에선 환차익이 발생하지만, "
                  "급격한 약세는 외국인 자금 이탈의 신호이기도 합니다.", col="#555"),
            ]
        else:
            parts += [
                p(f"원/달러 환율은 {pos(f'{fx_chg:.1f}원')} 내린 {em(fx_str)}입니다."),
                p("원화 강세 — 달러 자산의 원화 환산 수익이 일부 희석됩니다.",
                  col="#555"),
            ]

    wti_d   = data.get("macro2", {})
    wti_chg = None
    wti_last_v = None
    for _k, _v in wti_d.items():
        if "WTI" in _k.upper() or "유가" in _k:
            if isinstance(_v, dict):
                wti_chg    = _v.get("chg")
                wti_last_v = _v.get("last")
            break
    if wti_chg is not None and abs(wti_chg) >= 1.5:
        cause_added = True
        wti_s = f"{wti_last_v:.2f}" if wti_last_v else "?"
        if wti_chg < 0:
            parts.append(p(
                f"WTI 유가는 {neg(f'{wti_chg:+.1f}%')} 내려 ${wti_s}입니다. "
                "원자재 수요 둔화 우려가 반영되고 있습니다.", col="#555"))
        else:
            parts.append(p(
                f"WTI 유가는 {pos(f'{wti_chg:+.1f}%')} 올라 ${wti_s}입니다. "
                "에너지 인플레이션 압력이 다시 고개를 들고 있습니다.", col="#555"))

    if cause_added:
        parts.append(hr())

    # ══════════════════════════════════════════════════
    # ③ 반도체 / SOX 별도 진단
    # ══════════════════════════════════════════════════
    sox_names = ["SOXL", "SOX", "반도체", "SK하이닉스", "삼성전자", "NVDA", "엔비디아"]
    has_semi = any(any(k in h.get("name", "") for k in sox_names) for h in holdings)

    if sox_chg is not None and has_semi:
        sox_s = f"{sox_chg:+.2f}%"
        if sox_chg <= -3.0:
            parts += [
                p(f"{em('반도체 섹터')}를 따로 짚어봐야 합니다."),
                p(f"필라델피아 반도체 지수(SOX)가 {neg(sox_s)} 폭락했습니다."),
                p("반도체발 투매가 기술주 전체로 번지는 패턴입니다.", col="#555"),
                p("SOXL은 SOX의 3배 추종이므로, 이날 손실이 배로 확대됩니다.", col="#555"),
            ]
        elif sox_chg <= -1.5:
            parts += [
                p(f"필라델피아 반도체 지수가 {neg(sox_s)} 하락했습니다."),
                p("SK하이닉스·삼성전자는 SOX와 강하게 동조합니다. "
                  "오늘 국내장에도 하방 압력이 예상됩니다.", col="#555"),
            ]
        elif sox_chg >= 2.0:
            parts += [
                p(f"필라델피아 반도체 지수가 {pos(sox_s)} 강하게 올랐습니다."),
                p("반도체 모멘텀이 살아있습니다. "
                  "SOXL·SK하이닉스 보유 비중에 긍정적인 환경입니다.", col="#555"),
            ]
        parts.append(hr())

    # ══════════════════════════════════════════════════
    # ④ 내 포트폴리오 명암 — 종목별 등락
    # ══════════════════════════════════════════════════
    if h_sorted:
        parts.append(p(f"{em('내 포트폴리오')} 어떻게 됐을까요."))

        if port_chg is not None:
            port_s = f"{port_chg:+.2f}%"
            if port_chg < -2.0:
                parts += [
                    p(f"가중평균 기준 {neg(port_s)} 하락했습니다."),
                    p("레버리지 종목이 낙폭을 크게 키웠습니다.", col="#555"),
                ]
            elif port_chg < 0:
                parts.append(p(f"가중평균 기준 {neg(port_s)} 소폭 하락했습니다."))
            elif port_chg >= 2.0:
                parts += [
                    p(f"가중평균 기준 {pos(port_s)} 강하게 올랐습니다."),
                    p("레버리지 종목이 수익을 크게 끌어올린 하루였습니다.", col="#555"),
                ]
            else:
                parts.append(p(f"가중평균 기준 {pos(port_s)} 상승 마감했습니다."))

        if h_down:
            parts.append(p("가장 많이 빠진 종목들입니다.", col="#555"))
            down_items = []
            for h in h_down[:6]:
                h_nm  = h["name"]
                h_ch  = h["chg"]
                lev_tag  = " (3배 레버리지)" if "3배" in h_nm else ""
                h_news   = news_data.get(h_nm, [])
                news_hint = ""
                if h_news:
                    hn = h_news[0][:28] + "..." if len(h_news[0]) > 28 else h_news[0]
                    news_hint = f" — {hn}"
                down_items.append(
                    f"{neg(h_nm)}{lev_tag}({h_ch:+.2f}%){news_hint}")
            parts.append(badge(down_items))

        if h_up:
            parts.append(p("그나마 버텨준 종목들입니다.", col="#555"))
            up_items = []
            for h in h_up[:4]:
                h_nm = h["name"]
                h_ch = h["chg"]
                up_items.append(f"{pos(h_nm)}({h_ch:+.2f}%)")
            parts.append(badge(up_items))

        if lev_ratio >= 20:
            lev_s  = f"{lev_ratio:.0f}%"
            loss_s = f"{lev_ratio * 0.3:.0f}%"
            parts.append(p(
                f"현재 레버리지 ETF 비중은 {em(lev_s)}입니다. "
                f"지수가 -3% 빠지면 레버리지 포지션만으로 최대 {neg(f'-{loss_s}')} 손실이 "
                "발생할 수 있습니다.",
                col="#c62828"))

        parts.append(hr())

    # ══════════════════════════════════════════════════
    # ⑤ 섹터 로테이션 — 돈은 어디로 이동했나
    # ══════════════════════════════════════════════════
    if valid_secs:
        parts.append(p(f"{em('자금은 어디로 움직였을까요.')}"))

        top3 = valid_secs[:3]
        bot3 = valid_secs[-3:]

        top_items = [f"{pos(s['name'])} ({s['chg']:+.2f}%)" for s in top3]
        bot_items = [f"{neg(s['name'])} ({s['chg']:+.2f}%)" for s in bot3]

        parts.append(p("강세 섹터", col="#43a047", mb=6))
        parts.append(badge(top_items))
        parts.append(p("약세 섹터", col="#e53935", mb=6))
        parts.append(badge(bot_items))

        if a_avg is not None and b_avg is not None:
            diff = b_avg - a_avg
            a_s  = f"{a_avg:+.2f}%"
            b_s  = f"{b_avg:+.2f}%"
            if diff > 0.5:
                parts += [
                    p(f"방어 섹터 평균 {pos(b_s)} vs 성장 섹터 평균 {neg(a_s)}."),
                    p("자금이 기술·성장 섹터에서 방어주와 가치주로 이동하고 있습니다.", col="#555"),
                    p("이런 로테이션이 지속되면 레버리지 성장주엔 역풍입니다.", col="#e53935"),
                ]
            elif diff < -0.5:
                parts += [
                    p(f"성장 섹터 평균 {pos(a_s)} vs 방어 섹터 평균 {neg(b_s)}."),
                    p("자금이 여전히 기술·AI 섹터에 집중되고 있습니다. "
                      "현 포트폴리오 방향과 시장 흐름이 일치하는 구간입니다.", col="#555"),
                ]
            else:
                parts.append(p(
                    f"성장 섹터 {sc(a_avg)} / 방어 섹터 {sc(b_avg)}, "
                    "뚜렷한 로테이션 방향 없이 혼재된 장세입니다.", col="#555"))

        parts.append(hr())

    # ══════════════════════════════════════════════════
    # ⑥ 선물 방향 — 오늘 장 예고
    # ══════════════════════════════════════════════════
    nq_fut_chg = nq_fut.get("chg")
    sp_fut_chg = sp_fut.get("chg") if isinstance(sp_fut, dict) else None

    if nq_fut_chg is not None:
        parts.append(p(f"{em('오늘 장 분위기를 예고하는 선물 방향입니다.')}"))
        fut_items = []
        nq_s = f"{nq_fut_chg:+.2f}%"
        fut_items.append(
            f"{em('나스닥 선물')} {neg(nq_s) if nq_fut_chg < 0 else pos(nq_s)}")
        if sp_fut_chg is not None:
            sp_fs = f"{sp_fut_chg:+.2f}%"
            fut_items.append(
                f"{em('S&P500 선물')} {neg(sp_fs) if sp_fut_chg < 0 else pos(sp_fs)}")
        parts.append(badge(fut_items))

        if nq_fut_chg < -0.5 and nas_chg is not None and nas_chg < -1.0:
            parts.append(p(
                "전일 급락에 이어 선물까지 추가 하락 중입니다. "
                "오늘 장 초반 변동성이 클 수 있습니다. "
                "레버리지 종목은 특히 주의하세요.", col="#e53935"))
        elif nq_fut_chg > 0.5 and nas_chg is not None and nas_chg < -1.0:
            parts.append(p(
                "전일 하락 후 선물이 반등하고 있습니다. "
                "기술적 반등 시도 가능성이 있으나, "
                "장 초반 반등이 지속되는지 확인 후 대응하는 게 안전합니다.", col="#555"))
        elif nq_fut_chg > 0.3:
            parts.append(p(
                "선물 상승 중입니다. 오늘 장 초반 긍정적 흐름이 기대됩니다.", col="#555"))
        else:
            parts.append(p(
                "선물이 약보합권입니다. 오늘 장 초반 큰 방향성 없이 출발 예상입니다.",
                col="#555"))

        parts.append(hr())

    # ══════════════════════════════════════════════════
    # ⑦ 오늘의 액션 — 구체적 행동 지침
    # ══════════════════════════════════════════════════
    parts.append(p(f"그렇다면 {em('지금 어떻게 해야 할까요.')}"))

    actions = []
    nq_fut_chg_v = nq_fut.get("chg")

    if nas_chg is not None and nq_fut_chg_v is not None:
        if nas_chg < -1.5 and nq_fut_chg_v < 0:
            lev_s = f"{lev_ratio:.0f}%"
            actions.append(
                f"전일 급락에 선물까지 추가 하락 중입니다. "
                f"레버리지 비중 {lev_s} — 오늘은 추가 매수 금지. "
                "관망하며 반등 신호를 기다리세요.")

    if nas_chg is not None and nq_fut_chg_v is not None:
        if nas_chg < -2.0 and nq_fut_chg_v > 0.3:
            actions.append(
                "전일 급락 후 선물이 반등 중입니다. "
                "분할 매수라면 계획량의 30% 이하로만 진입하고, "
                "반등 지속 여부를 장 중반까지 확인하세요.")

    if tnx_last is not None and tnx_last >= 4.5:
        tnx_s = f"{tnx_last:.2f}%"
        if tnx_chg is not None and tnx_chg > 0.04:
            actions.append(
                f"금리 {tnx_s}에 추가 상승까지 — 이 수준에서 레버리지 성장주 "
                "신규 매수는 리스크 대비 기대수익이 낮습니다. "
                "현금 비중을 일부 확보해두세요.")
        else:
            actions.append(
                f"금리 {tnx_s} 고공 유지 중입니다. "
                "성장주 추가 매수 시 손절 기준을 반드시 설정하고 진입하세요.")

    if sox_chg is not None and sox_chg < -3.0:
        sox_s = f"{sox_chg:+.2f}%"
        actions.append(
            f"SOX가 {sox_s} 폭락했습니다. "
            "SOXL은 SOX의 3배 추종 — 오늘도 추가 하락 가능성이 있습니다. "
            "손절선 아래로 내려온다면 기계적으로 비중 축소를 실행하세요.")

    if lev_ratio >= 20 and port_chg is not None and port_chg >= 2.0:
        actions.append(
            "레버리지 종목에서 수익이 쌓인 상태입니다. "
            "TQQQ·SOXL 중 수익률이 높은 종목 10~20%를 "
            "오늘 장 초반 반등 시 부분 차익실현하는 방안을 검토하세요.")

    if a_avg is not None and b_avg is not None and (b_avg - a_avg) > 0.8:
        actions.append(
            "방어주 로테이션이 뚜렷합니다. "
            "기술주 비중을 줄인 현금으로 VDC(생필품)·XLV(헬스케어) 등 "
            "방어 ETF를 소액 분할 편입하는 방법도 있습니다.")

    if fx_chg is not None and fx_chg > 0.5:
        fx_s = f"{fx_last:,.0f}원" if fx_last else "?"
        actions.append(
            f"원화 약세 ({fx_s}) 구간입니다. "
            "달러 자산(미국 ETF·주식) 비중을 섣불리 줄이지 말고, "
            "환율 안정 이후 조정 여부를 판단하세요.")

    if not actions:
        actions.append(
            "현재 시장 지표상 긴급한 포지션 변경 사유는 없습니다. "
            "기존 포지션을 유지하되 선물 방향과 섹터 흐름을 모니터링하세요.")

    parts.append(badge(actions))
    parts.append(hr())

    # ══════════════════════════════════════════════════
    # ⑧ 이번 주 / 다음 주 체크포인트
    # ══════════════════════════════════════════════════
    parts.append(p(f"{em('앞으로 챙겨볼 것들입니다.')}"))
    checkpoint_items = []

    if tnx_last is not None and tnx_last >= 4.3:
        checkpoint_items.append(
            "CPI·PPI 발표 — 금리 방향의 핵심 변수. "
            "예상보다 낮게 나오면 기술주 반등 신호.")
    if sox_chg is not None and sox_chg < -2.0:
        checkpoint_items.append(
            "반도체 주요 기업 가이던스·실적 — SOX 급락의 추세 전환 여부 확인.")
    if nas_chg is not None and nas_chg < -2.0:
        checkpoint_items.append(
            "FOMC 의사록·연준 위원 발언 — 금리 인상 또는 동결 시그널 변화 체크.")
    checkpoint_items.append(
        "나스닥 선물 방향 — 매일 장 시작 전 확인. "
        "선물 -1% 이하면 레버리지 추가 매수 보류.")
    if lev_ratio >= 15:
        lev_s = f"{lev_ratio:.0f}%"
        checkpoint_items.append(
            f"레버리지 비중 {lev_s} 점검 — 목표 비중 범위(15~20%) 이탈 시 리밸런싱.")

    parts.append(badge(checkpoint_items))
    parts.append(hr())

    # ══════════════════════════════════════════════════
    # ⑨ 리스크 고지
    # ══════════════════════════════════════════════════
    if lev_ratio >= 15:
        lev_s  = f"{lev_ratio:.0f}%"
        loss_s = f"{lev_ratio * 0.3:.0f}%"
        parts.append(
            p(f"※ 레버리지 {lev_s} 보유 중 — 시장 -3% 시 최대 {loss_s} 손실 가능. "
              "본 칼럼은 수집 데이터 기반 자동 생성이며 투자 권유가 아닙니다.",
              size=12, col="#aaa", mb=0))
    else:
        parts.append(
            p("※ 본 칼럼은 수집 데이터 기반 자동 생성이며 투자 권유가 아닙니다.",
              size=12, col="#aaa", mb=0))

    body = "\n".join(parts)
    return f"""
    <table width='100%' cellpadding='0' cellspacing='0'
           style='background:#ffffff;margin-bottom:2px;'>
      <tr><td style='padding:24px 28px 8px;border-top:3px solid #111;'>
        <p style='margin:0 0 4px;font-size:10px;color:#999;
                  letter-spacing:.12em;text-transform:uppercase;'>오늘의 투자 칼럼</p>
        <p style='margin:0 0 22px;font-size:20px;font-weight:600;color:#111;
                  line-height:1.4;'>지금 내 포트폴리오,<br>어떻게 해야 하나</p>
        {body}
      </td></tr>
    </table>"""


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
        top_nm = top_s['name']; top_chg_s = fmt_chg(top_s['chg'])
        bot_nm = bot_s['name']; bot_chg_s = fmt_chg(bot_s['chg'])
        rotation = (f"<b>{top_nm}</b>({top_chg_s}) 최강 &nbsp;/&nbsp; "
                    f"<b>{bot_nm}</b>({bot_chg_s}) 최약. ")
        b_top = [s for s in valid[:3] if s["track"] == "B"]
        if b_top:
            names = ", ".join(s["name"] for s in b_top)
            rotation += f"방어/가치 섹터({names})로 자금 유입 감지 → 레버리지 비중 점검 권고."
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

    mentor_block = build_column(data)

    footer = """
    <table width="100%" cellpadding="0" cellspacing="0"
           style="background:#ffffff;border-radius:0 0 8px 8px;">
      <tr>
        <td width="33%" style="padding:16px 18px;vertical-align:top;border-top:1px solid #eeeeee;">
          <p style="margin:0 0 8px;font-size:11px;font-weight:500;color:#222;">포트폴리오</p>
          <p style="margin:0;font-size:12px;color:#888;line-height:1.9;">ISA (6386)<br>위탁 (6362)<br>위탁 (6466)</p>
        </td>
        <td width="33%" style="padding:16px 18px;vertical-align:top;border-top:1px solid #eeeeee;">
          <p style="margin:0 0 8px;font-size:11px;font-weight:500;color:#222;">데이터 소스</p>
          <p style="margin:0;font-size:12px;color:#888;line-height:1.9;">yfinance<br>Google News RSS<br>실시간 선물</p>
        </td>
        <td width="33%" style="padding:16px 18px;vertical-align:top;border-top:1px solid #eeeeee;">
          <p style="margin:0 0 8px;font-size:11px;font-weight:500;color:#222;">발송</p>
          <p style="margin:0;font-size:12px;color:#888;line-height:1.9;">매일 오전 06:30<br>Gmail · 텔레그램<br>투자 권유 아님</p>
          <p style="margin:8px 0 0;font-size:11px;">
            <a href="https://github.com/seungweon77-sketch/CLAUDE-H"
               style="color:#555;text-decoration:none;">
              ⌥ GitHub 코드 보기 ↗</a></p>
        </td>
      </tr>
    </table>"""

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
    """메신저용 모바일 친화 평문 브리핑."""
    import datetime as _dt
    today = _dt.datetime.now().strftime("%Y-%m-%d (%a)")
    def chg_str(v, ok):
        if not ok or v is None: return "  N/A"
        return f"{'🔺' if v >= 0 else '🔻'}{v:+.2f}%"
    L = [f"☕ Market Mentor  {today}"]
    weighted, w_sum = 0.0, 0
    for h in data["holdings"]:
        if h["ok"] and h["chg"] is not None:
            weighted += h["chg"] * h["buy"]; w_sum += h["buy"]
    port_chg = (weighted / w_sum) if w_sum else None
    nas = data.get("macro", {}).get("나스닥", {})
    fx  = data.get("macro2", {}).get("원/달러 환율", {})
    p  = f"{'🔺' if (port_chg or 0) >= 0 else '🔻'}{port_chg:+.2f}%" if port_chg is not None else "N/A"
    n  = f"{nas['chg']:+.2f}%" if nas.get("ok") else "N/A"
    f_ = f"{fx['last']:,.0f}원" if fx.get("ok") and fx.get("last") else "N/A"
    L.append(f"내 자산 {p} | 나스닥 {n} | 환율 {f_}")
    L.append("━━━━━━━━━━━━━━━━")
    L.append("📊 내 보유종목")
    holds = sorted(data["holdings"], key=lambda x: (x["chg"] if x["ok"] else -999), reverse=True)
    for h in holds:
        name = (h["name"][:11] + "…") if len(h["name"]) > 12 else h["name"].ljust(12)
        L.append(f"{name} {chg_str(h['chg'], h['ok'])}")
    news_lines = []
    for nm, items in data.get("news", {}).items():
        if items:
            news_lines.append(f"• {nm}")
            for t in items: news_lines.append(f"  - {t[:60]}")
    if news_lines:
        L.extend(["", "📰 핵심 뉴스"] + news_lines)
    def macro_block(title, dct, names):
        out = [title]
        for n in names:
            d = dct.get(n, {"chg": None, "ok": False})
            out.append(f"{n.ljust(10)} {chg_str(d.get('chg'), d.get('ok', False))}")
        return out
    L.extend([""] + macro_block("🇺🇸 전일 미국증시", data.get("macro", {}), [m[0] for m in MACRO]))
    L.extend([""] + macro_block("🌐 매크로", data.get("macro2", {}), [m[0] for m in MACRO_2]))
    secs = sorted(data.get("sectors", []), key=lambda x: (x["chg"] if x["ok"] else -999), reverse=True)
    if secs:
        L.extend(["", "🏭 섹터 (강세순)"])
        for s in secs:
            tag = "◆" if s["track"] == "A" else "◇"
            L.append(f"{tag}{s['name'][:9].ljust(9)} {chg_str(s['chg'], s['ok'])}")
    L.extend(["━━━━━━━━━━━━━━━━", "※ 참고자료이며 투자권유 아님"])
    return "\n".join(L)


def send_email(html: str) -> bool:
    import smtplib, datetime as dt
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    if not (CFG["gmail_user"] and CFG["gmail_pw"] and CFG["mail_to"]):
        log.info("이메일 설정 미비 → 발송 생략"); return False
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"☕ [{dt.datetime.now().strftime('%m/%d')}] Market Mentor 모닝 브리핑"
        msg["From"] = CFG["gmail_user"]; msg["To"] = CFG["mail_to"]
        msg.attach(MIMEText(html, "html", "utf-8"))
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=30) as s:
            s.login(CFG["gmail_user"], CFG["gmail_pw"]); s.send_message(msg)
        log.info("이메일 발송 성공"); return True
    except Exception as e:
        log.error(f"이메일 발송 실패: {e}"); return False


def send_telegram(text: str) -> bool:
    if not (CFG["tg_token"] and CFG["tg_chat"]):
        log.info("텔레그램 설정 미비 → 발송 생략"); return False
    try:
        import requests
        requests.post(f"https://api.telegram.org/bot{CFG['tg_token']}/sendMessage",
                      data={"chat_id": CFG["tg_chat"], "text": text[:4000]}, timeout=20).raise_for_status()
        log.info("텔레그램 발송 성공"); return True
    except Exception as e:
        log.error(f"텔레그램 발송 실패: {e}"); return False


def send_discord(text: str) -> bool:
    if not CFG["discord_webhook"]:
        log.info("Discord 설정 미비 → 발송 생략"); return False
    try:
        import requests
        requests.post(CFG["discord_webhook"],
                      json={"content": "```\n" + text[:1900] + "\n```"}, timeout=20).raise_for_status()
        log.info("Discord 발송 성공"); return True
    except Exception as e:
        log.error(f"Discord 발송 실패: {e}"); return False


def send_slack(text: str) -> bool:
    if not CFG["slack_webhook"]:
        log.info("Slack 설정 미비 → 발송 생략"); return False
    try:
        import requests
        requests.post(CFG["slack_webhook"],
                      json={"text": "```\n" + text[:3500] + "\n```"}, timeout=20).raise_for_status()
        log.info("Slack 발송 성공"); return True
    except Exception as e:
        log.error(f"Slack 발송 실패: {e}"); return False


def main():
    log.info("=" * 50); log.info("Market Mentor 실행 시작")
    try:
        data = collect_all()
        html = build_briefing(data)
        text = build_text(data)
        with open("latest_briefing.html", "w", encoding="utf-8") as f: f.write(html)
        with open("latest_briefing.txt",  "w", encoding="utf-8") as f: f.write(text)
        sent = any([send_email(html), send_telegram(text), send_discord(text), send_slack(text)])
        print("브리핑 발송 완료." if sent else "발송 채널 없음 -> latest_briefing.html 만 생성됨.")
        log.info("Market Mentor 실행 종료(정상)")
    except Exception as e:
        log.critical(f"치명적 오류: {e}", exc_info=True)
        import sys; sys.exit(1)
