#!/usr/bin/env python3
"""Fetch latest market data via Yahoo Finance Chart API (direct HTTP).

Replaces the yfinance dependency with explicit HTTPS calls to
`query2.finance.yahoo.com/v8/finance/chart/{symbol}`. This is more
resilient to upstream package breakage and makes the data path obvious.
"""
import json
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta

KST = timezone(timedelta(hours=9))
DRAM_TICKER    = 'DRAM'
SAMSUNG_TICKER = '005930.KS'
HYNIX_TICKER   = '000660.KS'
HISTORY_DAYS   = 12

CHART_URL = ('https://query2.finance.yahoo.com/v8/finance/chart/{symbol}'
             '?period1={p1}&period2={p2}&interval=1d')
UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '\
     'AppleWebKit/537.36 (KHTML, like Gecko) '\
     'Chrome/124.0 Safari/537.36'


def http_get_json(url: str, retries: int = 4) -> dict:
    """GET JSON with exponential backoff (2s, 4s, 8s, 16s)."""
    last_err = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={'User-Agent': UA})
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read().decode('utf-8'))
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as e:
            last_err = e
            wait = 2 ** (attempt + 1)
            print(f'  retry {attempt + 1}/{retries} after {wait}s: {e}',
                  file=sys.stderr)
            time.sleep(wait)
    raise RuntimeError(f'HTTP failed after {retries} retries: {last_err}')


def fetch_closes(ticker: str, lookback_days: int = 60) -> dict[str, float]:
    """Return {YYYY-MM-DD: close} from Yahoo Chart API."""
    now = int(time.time())
    p1  = now - lookback_days * 86400
    url = CHART_URL.format(symbol=ticker, p1=p1, p2=now)
    payload = http_get_json(url)

    err = payload.get('chart', {}).get('error')
    if err:
        raise ValueError(f'{ticker}: {err}')
    result = payload['chart']['result']
    if not result:
        raise ValueError(f'{ticker}: empty result')

    r0 = result[0]
    timestamps = r0.get('timestamp') or []
    closes = (r0.get('indicators', {}).get('quote') or [{}])[0].get('close') or []
    tz_name = r0.get('meta', {}).get('exchangeTimezoneName', 'UTC')
    tz_offset = r0.get('meta', {}).get('gmtoffset', 0)
    exch_tz = timezone(timedelta(seconds=tz_offset))

    out = {}
    for ts, close in zip(timestamps, closes):
        if close is None:
            continue
        d = datetime.fromtimestamp(ts, exch_tz).date()
        out[str(d)] = float(close)
    if not out:
        raise ValueError(f'{ticker}: no valid closes ({tz_name})')
    return out


def align_and_trim(
    kr_dicts: tuple[dict, dict],
    dram_closes: dict,
    n: int,
) -> tuple[list[str], list[str], list[list]]:
    """Align Korean stocks (KST) with DRAM ETF (US ET).

    Returns (labels, kr_dates, [dram, samsung, hynix]) where kr_dates are
    YYYY-MM-DD strings used for AUM anchor interpolation.
    """
    samsung_closes, hynix_closes = kr_dicts
    kr_common = sorted(set(samsung_closes) & set(hynix_closes))
    dram_sorted = sorted(dram_closes.keys())

    if len(kr_common) < n:
        print(f'Warning: only {len(kr_common)} common KR trading days '
              f'(needed {n}). Using all available.', file=sys.stderr)

    selected_kr = kr_common[-n:]

    labels, kr_dates, sam_vals, hyn_vals, dram_vals = [], [], [], [], []
    for kr_date in selected_kr:
        matching = [d for d in dram_sorted if d <= kr_date]
        if not matching:
            print(f'  skip {kr_date}: no DRAM date ≤ {kr_date}', file=sys.stderr)
            continue
        dram_date = matching[-1]
        labels.append(f"{int(kr_date[5:7])}/{int(kr_date[8:10])}")
        kr_dates.append(kr_date)
        sam_vals.append(samsung_closes[kr_date])
        hyn_vals.append(hynix_closes[kr_date])
        dram_vals.append(dram_closes[dram_date])
        if dram_date != kr_date:
            print(f'  {kr_date} (KR) ↔ {dram_date} (DRAM, 1-day lag)',
                  file=sys.stderr)

    return labels, kr_dates, [dram_vals, sam_vals, hyn_vals]


SPARK_URL = ('https://query2.finance.yahoo.com/v8/finance/spark'
             '?symbols={symbol}&range=1d&interval=1d')
STOCKANALYSIS_URL = 'https://stockanalysis.com/etf/{symbol}/'

# Verified AUM anchors ($M) from Roundhill press releases & confirmed reports.
# Used for linear interpolation between known data points.
AUM_ANCHORS: dict[str, float] = {
    '2026-04-09': 245.0,    # ~$245M (confirmed, 1 week post-launch)
    '2026-04-10': 428.0,    # ~$428M (confirmed)
    '2026-04-17': 732.0,    # $732M  (Roundhill announcement)
    '2026-04-21': 1099.6,   # $1,099.6M (Roundhill "$1B milestone" press release)
}


def fetch_shares_outstanding(ticker: str) -> float | None:
    """Scrape shares outstanding from stockanalysis.com (no auth required).

    The page embeds JSON-like data: sharesOut:"46.90M". Returns share count
    as a float, or None if extraction fails.
    """
    try:
        url = STOCKANALYSIS_URL.format(symbol=ticker.lower())
        req = urllib.request.Request(url, headers={'User-Agent': UA})
        with urllib.request.urlopen(req, timeout=15) as r:
            html = r.read().decode('utf-8', errors='ignore')
        import re
        m = re.search(r'sharesOut:"([\d.]+)\s*([MBK]?)"', html)
        if not m:
            return None
        val, unit = float(m.group(1)), m.group(2)
        mult = {'K': 1e3, 'M': 1e6, 'B': 1e9, '': 1}[unit]
        shares = val * mult
        print(f'  Shares outstanding: {shares/1e6:.2f}M', file=sys.stderr)
        return shares
    except Exception as e:
        print(f'  shares fetch failed: {e}', file=sys.stderr)
        return None


def fetch_total_assets(ticker: str) -> float | None:
    """Get ETF total net assets (USD).

    Strategy (in order):
    1. shares outstanding × current price (stockanalysis.com)
    2. Yahoo chart meta.totalAssets
    3. Yahoo spark endpoint totalAssets
    Returns None if all attempts fail.
    """
    # Attempt 1: shares outstanding × current price (most reliable)
    shares = fetch_shares_outstanding(ticker)
    if shares:
        try:
            now = int(time.time())
            url = CHART_URL.format(symbol=ticker, p1=now - 86400 * 2, p2=now)
            payload = http_get_json(url, retries=2)
            price = (payload.get('chart', {}).get('result') or [{}])[0] \
                      .get('meta', {}).get('regularMarketPrice')
            if price:
                aum = shares * float(price)
                print(f'  AUM from shares × price: ${aum/1e6:.1f}M', file=sys.stderr)
                return aum
        except Exception as e:
            print(f'  shares × price calc failed: {e}', file=sys.stderr)

    # Attempt 2: chart meta
    try:
        now = int(time.time())
        url = CHART_URL.format(symbol=ticker, p1=now - 86400 * 5, p2=now)
        payload = http_get_json(url, retries=2)
        ta = (payload.get('chart', {}).get('result') or [{}])[0] \
               .get('meta', {}).get('totalAssets')
        if ta and ta > 0:
            print(f'  AUM from chart meta: ${ta/1e6:.1f}M', file=sys.stderr)
            return float(ta)
    except Exception as e:
        print(f'  AUM chart meta failed: {e}', file=sys.stderr)

    # Attempt 3: spark endpoint
    try:
        url = SPARK_URL.format(symbol=ticker)
        payload = http_get_json(url, retries=2)
        ta = (payload.get('spark', {}).get('result') or [{}])[0] \
               .get('response', [{}])[0].get('meta', {}).get('totalAssets')
        if ta and ta > 0:
            print(f'  AUM from spark: ${ta/1e6:.1f}M', file=sys.stderr)
            return float(ta)
    except Exception as e:
        print(f'  AUM spark failed: {e}', file=sys.stderr)

    return None


def estimate_aum(kr_dates: list[str], dram_prices: list[float]) -> list[float]:
    """AUM ($M) per date using verified anchors + linear date interpolation.

    Most recent date uses shares × market price (most accurate).
    Dates between known anchors are linearly interpolated by calendar day.
    Dates before the earliest anchor fall back to the earliest anchor value.
    """
    from datetime import date as date_cls

    # Build anchor map: static anchors + today's live value
    anchors: dict[str, float] = dict(AUM_ANCHORS)
    total_assets = fetch_total_assets(DRAM_TICKER)
    if total_assets:
        current_aum_m = total_assets / 1_000_000
        anchors[kr_dates[-1]] = current_aum_m
        print(f'  AUM today ({kr_dates[-1]}): ${current_aum_m:.1f}M', file=sys.stderr)
    else:
        print('  AUM live fetch failed – using static anchors only', file=sys.stderr)

    sorted_anchors = sorted(anchors.items())
    anchor_ords = [date_cls.fromisoformat(d).toordinal() for d, _ in sorted_anchors]
    anchor_vals = [v for _, v in sorted_anchors]

    result = []
    for date_str in kr_dates:
        if date_str in anchors:
            result.append(round(anchors[date_str], 1))
            continue
        d_ord = date_cls.fromisoformat(date_str).toordinal()
        # Find left/right bracketing anchors
        li = next((i for i in range(len(anchor_ords) - 1, -1, -1)
                   if anchor_ords[i] <= d_ord), None)
        ri = next((i for i in range(len(anchor_ords))
                   if anchor_ords[i] >= d_ord), None)
        if li is None:
            aum = anchor_vals[0]
        elif ri is None or ri == li:
            aum = anchor_vals[-1]
        else:
            t = (d_ord - anchor_ords[li]) / (anchor_ords[ri] - anchor_ords[li])
            aum = anchor_vals[li] + t * (anchor_vals[ri] - anchor_vals[li])
        result.append(round(aum, 1))

    return result


def main() -> None:
    print('Fetching market data from Yahoo Chart API…')
    try:
        dram_closes    = fetch_closes(DRAM_TICKER)
        print(f'  ✓ DRAM: {len(dram_closes)} dates', file=sys.stderr)
    except Exception as e:
        print(f'  ✗ DRAM fetch failed: {e}', file=sys.stderr)
        raise

    try:
        samsung_closes = fetch_closes(SAMSUNG_TICKER)
        print(f'  ✓ Samsung: {len(samsung_closes)} dates', file=sys.stderr)
    except Exception as e:
        print(f'  ✗ Samsung fetch failed: {e}', file=sys.stderr)
        raise

    try:
        hynix_closes   = fetch_closes(HYNIX_TICKER)
        print(f'  ✓ SK Hynix: {len(hynix_closes)} dates', file=sys.stderr)
    except Exception as e:
        print(f'  ✗ SK Hynix fetch failed: {e}', file=sys.stderr)
        raise

    labels, kr_dates, (dram, samsung, hynix) = align_and_trim(
        (samsung_closes, hynix_closes), dram_closes, n=HISTORY_DAYS
    )
    aum = estimate_aum(kr_dates, dram)

    data = {
        'updated':      datetime.now(KST).strftime('%Y-%m-%d %H:%M'),
        'labels':       labels,
        'aum':          aum,
        'dramPrice':    [round(v, 2) for v in dram],
        'samsungPrice': [int(v) for v in samsung],
        'hynixPrice':   [int(v) for v in hynix],
    }

    try:
        with open('data.json', 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f'✓ data.json updated: {data["updated"]} ({len(labels)} trading days)')
    except IOError as e:
        print(f'✗ Failed to write data.json: {e}', file=sys.stderr)
        raise


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        print(f'Error: {exc}', file=sys.stderr)
        sys.exit(1)
