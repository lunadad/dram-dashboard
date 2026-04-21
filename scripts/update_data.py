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
) -> tuple[list[str], list[list]]:
    """Align Korean stocks (KST) with DRAM ETF (US ET).

    Korean market closes ~6:30 AM ET; US market opens at 9:30 AM ET.
    A Korean "4/21 KST" close therefore corresponds to the DRAM "4/20 ET"
    close (most recent available US session before the next US open).

    Strategy: take the last N common Korean dates, then for each Korean date
    find the most recent DRAM date that is ≤ Korean date (i.e. allow DRAM
    to lag Korea by up to 1 calendar day).
    """
    samsung_closes, hynix_closes = kr_dicts
    kr_common = sorted(set(samsung_closes) & set(hynix_closes))
    dram_sorted = sorted(dram_closes.keys())

    if len(kr_common) < n:
        print(f'Warning: only {len(kr_common)} common KR trading days '
              f'(needed {n}). Using all available.', file=sys.stderr)

    selected_kr = kr_common[-n:]

    labels, sam_vals, hyn_vals, dram_vals = [], [], [], []
    for kr_date in selected_kr:
        # find latest DRAM date ≤ kr_date (allow 1-day lag)
        matching = [d for d in dram_sorted if d <= kr_date]
        if not matching:
            print(f'  skip {kr_date}: no DRAM date ≤ {kr_date}', file=sys.stderr)
            continue
        dram_date = matching[-1]
        labels.append(f"{int(kr_date[5:7])}/{int(kr_date[8:10])}")
        sam_vals.append(samsung_closes[kr_date])
        hyn_vals.append(hynix_closes[kr_date])
        dram_vals.append(dram_closes[dram_date])
        if dram_date != kr_date:
            print(f'  {kr_date} (KR) ↔ {dram_date} (DRAM, 1-day lag)',
                  file=sys.stderr)

    return labels, [dram_vals, sam_vals, hyn_vals]


SPARK_URL = ('https://query2.finance.yahoo.com/v8/finance/spark'
             '?symbols={symbol}&range=1d&interval=1d')


def fetch_total_assets(ticker: str) -> float | None:
    """Try multiple Yahoo endpoints to get ETF total net assets (USD).

    1. chart meta.totalAssets (no auth needed)
    2. spark endpoint totalAssets
    Returns None if all attempts fail.
    """
    # Attempt 1: chart meta
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

    # Attempt 2: spark endpoint
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


def estimate_aum(dram_prices: list[float]) -> list[float]:
    """Approximate historical AUM ($M) scaled proportionally to DRAM closes.

    Current AUM anchor: Yahoo chart meta totalAssets when available,
    otherwise falls back to last known reliable value ($732M on 2026-04-17).
    """
    total_assets = fetch_total_assets(DRAM_TICKER)
    if total_assets:
        current_aum_m = total_assets / 1_000_000
    else:
        current_aum_m = 732.0
        print(f'  AUM fallback anchor: ${current_aum_m:.0f}M', file=sys.stderr)
    ratio = current_aum_m / dram_prices[-1]
    return [round(p * ratio, 2) for p in dram_prices]


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

    labels, (dram, samsung, hynix) = align_and_trim(
        (samsung_closes, hynix_closes), dram_closes, n=HISTORY_DAYS
    )
    aum = estimate_aum(dram)

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
