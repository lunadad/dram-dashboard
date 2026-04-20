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


def align_and_trim(*price_dicts: dict, n: int) -> tuple[list[str], list[list]]:
    common_dates = sorted(set.intersection(*(set(d.keys()) for d in price_dicts)))
    if len(common_dates) < n:
        print(f'Warning: only {len(common_dates)} common trading days '
              f'(needed {n}). Using all available.', file=sys.stderr)
    selected = common_dates[-n:]
    labels = [f"{int(d[5:7])}/{int(d[8:10])}" for d in selected]
    values = [[d[date] for date in selected] for d in price_dicts]
    return labels, values


QUOTE_SUMMARY_URL = ('https://query2.finance.yahoo.com/v10/finance/quoteSummary/'
                     '{symbol}?modules=defaultKeyStatistics,summaryDetail')


def fetch_total_assets(ticker: str) -> float | None:
    """Return ETF total net assets in USD, or None if unavailable."""
    try:
        payload = http_get_json(QUOTE_SUMMARY_URL.format(symbol=ticker), retries=2)
    except Exception as e:
        print(f'  AUM lookup failed: {e}', file=sys.stderr)
        return None
    result = (payload.get('quoteSummary', {}).get('result') or [None])[0]
    if not result:
        return None
    for mod in ('defaultKeyStatistics', 'summaryDetail'):
        node = (result.get(mod) or {}).get('totalAssets') or {}
        raw = node.get('raw')
        if raw and raw > 0:
            return float(raw)
    return None


def estimate_aum(dram_prices: list[float]) -> list[float]:
    """Approximate historical AUM ($M) by scaling current AUM proportionally
    to DRAM ETF closing prices. Uses Yahoo quoteSummary totalAssets when
    available; otherwise falls back to a $732M anchor from the last known
    reliable datapoint (2026-04-17)."""
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
        dram_closes, samsung_closes, hynix_closes, n=HISTORY_DAYS
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
