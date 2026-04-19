#!/usr/bin/env python3
"""Fetch latest market data and update data.json for the DRAM dashboard."""
import json
import sys
from datetime import datetime, timezone, timedelta

import yfinance as yf

KST = timezone(timedelta(hours=9))
DRAM_TICKER    = 'DRAM'
SAMSUNG_TICKER = '005930.KS'
HYNIX_TICKER   = '000660.KS'
HISTORY_DAYS   = 12   # trading days to keep in data.json


def fetch_closes(ticker: str, period: str = '2mo') -> dict[str, float]:
    """Download daily adjusted closing prices and return {YYYY-MM-DD: price}."""
    df = yf.download(ticker, period=period, interval='1d',
                     auto_adjust=True, progress=False)
    if df.empty:
        raise ValueError(f'No data returned for {ticker}')
    series = df['Close'].dropna()
    return {str(d.date()): float(v) for d, v in series.items()}


def align_and_trim(*price_dicts: dict, n: int) -> tuple[list[str], list[list]]:
    """Return (labels, value_lists) for the last n dates common to all series."""
    common_dates = sorted(
        set.intersection(*(set(d.keys()) for d in price_dicts))
    )
    if len(common_dates) < n:
        print(f'Warning: only {len(common_dates)} common trading days found '
              f'(needed {n}). Using all available.', file=sys.stderr)
    selected = common_dates[-n:]

    labels = [
        f"{int(d[5:7])}/{int(d[8:10])}"   # cross-platform "M/D"
        for d in selected
    ]
    values = [[d[date] for date in selected] for d in price_dicts]
    return labels, values


def estimate_aum(dram_prices: list[float]) -> list[float]:
    """Approximate daily AUM ($M) using shares_outstanding * close_price."""
    try:
        info = yf.Ticker(DRAM_TICKER).fast_info
        total_assets = getattr(info, 'total_assets', None)
        if total_assets and total_assets > 0:
            current_aum_m = total_assets / 1_000_000
            ratio = current_aum_m / dram_prices[-1]
            return [round(p * ratio, 2) for p in dram_prices]
    except Exception as e:
        print(f'AUM estimate fallback: {e}', file=sys.stderr)

    # Fallback: assume $1M base and scale by price
    return [round(p / dram_prices[0], 2) for p in dram_prices]


def main() -> None:
    print('Fetching market data…')
    dram_closes    = fetch_closes(DRAM_TICKER)
    samsung_closes = fetch_closes(SAMSUNG_TICKER)
    hynix_closes   = fetch_closes(HYNIX_TICKER)

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

    with open('data.json', 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f'✓ data.json updated: {data["updated"]} ({len(labels)} trading days)')


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        print(f'Error: {exc}', file=sys.stderr)
        sys.exit(1)
