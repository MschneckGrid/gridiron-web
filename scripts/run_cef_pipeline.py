#!/usr/bin/env python3
"""
CEF Pipeline Runner
====================
Connects your Supabase + FastTrack data to the unified gridiron_engine
and outputs cef_signals_output.json in the exact format the Command
Center expects.

Data Flow:
  1. Supabase: cef_latest_with_zscores view -> price, NAV, discount, z-scores
  2. FastTrack: momentum, volume, yield, RSI, volatility enrichment
  3. Unified Engine: L1-L6 scoring, momentum gate, vetoes, conviction
  4. Output: cef_signals_output.json -> drag into Command Center (or auto-load)

Usage:
    # Set environment variables (or edit the CONFIG section below)
    $env:SUPABASE_URL = "https://your-project.supabase.co"
    $env:SUPABASE_KEY = "your-anon-key"
    $env:FASTTRACK_API_KEY = "your-ft-key"

    # Run
    python run_cef_pipeline.py

    # Output: ./cef_signals_output.json (drop into Command Center)

Gridiron Partners | Unified Analysis Engine v1.0
"""

import json
import math
import os
import sys
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

# ============================================================================
#  CONFIG - Set these or use environment variables
# ============================================================================
SUPABASE_URL = "https://nzinvxticgyjobkqxxhl.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im56aW52eHRpY2d5am9ia3F4eGhsIiwicm9sZSI6ImFub24iLCJpYXQiOjE3Njg5NTUwOTIsImV4cCI6MjA4NDUzMTA5Mn0.YhaW1zesBaEC6lRMWx6_gyZN5Uhh8FX2wgahunHQfok"
FASTTRACK_API_KEY = "ak_live_df9cffe02b9149bea4d1f35cb9d31937_3efe3745_b01ea5d87815052d371c5d0e7b4b822e3e26708b6f2f9a7d63a98b99d69f"

OUTPUT_FILE = "cef_signals_output.json"

# If you want to hardcode credentials instead of env vars, uncomment:
# SUPABASE_URL = "https://your-project.supabase.co"
# SUPABASE_KEY = "your-anon-key"
# FASTTRACK_API_KEY = "your-ft-key"

# ============================================================================
#  SECTOR MAPPINGS (must match Command Center SECTORS object)
# ============================================================================
SECTORS = {
    "NIE":"Convertible",
    "ACV":"Converts","AVK":"Converts","CCD":"Converts","CHI":"Converts",
    "CHY":"Converts","NCV":"Converts","NCZ":"Converts",
    "ACP":"Corp/HY","ARDC":"Corp/HY","BGX":"Corp/HY","BIT":"Corp/HY",
    "BTZ":"Corp/HY","CIK":"Corp/HY","DHF":"Corp/HY","DHY":"Corp/HY",
    "DSU":"Corp/HY","EAD":"Corp/HY","ECC":"Corp/HY","ERH":"Corp/HY",
    "FSCO":"Corp/HY","FTHY":"Corp/HY","GHY":"Corp/HY","HIO":"Corp/HY",
    "HIX":"Corp/HY","HYI":"Corp/HY","HYT":"Corp/HY","ISD":"Corp/HY",
    "JGH":"Corp/HY","JHS":"Corp/HY","JQC":"Corp/HY","KIO":"Corp/HY",
    "MCI":"Corp/HY","NHS":"Corp/HY","OCCI":"Corp/HY","OXLC":"Corp/HY",
    "PAI":"Corp/HY","PCF":"Corp/HY","PHK":"Corp/HY","PTY":"Corp/HY",
    "SDHY":"Corp/HY",
    "BDJ":"Covered Call","BGY":"Covered Call","BOE":"Covered Call",
    "BXMX":"Covered Call","ETB":"Covered Call","ETJ":"Covered Call",
    "ETW":"Covered Call","ETY":"Covered Call","ETV":"Covered Call",
    "JSN":"Covered Call","SPXX":"Covered Call",
    "AWP":"Diversified","EOI":"Covered Call","EOS":"Covered Call",
    "EVV":"Diversified",
    "DMO":"Global FI","EDD":"EM Debt","EDI":"EM Debt","EMD":"EM Debt",
    "ESD":"EM Debt","FAX":"Global FI","FCO":"Global FI",
    "GDO":"Global FI","JHY":"Global FI","TEI":"Global FI",
    "AOD":"Global Equity","EAM":"Global Equity","GAM":"Global Equity",
    "JOF":"Global Equity","KF":"Global Equity",
    "GOF":"Multi-Sector","JPC":"Multi-Sector","PCM":"Multi-Sector",
    "PCN":"Multi-Sector","PDI":"Multi-Sector","PCI":"Multi-Sector",
    "PKO":"Multi-Sector","PFN":"Multi-Sector","PHD":"Multi-Sector",
    "PTN":"Multi-Sector","RCS":"Multi-Sector",
    "XFLT":"Multi-Sector","DSL":"Multi-Sector","AWF":"Multi-Sector",
    "FRA":"Multi-Sector",
    "BLE":"Muni National","BTT":"Muni National",
    "MQY":"Muni National","MUC":"Muni National",
    "MYI":"Muni National","NAD":"Muni National","NEA":"Muni National",
    "NUV":"Muni National","NVG":"Muni National","NZF":"Muni National",
    "VMO":"Muni National","VGM":"Muni National",
    "NMZ":"Muni State","NXJ":"Muni State","MYN":"Muni State",
    "MEN":"Muni State","NKX":"Muni State","NYV":"Muni State",
    "BGB":"Senior Loans","BGH":"Senior Loans","EFR":"Senior Loans",
    "EVF":"Senior Loans","FRA":"Senior Loans","JFR":"Senior Loans",
    "JSD":"Senior Loans","NSL":"Senior Loans","PPR":"Senior Loans",
    "TLI":"Senior Loans","VVR":"Senior Loans",
    "FPF":"Preferred","JPC":"Preferred",
    "LDP":"Preferred","FFC":"Preferred","PFD":"Preferred",
    "ADX":"Equity","CET":"Equity","GAB":"Equity","GDV":"Equity",
    "FUND":"Equity","RVT":"Equity","SPE":"Equity","SOR":"Equity",
    "TY":"Equity","USA":"Equity",
    "UTF":"Utilities/Infra","UTG":"Utilities/Infra","DNP":"Utilities/Infra",
    "HTD":"Utilities/Infra","IGR":"Utilities/Infra","JRI":"Utilities/Infra",
    "UTF":"Utilities/Infra","UTG":"Utilities/Infra",
    "NML":"MLP/Energy","KYN":"MLP/Energy",
    "RNP":"Real Estate","RQI":"Real Estate","JRS":"Real Estate",
    "RFI":"Real Estate","NRO":"Real Estate",
    "ASG":"Tech/Growth","BST":"Tech/Growth","BIGZ":"Tech/Growth",
    "BME":"Health/Bio","GRX":"Health/Bio","THQ":"Health/Bio","THW":"Health/Bio",
}

# Fixed Income tickers (same as CC)
FI_TICKERS = {
    "ACP","ARDC","AWF","AWP","BGB","BGH","BGX","BIT","BLE","BTT","BTZ",
    "CCD","CHI","CHY","CIK","DHF","DHY","DMO","DSL","DSU","EAD","ECC","EDD",
    "EDI","EMD","ERH","ESD","EVF","EVV","FAX","FCO","FFC","FPF","FRA","FSCO",
    "FTHY","GDO","GHY","GOF","HIO","HIX","HYI","HYT","ISD","JFR","JGH",
    "JHS","JHY","JPC","JQC","JSD","KIO","LDP","MCI","MEN",
    "MQY","MUC","MYI","MYN","NAD","NEA","NHS","NIE","NKX","NMZ","NML","NRO",
    "NSL","NUV","NVG","NXJ","NYV","OCCI","OXLC","PAI","PCI","PCF","PCM","PCN",
    "PDI","PFD","PFN","PHD","PHK","PKO","PPR","PTN","PTY","RCS","SDHY","TEI",
    "TLI","VGM","VMO","VVR","XFLT","NZF",
}


# ============================================================================
#  SUPABASE CONNECTOR
# ============================================================================

def supabase_fetch(table_or_view: str, select: str = "*",
                   params: Dict[str, str] = None) -> List[Dict]:
    """Fetch from Supabase REST API."""
    import urllib.request
    import urllib.parse

    url = f"{SUPABASE_URL}/rest/v1/{table_or_view}?select={select}"
    if params:
        for k, v in params.items():
            url += f"&{k}={urllib.parse.quote(str(v))}"

    req = urllib.request.Request(url, headers={
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    })

    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        body = e.read().decode() if e.fp else ""
        print(f"  Supabase error {e.code}: {body[:300]}")
        raise


def load_cef_data() -> List[Dict]:
    """Load latest CEF data from Supabase cef_latest_with_zscores view."""
    print("  Fetching from Supabase: cef_latest_with_zscores...")
    rows = supabase_fetch(
        "cef_latest_with_zscores",
        select="ticker,price,nav,discount_pct,z1m,z3m,z6m,z1y,z3y,z5y,z7y,z10y,trade_date,yield_pct",
    )
    print(f"  Got {len(rows)} CEF records from Supabase")
    return rows


def load_cef_history(ticker: str, days: int = 90) -> List[Dict]:
    """Load price history for momentum/trend calculations."""
    since = (date.today() - timedelta(days=days)).isoformat()
    rows = supabase_fetch(
        "cef_daily",
        select="trade_date,price,nav,discount_pct",
        params={
            "ticker": f"eq.{ticker}",
            "trade_date": f"gte.{since}",
            "order": "trade_date.asc",
        },
    )
    return rows


# ============================================================================
#  FASTTRACK CONNECTOR
# ============================================================================

def ft_auth() -> Optional[str]:
    """Authenticate with FastTrack API, return bearer token."""
    if not FASTTRACK_API_KEY:
        return None

    import http.client

    try:
        conn = http.client.HTTPSConnection("api.fasttrack.net")
        conn.request("POST", "/v2/auth", headers={
            "x-api-key": FASTTRACK_API_KEY,
            "Content-Type": "application/json",
        })
        resp = conn.getresponse()
        body = resp.read().decode()
        conn.close()

        if resp.status != 200:
            print(f"  FastTrack auth HTTP {resp.status}: {body[:200]}")
            return None

        data = json.loads(body)
        token = data.get("id_token") or data.get("token") or data.get("access_token")
        if token:
            print(f"  FastTrack authenticated OK")
            return token
        else:
            print(f"  FastTrack auth: no token in response")
            return None
    except Exception as e:
        print(f"  FastTrack auth failed: {e}")
        return None


def ft_fetch_stats(tickers: List[str], token: str) -> Dict[str, Dict]:
    """Fetch stats snapshot from FastTrack for momentum/vol/yield."""
    import urllib.request

    results = {}
    # Batch in groups of 75
    for i in range(0, len(tickers), 75):
        batch = tickers[i:i+75]
        body = json.dumps({
            "assets": batch,
            "time_periods": ["1y"],
            "benchmark": "BBG-",
            "include": ["security_info"],
        }).encode()

        req = urllib.request.Request(
            "https://api.fasttrack.net/v2/stats/snapshot",
            data=body,
            method="POST",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
        )

        try:
            with urllib.request.urlopen(req) as resp:
                data = json.loads(resp.read().decode())
                for item in data.get("results", []):
                    if item.get("error") or not item.get("ticker"):
                        continue
                    p1y = item.get("periods", {}).get("1y", {})
                    results[item["ticker"]] = {
                        "ret1y": p1y.get("return", {}).get("total"),
                        "sharpe1y": p1y.get("sharpe"),
                        "sdAnn1y": p1y.get("standard_deviation", {}).get("annualized"),
                        "maxdraw1y": p1y.get("max_drawdown"),
                        "beta1y": p1y.get("beta"),
                        "alpha1y": p1y.get("alpha"),
                    }
        except Exception as e:
            print(f"  FastTrack stats batch {i//75+1} failed: {e}")

    return results


def ft_fetch_volume(tickers: List[str], token: str) -> Dict[str, Dict]:
    """Fetch recent volume data from FastTrack."""
    import urllib.request

    end = date.today().isoformat()
    start = (date.today() - timedelta(days=35)).isoformat()
    results = {}

    for i in range(0, len(tickers), 75):
        batch = tickers[i:i+75]
        body = json.dumps({
            "assets": batch,
            "include": ["volumes", "prices_unadjusted"],
            "start_date": start,
            "end_date": end,
            "frequency": "daily",
        }).encode()

        req = urllib.request.Request(
            "https://api.fasttrack.net/v2/data",
            data=body,
            method="POST",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
        )

        try:
            with urllib.request.urlopen(req) as resp:
                data = json.loads(resp.read().decode())
                for item in data.get("results", []):
                    if item.get("error") or not item.get("ticker"):
                        continue
                    vols = item.get("volumes", [])
                    prices = item.get("prices_unadjusted", item.get("prices", []))
                    if not vols or not prices:
                        continue

                    # Calculate ADV (dollar volume)
                    dvols = []
                    for j in range(min(len(vols), len(prices))):
                        v = vols[j] if isinstance(vols[j], (int, float)) else vols[j].get("value", 0)
                        p = prices[j] if isinstance(prices[j], (int, float)) else prices[j].get("value", 0)
                        if v > 0 and p > 0:
                            dvols.append(v)

                    if dvols:
                        avg_vol = sum(dvols) / len(dvols)
                        last_vol = dvols[-1] if dvols else avg_vol
                        results[item["ticker"]] = {
                            "avg_volume": int(avg_vol),
                            "last_volume": int(last_vol),
                            "volume_ratio": round(last_vol / avg_vol, 2) if avg_vol > 0 else 1.0,
                        }
        except Exception as e:
            print(f"  FastTrack volume batch {i//75+1} failed: {e}")

    return results


def ft_fetch_reference(tickers: List[str], token: str) -> Dict[str, Dict]:
    """
    Fetch reference/portfolio data from FastTrack for L2-L4 enrichment.
    Returns: expense ratio, bond quality breakdown, top holdings,
    sector allocations, maturity/duration proxy, fund family.
    """
    import urllib.request

    results = {}
    for i in range(0, len(tickers), 25):
        batch = tickers[i:i+25]
        body = json.dumps({
            "assets": batch,
            "include": ["details", "portfolio"],
            "topcount": "10",
        }).encode()

        req = urllib.request.Request(
            "https://api.fasttrack.net/v2/reference",
            data=body,
            method="POST",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
        )

        try:
            with urllib.request.urlopen(req) as resp:
                data = json.loads(resp.read().decode())
                for item in data.get("results", []):
                    if item.get("error") or not item.get("ticker"):
                        continue

                    details = item.get("details", {})
                    port = item.get("portfolio", {})

                    # Bond quality breakdown
                    bq_raw = port.get("bondquality", {}).get("datalist", [])
                    bond_quality = {}
                    for bq in bq_raw:
                        name = bq.get("name", "").upper()
                        pct = float(bq.get("percent", 0))
                        if "AAA" in name:
                            bond_quality["aaa_pct"] = pct
                        elif "AA" in name and "AAA" not in name:
                            bond_quality["aa_pct"] = pct
                        elif name.startswith("A") and "AA" not in name:
                            bond_quality["a_pct"] = pct
                        elif "BBB" in name:
                            bond_quality["bbb_pct"] = pct
                        elif "BB" in name or "B " in name or "CCC" in name or "BELOW" in name:
                            bond_quality.setdefault("below_ig_pct", 0)
                            bond_quality["below_ig_pct"] += pct
                        elif "NOT" in name or "NR" in name:
                            bond_quality["not_rated_pct"] = pct

                    # Top holdings concentration
                    top_holdings = port.get("topten", {}).get("datalist", [])
                    top_10_pct = sum(float(h.get("percent", 0)) for h in top_holdings[:10])
                    top_1_pct = float(top_holdings[0].get("percent", 0)) if top_holdings else 0

                    # Maturity/duration proxy from bond maturity
                    bm_raw = port.get("bondmaturity", {}).get("datalist", [])
                    duration_proxy = estimate_duration_from_maturity(bm_raw)

                    # Sector breakdown
                    sectors = port.get("sectorBond", {}).get("datalist", []) or port.get("sector", {}).get("datalist", [])
                    sector_count = len([s for s in sectors if float(s.get("percent", 0)) > 1])

                    # Asset class breakdown
                    asset_classes = port.get("assetclass", {}).get("datalist", [])

                    results[item["ticker"]] = {
                        "family": details.get("family", ""),
                        "expense_ratio": parse_float(details.get("expenseratio") or details.get("expense_ratio")),
                        "category": details.get("category", ""),
                        "inception": details.get("inceptiondate", ""),
                        "bond_quality": bond_quality,
                        "top_10_pct": round(top_10_pct, 1),
                        "top_1_pct": round(top_1_pct, 1),
                        "duration_proxy": duration_proxy,
                        "sector_count": sector_count,
                        "total_holdings_est": len(top_holdings),  # FT only gives top N
                    }
        except Exception as e:
            print(f"  FastTrack reference batch {i//25+1} failed: {e}")

    return results


def parse_float(val) -> float:
    """Safely parse a float from FT response."""
    if val is None:
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def estimate_duration_from_maturity(maturity_data: List[Dict]) -> float:
    """
    Estimate effective duration from FT bond maturity breakdown.
    Maps maturity buckets to approximate duration midpoints.
    """
    if not maturity_data:
        return 0.0

    # Approximate duration for each maturity bucket
    bucket_durations = {
        "1-3": 1.5, "3-5": 3.5, "5-7": 5.0, "7-10": 7.0,
        "10-15": 10.0, "15-20": 13.0, "20-30": 18.0, "30+": 22.0,
        "0-1": 0.5, "1": 0.5, "3": 2.0, "5": 3.5, "7": 5.0,
        "10": 7.0, "15": 10.0, "20": 13.0, "30": 18.0,
    }

    weighted_dur = 0.0
    total_pct = 0.0

    for bucket in maturity_data:
        name = bucket.get("name", "")
        pct = float(bucket.get("percent", 0))
        if pct <= 0:
            continue

        # Find matching duration
        dur = 5.0  # default
        for key, val in bucket_durations.items():
            if key in name:
                dur = val
                break

        weighted_dur += dur * (pct / 100)
        total_pct += pct

    return round(weighted_dur, 1) if total_pct > 0 else 0.0


# Manager quality scores for known fund families
MANAGER_SCORES = {
    "pimco": 95, "blackrock": 90, "nuveen": 88, "eaton vance": 85,
    "cohen & steers": 88, "invesco": 80, "franklin": 78,
    "john hancock": 80, "western asset": 85, "pgim": 82,
    "doubleline": 85, "guggenheim": 80, "calamos": 75,
    "aberdeen": 75, "lazard": 78, "neuberger": 78, "kayne": 80,
    "ares": 85, "brookfield": 82, "clearbridge": 80, "royce": 78,
    "gabelli": 75, "virtus": 72, "first trust": 72, "voya": 70,
    "alliancebernstein": 82, "jpmorgan": 85, "goldman": 85,
}


def build_layer_data(ticker: str, ft_ref: Dict, supabase_row: Dict,
                     is_fi: bool) -> Dict[str, Dict]:
    """
    Build fundamentals, management, credit_data, and sentiment dicts
    from FastTrack reference data and Supabase row.
    """
    ref = ft_ref.get(ticker, {})
    bq = ref.get("bond_quality", {})

    yield_pct = float(supabase_row.get("yield_pct", 0) or 0)
    if yield_pct < 1:
        yield_pct *= 100  # normalize

    # L2: Fundamentals
    fundamentals = {}
    expense = ref.get("expense_ratio", 0)
    if expense > 0 or yield_pct > 0:
        fundamentals = {
            "distribution_rate": yield_pct,
            "nii_pct_of_distribution": 70.0 if is_fi else 40.0,  # estimate: FI funds mostly NII
            "return_of_capital_pct": 5.0 if is_fi else 15.0,     # estimate
            "unii_per_share": 0.0,
            "unii_trend": "stable",
            "effective_leverage_pct": 30.0 if is_fi else 20.0,   # typical CEF leverage
            "leverage_cost_pct": 5.0,
            "asset_coverage_ratio": 300.0,
            "baseline_expense_ratio": expense if expense > 0 else 1.5,
            "expense_ratio_vs_peers": 0.0,
            "nav_total_return_1yr": 0.0,
            "nav_total_return_3yr": 0.0,
        }

    # L3: Management
    management = {}
    family = ref.get("family", "").lower()
    if family:
        score = 60  # default
        for name, s in MANAGER_SCORES.items():
            if name in family:
                score = s
                break
        management = {
            "manager_name": ref.get("family", ""),
            "manager_aum_billions": 100,  # placeholder
            "manager_cef_count": 10,
            "manager_track_record_years": 20,
            "manager_reputation_score": score,
        }

    # L4: Portfolio / Credit
    credit_data = {}
    if bq or ref.get("top_10_pct", 0) > 0:
        ig_pct = bq.get("aaa_pct", 0) + bq.get("aa_pct", 0) + bq.get("a_pct", 0) + bq.get("bbb_pct", 0)
        credit_data = {
            "aaa_pct": bq.get("aaa_pct", 0),
            "aa_pct": bq.get("aa_pct", 0),
            "a_pct": bq.get("a_pct", 0),
            "bbb_pct": bq.get("bbb_pct", 0),
            "below_ig_pct": bq.get("below_ig_pct", 0),
            "not_rated_pct": bq.get("not_rated_pct", 0),
            "effective_duration": ref.get("duration_proxy", 5.0),
            "top_10_pct": ref.get("top_10_pct", 0),
            "top_holding_pct": ref.get("top_1_pct", 0),
            "total_holdings": max(ref.get("total_holdings_est", 0) * 10, 100),  # FT only shows top N
            "sector_count": ref.get("sector_count", 5),
            "strategy_drift_score": 5.0,
            "domestic_pct": 70.0,
            "international_pct": 30.0,
        }

    # L6: Sentiment (seasonal is date-based, always available)
    sentiment = {
        "seasonal_override": True,  # signal to use seasonal scoring
    }

    return {
        "fundamentals": fundamentals,
        "management": management,
        "credit_data": credit_data,
        "sentiment": sentiment,
    }


# ============================================================================
#  DERIVED METRICS
# ============================================================================

def compute_momentum(history: List[Dict]) -> Dict[str, float]:
    """Compute momentum indicators from price history."""
    if not history or len(history) < 10:
        return {
            "momentum_60d": 0.0,
            "discount_slope_30d": 0.0,
            "rsi_14": 50.0,
        }

    prices = [float(h["price"]) for h in history if h.get("price")]
    discounts = [float(h["discount_pct"]) for h in history if h.get("discount_pct") is not None]

    # 60-day momentum: price return over last 60 trading days
    lookback = min(60, len(prices) - 1)
    if lookback > 0 and prices[-lookback-1] > 0:
        mom_60d = (prices[-1] / prices[-lookback-1] - 1) * 100
    else:
        mom_60d = 0.0

    # 30-day discount slope (linear regression)
    slope_30d = 0.0
    if len(discounts) >= 10:
        recent = discounts[-30:]
        n = len(recent)
        x_mean = (n - 1) / 2
        y_mean = sum(recent) / n
        numer = sum((i - x_mean) * (y - y_mean) for i, y in enumerate(recent))
        denom = sum((i - x_mean) ** 2 for i in range(n))
        if denom > 0:
            slope_30d = numer / denom

    # RSI (14-day)
    rsi = 50.0
    if len(prices) >= 15:
        deltas = [prices[i] - prices[i-1] for i in range(-14, 0)]
        gains = [d for d in deltas if d > 0]
        losses = [-d for d in deltas if d < 0]
        avg_gain = sum(gains) / 14 if gains else 0.001
        avg_loss = sum(losses) / 14 if losses else 0.001
        rs = avg_gain / avg_loss if avg_loss > 0 else 100
        rsi = 100 - (100 / (1 + rs))

    return {
        "momentum_60d": round(mom_60d, 2),
        "discount_slope_30d": round(slope_30d, 5),
        "rsi_14": round(rsi, 1),
    }


def compute_peer_stats(all_data: List[Dict]) -> Dict[str, Dict]:
    """Compute peer group median discounts and percentile rankings."""
    from collections import defaultdict

    by_sector = defaultdict(list)
    for d in all_data:
        sector = SECTORS.get(d.get("ticker", ""), "Other")
        disc = d.get("discount_pct")
        if disc is not None:
            by_sector[sector].append({
                "ticker": d["ticker"],
                "discount": float(disc),
            })

    peer_stats = {}
    for sector, funds in by_sector.items():
        discs = sorted([f["discount"] for f in funds])
        median = discs[len(discs) // 2] if discs else 0
        for f in funds:
            # Percentile: what % of peers have wider (more negative) discount
            wider_count = sum(1 for d in discs if d < f["discount"])
            pctile = wider_count / len(discs) * 100 if discs else 50
            peer_stats[f["ticker"]] = {
                "peer_median_discount": median,
                "peer_percentile": round(pctile, 1),
                "peer_group": sector,
                "peer_count": len(discs),
            }

    return peer_stats


# ============================================================================
#  WEIGHT REDISTRIBUTION FOR EMPTY/ESTIMATED LAYERS
# ============================================================================

def classify_layer_quality(ls) -> str:
    """
    Classify a layer's data quality.

    Returns:
        "empty"     - no real data at all (notes flag or raw 50.0 with no components)
        "estimated" - data was provided but scores cluster near neutral (~50)
        "real"      - meaningful scoring dispersion, treat as real data
    """
    # --- Check 1: Explicit empty flags in notes ---
    if ls.notes:
        for note in ls.notes:
            nl = note.lower()
            if any(phrase in nl for phrase in [
                "neutral score", "no data", "no fundamentals",
                "no management", "no portfolio", "no sentiment",
                "using default",
            ]):
                return "empty"

    # --- Check 2: Exact 50.0 with no component breakdown = definite empty ---
    if ls.raw_score == 50.0 and not ls.components:
        return "empty"

    # --- Check 3: Component variance analysis ---
    if ls.components:
        comp_values = [v for v in ls.components.values()
                       if isinstance(v, (int, float))]
        if comp_values:
            mean_val = sum(comp_values) / len(comp_values)
            spread = max(comp_values) - min(comp_values)
            all_near_50 = all(35 <= v <= 65 for v in comp_values)
            variance = sum((v - mean_val) ** 2 for v in comp_values) / len(comp_values)

            # Signature of estimated data: everything between 35-65,
            # low variance, and raw score near 50
            if all_near_50 and variance < 100 and 40 <= ls.raw_score <= 60:
                return "estimated"

            # Also catch: tight spread centered on ~50
            if spread < 15 and 42 <= mean_val <= 58:
                return "estimated"

    # --- Check 4: No components but raw near 50 ---
    if not ls.components and 45 <= ls.raw_score <= 55:
        return "estimated"

    return "real"


def reweight_composite(result):
    """
    Redistribute weight from empty/estimated layers to layers with real data.

    Three-tier approach:
      - "empty" layers:     100% of weight redistributed
      - "estimated" layers:  75% of weight redistributed, 25% retained
      - "real" layers:      Full weight retained

    Prevents HOLD compression where estimated data (~50) on 60% of the
    composite drags everything into the 40-64 band.

    Also properly gates STRONG_BUY on momentum (old version only gated BUY).
    """
    from gridiron_engine.core.models import Signal

    layer_scores = result.layer_scores
    if not layer_scores:
        return

    # --- Classify every layer ---
    l1_score = None
    weight_to_redistribute = 0.0
    real_layers = []
    retained_estimated = []

    for ls in layer_scores:
        if ls.layer_number == 1:
            real_layers.append(ls)
            l1_score = ls
        elif ls.weight == 0:
            pass  # L5 penalty-only, skip
        else:
            quality = classify_layer_quality(ls)
            if quality == "real":
                real_layers.append(ls)
            elif quality == "empty":
                weight_to_redistribute += ls.weight
            elif quality == "estimated":
                redistribute_portion = ls.weight * 0.75
                keep_portion = ls.weight * 0.25
                weight_to_redistribute += redistribute_portion
                retained_estimated.append((ls, keep_portion))

    if not l1_score or weight_to_redistribute == 0:
        return  # all layers have real data, no reweighting needed

    # --- Redistribute to real layers proportionally ---
    real_total_weight = sum(ls.weight for ls in real_layers)
    if real_total_weight == 0:
        return

    new_composite = 0.0

    for ls in real_layers:
        share = ls.weight / real_total_weight
        new_weight = ls.weight + share * weight_to_redistribute
        new_weighted = ls.raw_score * new_weight
        ls.weight = round(new_weight, 4)
        ls.weighted_score = round(new_weighted, 2)
        new_composite += new_weighted

    for ls, keep_weight in retained_estimated:
        new_weighted = ls.raw_score * keep_weight
        ls.weight = round(keep_weight, 4)
        ls.weighted_score = round(new_weighted, 2)
        new_composite += new_weighted

    # --- Update result ---
    result.composite_score = round(new_composite, 2)

    # --- Score stretching when L1 dominates ---
    # When most weight was redistributed (L1-only mode), the composite
    # clusters in a narrow band around 50. Stretch scores so the actual
    # dispersion maps to the full signal spectrum.
    #
    # Stretch factor scales with how much weight was redistributed:
    #   100% redistributed → 1.8x stretch (full L1-only)
    #   50% redistributed  → 1.4x stretch
    #   0% redistributed   → 1.0x (no stretch, normal operation)
    total_weight = sum(ls.weight for ls in layer_scores if ls.weight > 0)
    redistribution_pct = weight_to_redistribute / (total_weight + weight_to_redistribute) if (total_weight + weight_to_redistribute) > 0 else 0
    stretch_factor = 1.0 + (2.2 * redistribution_pct)

    stretched_composite = 50.0 + (new_composite - 50.0) * stretch_factor
    stretched_composite = max(0.0, min(100.0, stretched_composite))  # clamp 0-100

    result.adjusted_score = round(stretched_composite - result.risk_penalties, 2)

    # --- Re-determine signal with FULL momentum gating ---
    score = result.adjusted_score
    mom_confirmed = result.metadata.get("momentum_confirmed", False)

    if score >= 80:
        result.signal = Signal.STRONG_BUY if mom_confirmed else Signal.WAIT
    elif score >= 65:
        result.signal = Signal.BUY if mom_confirmed else Signal.WAIT
    elif score >= 40:
        result.signal = Signal.HOLD
    elif score >= 25:
        result.signal = Signal.SELL
    else:
        result.signal = Signal.STRONG_SELL


# ============================================================================
#  ENGINE RUNNER
# ============================================================================

def build_market_data(row: Dict, momentum: Dict, volume: Dict,
                      peer: Dict) -> "MarketData":
    """Convert Supabase row + enrichment into engine MarketData."""
    from gridiron_engine.core.models import AssetClass, MarketData

    ticker = row["ticker"]
    price = float(row.get("price", 0) or 0)
    nav = float(row.get("nav", 0) or 0)
    disc = float(row.get("discount_pct", 0) or 0)

    z1y = float(row.get("z1y", 0) or 0)
    z3y = float(row.get("z3y", 0) or 0)
    z5y = float(row.get("z5y", 0) or 0)

    yield_pct = float(row.get("yield_pct", 0) or 0)

    vol_data = volume.get(ticker, {})
    peer_data = peer.get(ticker, {})
    mom_data = momentum.get(ticker, {})

    return MarketData(
        ticker=ticker,
        asset_class=AssetClass.CEF,
        as_of_date=date.fromisoformat(row.get("trade_date", date.today().isoformat())),
        price=price,
        nav=nav,
        volume=vol_data.get("last_volume", 0),
        avg_volume_90d=vol_data.get("avg_volume", 1),
        discount_pct=disc * 100 if abs(disc) < 1 else disc,  # normalize to %
        z_score_1yr=z1y,
        z_score_3yr=z3y,
        z_score_5yr=z5y,
        discount_percentile_5yr=50.0,  # placeholder - compute from history
        peer_median_discount=peer_data.get("peer_median_discount", 0) * 100 if abs(peer_data.get("peer_median_discount", 0)) < 1 else peer_data.get("peer_median_discount", 0),
        momentum_60d=mom_data.get("momentum_60d", 0),
        discount_slope_30d=mom_data.get("discount_slope_30d", 0),
        rsi_14=mom_data.get("rsi_14", 50),
        current_yield=yield_pct * 100 if yield_pct < 1 else yield_pct,
        yield_on_nav=yield_pct * 100 if yield_pct < 1 else yield_pct,
        yield_spread_vs_peers=0.0,
        volatility_30d=15.0,   # placeholder - compute from history
        ulcer_index=5.0,       # placeholder
    )


def engine_output_to_cc_format(result, peer_data: Dict) -> Dict:
    """
    Convert unified engine SEFOutput to Command Center signal format.
    This is the critical mapping - every field the CC table expects.
    """
    d = result.to_dict()
    km = d.get("key_metrics", {})
    l1 = {}
    for ls in d.get("layer_scores", []):
        if ls["number"] == 1:
            l1 = ls.get("components", {})
            break

    ticker = d["ticker"]
    peer = peer_data.get(ticker, {})

    # Map signal enum to CC format (underscores, no spaces)
    signal_map = {
        "STRONG BUY": "STRONG_BUY",
        "BUY": "BUY",
        "WAIT": "WAIT",
        "HOLD": "HOLD",
        "SELL": "SELL",
        "STRONG SELL": "STRONG_SELL",
        "AVOID": "AVOID",
    }

    # Momentum confirmation from engine metadata
    mom_confirmed = d.get("metadata", {}).get("momentum_confirmed", False)

    # Z-score signal mapping
    zscore = km.get("z_score_1yr", 0)
    if zscore <= -2:
        zscore_signal = "STRONG_BUY"
    elif zscore <= -1:
        zscore_signal = "BUY"
    elif zscore <= 1:
        zscore_signal = "HOLD"
    elif zscore <= 2:
        zscore_signal = "SELL"
    else:
        zscore_signal = "STRONG_SELL"

    # Peer signal from percentile
    peer_pctile = peer.get("peer_percentile", 50)
    if peer_pctile <= 15:
        peer_signal = "STRONG_BUY"
    elif peer_pctile <= 30:
        peer_signal = "BUY"
    elif peer_pctile <= 70:
        peer_signal = "HOLD"
    elif peer_pctile <= 85:
        peer_signal = "SELL"
    else:
        peer_signal = "STRONG_SELL"

    return {
        "ticker": ticker,
        "final_signal": signal_map.get(d["signal"], d["signal"]),
        "regime_adjusted_score": round(d["adjusted_score"]),
        "composite_score": round(d["composite_score"], 1),

        # L1 components the CC displays
        "zscore": round(zscore, 2),
        "zscore_signal": zscore_signal,
        "peer_percentile": round(peer_pctile, 1),
        "peer_signal": peer_signal,
        "momentum_score": round(l1.get("momentum_trend", 50)),
        "momentum_confirmed": mom_confirmed,
        "volume_ratio": round(km.get("volume_ratio", 1.0) if "volume_ratio" in km else 1.0, 1),

        # Price data
        "price": km.get("price"),
        "nav": km.get("nav"),
        "discount_pct": round(km.get("discount_pct", 0), 4),

        # Sector / classification
        "peer_group": peer.get("peer_group", SECTORS.get(ticker, "Other")),
        "asset_class": "FI" if ticker in FI_TICKERS else "EQ",

        # Momentum detail
        "discount_narrowing": l1.get("discount_slope", 50) > 50,
        "price_above_ma50": l1.get("momentum_60d", 50) > 50,
        "volume_spike": (km.get("volume_ratio", 1.0) if "volume_ratio" in km else 1.0) > 1.5,

        # Regime (from unified engine)
        "regime_shift": False,  # populated by regime detector if running full pipeline

        # Engine metadata (extra fields the CC can use)
        "confidence": d.get("confidence", "LOW"),
        "risk_penalties": d.get("risk_penalties", 0),
        "veto_triggers": d.get("veto_triggers", []),

        # Layer detail (new - the CC can display if upgraded)
        "layer_scores": d.get("layer_scores", []),
        "conviction": d.get("conviction"),

        # Seasonal score (CEF-specific)
        "seasonal_score": next(
            (ls["components"].get("seasonal", 50)
             for ls in d.get("layer_scores", []) if ls["number"] == 6),
            50
        ),

        # Current yield for CC enrichment
        "yield_pct": km.get("current_yield", 0),

        # Sub-scores the CC detail panel renders
        "yield_score": round(l1.get("yield_volatility", 50)),
        "volume_score": round(min(100, (km.get("volume_ratio", 1.0) if "volume_ratio" in km else 1.0) * 50)),
        "dist_signal": "HOLD",  # placeholder - needs distribution analysis data
    }


def run_pipeline():
    """Main pipeline execution."""
    print("=" * 70)
    print("GRIDIRON ENGINE - CEF Pipeline Runner")
    print("=" * 70)

    # Validate config
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("\nERROR: Supabase credentials not configured.")
        print("Set environment variables:")
        print('  $env:SUPABASE_URL = "https://your-project.supabase.co"')
        print('  $env:SUPABASE_KEY = "your-anon-key"')
        print("\nOr edit the CONFIG section at the top of this script.")
        sys.exit(1)

    # --- Step 1: Load CEF data from Supabase ---
    print("\n[1/5] Loading CEF data from Supabase...")
    try:
        cef_data = load_cef_data()
    except Exception as e:
        print(f"  ERROR: {e}")
        sys.exit(1)

    # Filter to known universe
    cef_data = [r for r in cef_data if r.get("ticker") in SECTORS]
    tickers = [r["ticker"] for r in cef_data]
    print(f"  {len(tickers)} CEFs in universe")

    # --- Step 2: Compute peer stats ---
    print("\n[2/5] Computing peer group statistics...")
    peer_stats = compute_peer_stats(cef_data)

    # --- Step 3: FastTrack enrichment (optional) ---
    ft_volume = {}
    ft_stats = {}
    ft_reference = {}
    if FASTTRACK_API_KEY:
        print("\n[3/5] Fetching FastTrack enrichment data...")
        token = ft_auth()
        if token:
            print(f"  Fetching volume for {len(tickers)} tickers...")
            ft_volume = ft_fetch_volume(tickers, token)
            print(f"  Got volume for {len(ft_volume)} tickers")

            print(f"  Fetching stats...")
            ft_stats = ft_fetch_stats(tickers, token)
            print(f"  Got stats for {len(ft_stats)} tickers")

            print(f"  Fetching reference/portfolio data for L2-L4...")
            ft_reference = ft_fetch_reference(tickers, token)
            print(f"  Got reference for {len(ft_reference)} tickers")
        else:
            print("  FastTrack auth failed - running with Supabase data only")
    else:
        print("\n[3/5] Skipping FastTrack (no API key) - using Supabase data only")

    # --- Step 4: Compute momentum from Supabase history ---
    print("\n[4/5] Computing momentum indicators...")
    momentum_data = {}
    batch_size = 20
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i+batch_size]
        for ticker in batch:
            try:
                history = load_cef_history(ticker, days=90)
                momentum_data[ticker] = compute_momentum(history)
            except Exception:
                momentum_data[ticker] = {
                    "momentum_60d": 0.0,
                    "discount_slope_30d": 0.0,
                    "rsi_14": 50.0,
                }
        done = min(i + batch_size, len(tickers))
        print(f"  Momentum computed: {done}/{len(tickers)}", end="\r")
    print(f"  Momentum computed: {len(momentum_data)}/{len(tickers)} tickers")

    # --- Step 5: Run engine ---
    print("\n[5/5] Running unified engine on all CEFs...")
    from gridiron_engine import UnifiedEngine
    from gridiron_engine.profiles.cef import CEFProfile

    engine = UnifiedEngine()
    engine.register_profile(CEFProfile())

    cc_signals = []
    errors = []

    for row in cef_data:
        ticker = row["ticker"]
        try:
            md = build_market_data(row, momentum_data, ft_volume, peer_stats)

            # Build L2-L6 data from FastTrack reference + Supabase
            is_fi = ticker in FI_TICKERS
            layer_data = build_layer_data(ticker, ft_reference, row, is_fi)

            result = engine.evaluate(
                market_data=md,
                fundamentals=layer_data["fundamentals"],
                management=layer_data["management"],
                credit_data=layer_data["credit_data"],
                sentiment=layer_data["sentiment"],
            )

            # --- Weight redistribution for empty layers ---
            # When L2-L6 have no real data (score=50 default), their weight
            # should flow to L1 so real market data drives the signal.
            reweight_composite(result)

            cc_signal = engine_output_to_cc_format(result, peer_stats)
            cc_signals.append(cc_signal)
        except Exception as e:
            errors.append(f"{ticker}: {e}")

    print(f"  Evaluated {len(cc_signals)} CEFs successfully")
    if errors:
        print(f"  {len(errors)} errors:")
        for err in errors[:10]:
            print(f"    {err}")

    # Enrichment stats
    ft_enriched = len([t for t in tickers if t in ft_reference])
    print(f"  FT reference data: {ft_enriched}/{len(tickers)} tickers (powers L2-L4)")
    print(f"  FT volume data: {len(ft_volume)}/{len(tickers)} tickers")

    # --- Signal summary ---
    from collections import Counter
    sig_counts = Counter(s["final_signal"] for s in cc_signals)
    print(f"\n  Signal distribution:")
    for sig in ["STRONG_BUY", "BUY", "WAIT", "HOLD", "SELL", "STRONG_SELL"]:
        count = sig_counts.get(sig, 0)
        if count > 0:
            print(f"    {sig}: {count}")

    # --- Alerts ---
    alerts = []
    for sig in cc_signals:
        s = sig["final_signal"]
        t = sig["ticker"]
        sc = sig["regime_adjusted_score"]
        dp = sig["discount_pct"]
        z = sig.get("zscore", 0)

        if s in ("STRONG_BUY", "BUY", "WAIT"):
            alerts.append({
                "ticker": t, "alert_type": "BUY_SIGNAL",
                "message": f"{s.replace('_',' ')} signal — score {sc}, discount {dp:.1f}%",
                "composite_score": sc, "discount_pct": dp,
            })
        elif s in ("SELL", "STRONG_SELL"):
            alerts.append({
                "ticker": t, "alert_type": "SELL_SIGNAL",
                "message": f"{s.replace('_',' ')} signal — score {sc}, discount {dp:.1f}%",
                "composite_score": sc, "discount_pct": dp,
            })
        if z is not None and z <= -2:
            alerts.append({
                "ticker": t, "alert_type": "DEEP_DISCOUNT",
                "message": f"Z-score {z:.2f} — historically cheap",
                "composite_score": sc, "discount_pct": dp,
            })

    print(f"  Alerts generated: {len(alerts)}")

    # --- Output ---
    output = {
        "generated_at": datetime.now().isoformat(),
        "engine_version": "gridiron_engine v1.0",
        "universe_count": len(cc_signals),
        "all_signals": cc_signals,
        "alerts": alerts,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, default=str)

    print(f"\n  Output: {OUTPUT_FILE}")
    print(f"  Size: {os.path.getsize(OUTPUT_FILE):,} bytes")
    print()
    print("=" * 70)
    print("Done. Command Center will auto-load from Supabase on next refresh.")
    print(f"  Local JSON: {OUTPUT_FILE} ({os.path.getsize(OUTPUT_FILE):,} bytes)")
    print(f"  Uploading to Supabase...")

    # --- Upload to Supabase ---
    try:
        import urllib.request
        # Upload full JSON to cc_engine_output table for legacy support
        json_bytes = json.dumps(output, default=str).encode("utf-8")
        upsert_url = f"{SUPABASE_URL}/rest/v1/cc_engine_output?on_conflict=id"
        body = json.dumps({"id": "latest", "payload": output, "updated_at": datetime.now().isoformat()}, default=str).encode("utf-8")
        req = urllib.request.Request(upsert_url, data=body, method="POST", headers={
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates",
        })
        try:
            with urllib.request.urlopen(req) as resp:
                print(f"  Uploaded to Supabase cc_engine_output (id=latest)")
        except Exception as e:
            print(f"  cc_engine_output upload skipped: {e}")
    except Exception as e:
        print(f"  Supabase upload skipped: {e}")

    print("=" * 70)


if __name__ == "__main__":
    run_pipeline()
