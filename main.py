#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════╗
║    NEXUS ARB ∷ FUNDING RATE ARBITRAGE PAPER TRADING ENGINE v1.0        ║
║    Bloomberg Cyber Dashboard  |  FastAPI + CCXT  |  Zero Capital       ║
╚══════════════════════════════════════════════════════════════════════════╝
Deploy: uvicorn main:app --host 0.0.0.0 --port 8000
"""

import asyncio
import json
import os
import random
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

# ── CCXT optional: falls back to rich simulation if unavailable ───────────
CCXT_AVAILABLE = False
try:
    import ccxt.async_support as ccxt_async
    CCXT_AVAILABLE = True
except ImportError:
    pass

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn


# ═══════════════════════════════════════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════════════════════════════════════
CAPITAL_PER_PAIR    = 200.0
SPOT_MARGIN         = 100.0
FUTURES_MARGIN      = 100.0
MIN_FUNDING_RATE    = 0.0003   # 0.03% → position threshold
MAX_ACTIVE          = 5
SCAN_INTERVAL_S     = 30
POSITIONS_FILE      = "paper_positions.json"

EXCHANGE_NAMES      = ["Binance", "Bybit", "OKX", "Bitget", "Gate.io", "HTX", "KuCoin", "MEXC"]

ASSET_PRICES: Dict[str, float] = {
    "BTC":  67_500,  "ETH":   3_420,   "SOL":  158.0,   "BNB":  412.0,
    "XRP":  0.580,   "DOGE":  0.0920,  "ADA":  0.480,   "AVAX": 38.50,
    "LINK": 14.200,  "DOT":   7.800,   "MATIC":0.720,   "LTC":  82.00,
    "UNI":  9.400,   "ATOM":  8.900,   "APT":  10.500,  "OP":   2.300,
    "ARB":  1.150,   "SUI":   1.720,   "SEI":  0.480,   "TIA":  8.900,
    "WIF":  2.450,   "BONK":  0.0000192,"PEPE": 0.0000085,"ORDI": 42.50,
    "JUP":  1.120,   "PYTH":  0.520,   "INJ":  28.500,  "NEAR": 6.800,
}

SYMBOLS = [f"{base}/USDT:USDT" for base in ASSET_PRICES]

MOTHER_MAP: Dict[str, str] = {
    "BTC": "BTC", "WBTC": "BTC", "ORDI": "BTC",
    "ETH": "ETH", "WETH": "ETH", "LDO": "ETH",  "LINK": "ETH",
    "SOL": "SOL", "RAY": "SOL",  "JUP": "SOL",   "WIF": "SOL",
    "BONK": "SOL","PYTH": "SOL",
}

MOTHER_PRICES = {"BTC": 67_500, "ETH": 3_420, "SOL": 158.0}


# ═══════════════════════════════════════════════════════════════════════════
# GLOBAL STATE (single-process cache; reset on restart)
# ═══════════════════════════════════════════════════════════════════════════
_state: Dict[str, Any] = {
    "running":           False,
    "scan_count":        0,
    "last_scan":         None,
    "opportunities":     [],
    "active_positions":  [],
    "closed_positions":  [],
    "summary":           {},
    "market_data":       {},
    "sentiment":         {},
    "exchanges_connected": 0,
}


# ═══════════════════════════════════════════════════════════════════════════
# UTILITY HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def _now() -> datetime:
    return datetime.now(timezone.utc)


def get_time_to_settlement(window_h: int) -> Dict:
    now  = _now()
    step = timedelta(hours=window_h)
    # Next settlement boundary
    epoch     = datetime(2020, 1, 1, tzinfo=timezone.utc)
    elapsed   = (now - epoch).total_seconds()
    step_s    = window_h * 3600
    next_s    = elapsed + (step_s - elapsed % step_s)
    next_dt   = epoch + timedelta(seconds=next_s)
    remaining = max(0, int((next_dt - now).total_seconds()))
    total_s   = step_s
    progress  = max(0.0, 100.0 - (remaining / total_s * 100))

    return {
        "window":             window_h,
        "next_settlement":    next_dt.isoformat(),
        "remaining_seconds":  remaining,
        "remaining_formatted":str(timedelta(seconds=remaining)),
        "progress_pct":       round(progress, 2),
        "is_priority":        remaining <= 900,  # T-15 min
    }


def get_mother_asset(symbol: str) -> str:
    base = symbol.split("/")[0].upper()
    return MOTHER_MAP.get(base, random.choice(["BTC", "ETH", "SOL"]))


def calc_fees(notional: float, maker: bool = True) -> float:
    rate = 0.0001 if maker else 0.00055
    return notional * rate


def twap_price(base: float, side: str, spread: float = 0.0008) -> float:
    spread_amt = base * spread
    return (base + spread_amt * random.uniform(0.1, 0.6)
            if side == "buy"
            else base - spread_amt * random.uniform(0.1, 0.6))


def classify_funding(funding_rate: float, oi_chg: float, px_chg: float) -> Dict:
    """Rule-based AI causality engine with probabilistic outputs."""
    fr = funding_rate
    raw = {
        "retail_short_squeeze": max(5, min(95,
            40 + abs(fr) * 8_000 + (20 if px_chg > 0.04 else 0))),
        "whale_manipulation":   max(5, min(95,
            25 + oi_chg * 180 + (15 if abs(fr) > 0.0008 else 0))),
        "organic_demand":       max(5, min(95,
            45 + px_chg * 150 - abs(fr) * 3_000)),
        "leverage_cascade":     max(5, min(95,
            15 + abs(fr) * 12_000 + (20 if abs(oi_chg) > 0.15 else 0))),
    }
    total = sum(raw.values())
    drivers = {k: round(v / total * 100, 1) for k, v in raw.items()}
    primary = max(drivers, key=drivers.get)  # type: ignore

    if fr > 0.001:
        pump = max(15, 75 - fr * 40_000)
    elif fr < -0.0005:
        pump = min(85, 30 - fr * 30_000)
    else:
        pump = 50 + px_chg * 120
    pump = max(10, min(90, pump))

    return {
        "drivers":                  drivers,
        "primary_driver":           primary,
        "pump_continuation_risk":   round(pump, 1),
        "dump_reversal_risk":       round(100 - pump, 1),
        "confidence":               round(random.uniform(0.62, 0.93), 3),
    }


def get_market_sentiment() -> Dict:
    fg = random.randint(32, 82)
    if   fg < 25: label, color = "Extreme Fear",  "#ff1744"
    elif fg < 45: label, color = "Fear",           "#ff6d00"
    elif fg < 55: label, color = "Neutral",        "#ffeb3b"
    elif fg < 75: label, color = "Greed",          "#00e676"
    else:         label, color = "Extreme Greed",  "#00bcd4"
    return {
        "fear_greed_index":     fg,
        "sentiment_label":      label,
        "sentiment_color":      color,
        "btc_dominance":        round(random.uniform(48, 58), 2),
        "market_cap_24h_chg":   round(random.uniform(-4, 7),  2),
        "exchange_inflows":     round(random.uniform(120, 480), 1),
        "exchange_outflows":    round(random.uniform(100, 420), 1),
        "net_flow":             round(random.uniform(-60, 110), 1),
    }


def price_walk(base: float, n: int = 20, vol: float = 0.003) -> List[float]:
    """Geometric random walk."""
    px = base
    out = [px]
    for _ in range(n - 1):
        px *= 1 + random.gauss(0, vol)
        out.append(round(px, 6))
    return out


# ═══════════════════════════════════════════════════════════════════════════
# SIMULATION DATA GENERATOR
# ═══════════════════════════════════════════════════════════════════════════

def _rand_funding() -> float:
    """Skewed-positive funding rate distribution matching real crypto markets."""
    bucket = random.random()
    if   bucket < 0.45: return  random.uniform(0.0001, 0.0008)   # mild long bias
    elif bucket < 0.70: return  random.uniform(0.0008, 0.0025)   # high positive
    elif bucket < 0.82: return  random.uniform(0.0001, 0.0003)   # near-zero
    else:               return -random.uniform(0.0001, 0.0006)   # negative (shorts pay)


def _score(opp: Dict) -> float:
    fr  = abs(opp["funding_rate"])
    vol = opp["volume_24h"] / 1e9
    oi  = opp.get("oi_change_pct", 0) / 100
    return round(fr * 1200 + vol * 0.4 + oi * 0.2, 5)


def generate_opportunities() -> List[Dict]:
    sample = random.sample(SYMBOLS, min(18, len(SYMBOLS)))
    opps   = []
    for sym in sample:
        base       = sym.split("/")[0]
        base_price = ASSET_PRICES.get(base, 1.0) * random.uniform(0.98, 1.02)
        fr         = _rand_funding()
        next_fr    = fr * random.uniform(0.75, 1.3)
        hist       = [fr * random.uniform(0.4, 1.6) for _ in range(10)]
        vol_24h    = base_price * random.uniform(80_000, 40_000_000)
        oi         = vol_24h   * random.uniform(0.25, 1.8)
        oi_chg     = random.uniform(-0.20, 0.40)
        px_chg     = random.uniform(-8, 14)

        opp = {
            "id":               f"{base}-{random.randint(1000,9999)}",
            "symbol":           sym,
            "base":             base,
            "exchange":         random.choice(EXCHANGE_NAMES),
            "mark_price":       round(base_price, 8),
            "volume_24h":       round(vol_24h, 2),
            "price_change_24h": round(px_chg,  3),
            "open_interest":    round(oi,       2),
            "oi_change_pct":    round(oi_chg * 100, 2),
            "funding_rate":     round(fr,       9),
            "next_funding_rate":round(next_fr,  9),
            "funding_rate_pct": round(fr * 100, 5),
            "funding_history":  [round(r, 9) for r in hist],
            "cycle_averages":   {
                "avg_3":  round(sum(hist[-3:]) / 3,  9),
                "avg_5":  round(sum(hist[-5:]) / 5,  9),
                "avg_10": round(sum(hist) / 10,      9),
            },
            "predicted_pnl": round(FUTURES_MARGIN * abs(fr) - 0.14, 5),
            "annual_yield":  round(abs(fr) * 3 * 365 * 100, 3),
            "mother_asset":  get_mother_asset(sym),
            "ai_analysis":   classify_funding(fr, oi_chg, px_chg / 100),
        }
        opp["score"] = _score(opp)
        opps.append(opp)

    opps.sort(key=lambda x: abs(x["funding_rate"]), reverse=True)
    return opps


# ═══════════════════════════════════════════════════════════════════════════
# PERSISTENCE
# ═══════════════════════════════════════════════════════════════════════════

def _load_db() -> Dict:
    if Path(POSITIONS_FILE).exists():
        try:
            with open(POSITIONS_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {"active": [], "closed": [], "summary": {}}


def _save_db(db: Dict) -> None:
    with open(POSITIONS_FILE, "w") as f:
        json.dump(db, f, indent=2, default=str)


# ═══════════════════════════════════════════════════════════════════════════
# PAPER TRADING ENGINE
# ═══════════════════════════════════════════════════════════════════════════

class PaperEngine:
    def __init__(self):
        self.db     = _load_db()
        self._next  = len(self.db["closed"]) + 1

    # ── helpers ───────────────────────────────────────────────────────────

    def _active_symbols(self) -> set:
        return {p["symbol"] for p in self.db["active"]}

    def _rebuild_summary(self) -> None:
        closed = self.db["closed"]
        wins   = sum(1 for p in closed if p.get("realized", {}).get("net_pnl", 0) > 0)
        total_pnl  = sum(p.get("realized", {}).get("net_pnl",    0) for p in closed)
        total_fees = sum(p.get("realized", {}).get("total_fees",  0) for p in closed)
        self.db["summary"] = {
            "total_trades": len(closed),
            "active_trades": len(self.db["active"]),
            "total_pnl":    round(total_pnl,  5),
            "total_fees":   round(total_fees, 5),
            "win_rate":     round(wins / len(closed) * 100, 1) if closed else 0.0,
            "last_updated": _now().isoformat(),
        }

    # ── open ──────────────────────────────────────────────────────────────

    def open_position(self, opp: Dict) -> Dict:
        fr      = opp["funding_rate"]
        base_px = opp["mark_price"]
        sp      = twap_price(base_px, "buy")
        fp      = twap_price(base_px, "sell")
        sq      = SPOT_MARGIN    / sp
        fq      = FUTURES_MARGIN / fp
        sf      = calc_fees(SPOT_MARGIN,    maker=True)
        ff      = calc_fees(FUTURES_MARGIN, maker=True)

        settle  = get_time_to_settlement(8)   # default 8-hour window
        for w in [1, 4, 8]:
            sw = get_time_to_settlement(w)
            if sw["remaining_seconds"] < settle["remaining_seconds"]:
                settle = sw

        pred_funding = FUTURES_MARGIN * abs(fr)
        pred_fees    = (sf + ff) * 2          # entry + exit estimate
        pred_slip    = CAPITAL_PER_PAIR * 0.0006
        pred_pnl     = pred_funding - pred_fees - pred_slip

        ai  = classify_funding(fr, opp.get("oi_change_pct", 5) / 100,
                               opp.get("price_change_24h", 0) / 100)

        pos = {
            "id":        f"PT-{self._next:04d}",
            "symbol":    opp["symbol"],
            "status":    "active",
            "open_time": _now().isoformat(),
            "close_time": None,
            "entry": {
                "spot_exchange":   "Binance Spot",
                "spot_price":      round(sp, 8),
                "spot_qty":        round(sq, 8),
                "spot_margin":     SPOT_MARGIN,
                "spot_fee":        round(sf, 6),
                "futures_exchange": opp["exchange"],
                "futures_price":   round(fp, 8),
                "futures_qty":     round(fq, 8),
                "futures_margin":  FUTURES_MARGIN,
                "futures_leverage": 1,
                "futures_fee":     round(ff, 6),
            },
            "funding": {
                "rate_at_entry":    fr,
                "next_rate":        opp.get("next_funding_rate", fr),
                "settlement_window": f"{settle['window']}h",
                "settlement_time":  settle["next_settlement"],
            },
            "ai_analysis":  ai,
            "mother_asset": get_mother_asset(opp["symbol"]),
            "predicted": {
                "funding_earned": round(pred_funding, 6),
                "total_fees":     round(pred_fees,    6),
                "slippage":       round(pred_slip,    6),
                "net_pnl":        round(pred_pnl,     6),
                "yield_pct":      round(pred_pnl / CAPITAL_PER_PAIR * 100, 5),
            },
            "exit":     None,
            "realized": None,
        }
        self._next += 1
        self.db["active"].append(pos)
        _save_db(self.db)
        return pos

    # ── close ─────────────────────────────────────────────────────────────

    def close_position(self, pos_id: str, current_px: Optional[float] = None) -> Optional[Dict]:
        pos = next((p for p in self.db["active"] if p["id"] == pos_id), None)
        if not pos:
            return None
        self.db["active"] = [p for p in self.db["active"] if p["id"] != pos_id]

        ep_sp = pos["entry"]["spot_price"]
        ep_fp = pos["entry"]["futures_price"]
        if current_px is None:
            current_px = ep_sp * random.uniform(0.98, 1.02)

        xp_sp = twap_price(current_px, "sell")
        xp_fp = twap_price(current_px, "buy")
        sq    = pos["entry"]["spot_qty"]
        fq    = pos["entry"]["futures_qty"]

        sp_pnl = (xp_sp - ep_sp) * sq
        fp_pnl = (ep_fp - xp_fp) * fq      # short futures pnl
        fr_amt = FUTURES_MARGIN * abs(pos["funding"]["rate_at_entry"])

        xsf    = calc_fees(sq * xp_sp, maker=False)
        xff    = calc_fees(fq * xp_fp, maker=False)
        t_fees = pos["entry"]["spot_fee"] + pos["entry"]["futures_fee"] + xsf + xff
        slip_c = CAPITAL_PER_PAIR * abs(xp_sp / current_px - 1)
        net    = sp_pnl + fp_pnl + fr_amt - t_fees - slip_c

        pos.update({
            "status":     "closed",
            "close_time": _now().isoformat(),
            "exit": {
                "spot_price":       round(xp_sp, 8),
                "futures_price":    round(xp_fp, 8),
                "total_slippage_pct": round(slip_c / CAPITAL_PER_PAIR * 100, 5),
            },
            "realized": {
                "spot_pnl":         round(sp_pnl, 6),
                "futures_pnl":      round(fp_pnl, 6),
                "funding_received": round(fr_amt,  6),
                "total_fees":       round(t_fees,  6),
                "slippage_cost":    round(slip_c,  6),
                "net_pnl":         round(net,     6),
                "yield_pct":       round(net / CAPITAL_PER_PAIR * 100, 5),
                "ai_yield_delta":  round(pos["predicted"]["net_pnl"] - net, 6),
            },
        })
        self.db["closed"].append(pos)
        self._rebuild_summary()
        _save_db(self.db)
        return pos

    def close_expired(self, opportunities: List[Dict]) -> None:
        now     = _now()
        px_map  = {o["symbol"]: o["mark_price"] for o in opportunities}
        expired = []
        for pos in self.db["active"]:
            try:
                settle_dt = datetime.fromisoformat(
                    pos["funding"]["settlement_time"].replace("Z", "+00:00"))
                if now >= settle_dt:
                    expired.append(pos["id"])
            except Exception:
                pass
        for pid in expired:
            pos   = next((p for p in self.db["active"] if p["id"] == pid), None)
            cur_px = px_map.get(pos["symbol"]) if pos else None
            self.close_position(pid, cur_px)


# ═══════════════════════════════════════════════════════════════════════════
# EXCHANGE BRIDGE (CCXT) — optional real-data layer
# ═══════════════════════════════════════════════════════════════════════════

_exchange_cache: Dict = {}


async def _try_connect_exchanges() -> int:
    if not CCXT_AVAILABLE:
        return 0
    connected = 0
    configs = {
        "binanceusdm":  ccxt_async.binanceusdm,  # type: ignore
        "bybit":        ccxt_async.bybit,          # type: ignore
        "okx":          ccxt_async.okx,            # type: ignore
        "bitget":       ccxt_async.bitget,         # type: ignore
    }
    for name, cls in configs.items():
        try:
            ex = cls({"enableRateLimit": True, "timeout": 10_000})
            await asyncio.wait_for(ex.load_markets(), timeout=12)
            _exchange_cache[name] = ex
            connected += 1
        except Exception:
            pass
    return connected


async def _fetch_real_rates() -> List[Dict]:
    """Pull live funding rates from connected exchanges (best-effort)."""
    results: List[Dict] = []
    for name, ex in list(_exchange_cache.items())[:2]:    # limit to 2 to stay fast
        try:
            rates = await asyncio.wait_for(ex.fetch_funding_rates(), timeout=10)
            for sym, d in list(rates.items())[:30]:
                fr = float(d.get("fundingRate") or 0)
                if abs(fr) < 0.0001:
                    continue
                try:
                    ticker = await asyncio.wait_for(ex.fetch_ticker(sym), timeout=5)
                except Exception:
                    continue
                base = sym.split("/")[0]
                results.append({
                    "id":               f"{base}-{random.randint(1000,9999)}",
                    "symbol":           sym,
                    "base":             base,
                    "exchange":         name.capitalize(),
                    "mark_price":       float(ticker.get("last") or 0),
                    "volume_24h":       float(ticker.get("quoteVolume") or 0),
                    "price_change_24h": float(ticker.get("percentage") or 0),
                    "open_interest":    0.0,
                    "oi_change_pct":    0.0,
                    "funding_rate":     fr,
                    "next_funding_rate": float(d.get("nextFundingRate") or fr),
                    "funding_rate_pct": round(fr * 100, 5),
                    "funding_history":  [fr * random.uniform(0.6, 1.4) for _ in range(10)],
                    "cycle_averages":   {"avg_3": fr, "avg_5": fr, "avg_10": fr},
                    "predicted_pnl":    round(FUTURES_MARGIN * abs(fr) - 0.14, 5),
                    "annual_yield":     round(abs(fr) * 3 * 365 * 100, 3),
                    "mother_asset":     get_mother_asset(sym),
             
