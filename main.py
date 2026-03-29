#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Google Ads Bidding Health Dashboard
FastAPI Web-App für Railway Deployment
"""

import os
import json
import statistics
from dataclasses import dataclass, field, asdict
from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
import uvicorn

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google.protobuf.field_mask_pb2 import FieldMask

app = FastAPI(title="Google Ads Bidding Health Dashboard")

# ============================
# CONFIG
# ============================

CUSTOMER_ID = os.environ.get("GOOGLE_ADS_CUSTOMER_ID", "")

MIN_CAP_MICROS        = 10_000
NEAR30_MIN            = 24
NEAR30_MAX            = 29
NEAR15_NODECREASE_MIN = 12
NEAR15_NODECREASE_MAX = 14
MAX_CAP_WARNING_EUR   = 6.00
MAX_CAP_WARNING_MICROS = int(MAX_CAP_WARNING_EUR * 1_000_000)

# Median-CPC: min. Wochen ohne Urlaubsverzerrung, sonst Fallback auf avg
MEDIAN_MIN_WEEKS = 4
# Woche gilt als Urlaubswoche wenn >= N ihrer Tage Urlaub sind
HOLIDAY_WEEK_MIN_DAYS = 3
# Lookback für Median
MEDIAN_LOOKBACK_DAYS = 90

# ============================
# HOLIDAY CONFIG
# ============================

_BASE_DIR      = os.environ.get("DATA_DIR", os.path.dirname(os.path.abspath(__file__)))
HOLIDAYS_PATH  = os.path.join(_BASE_DIR, "holidays.json")
SNAPSHOTS_PATH = os.path.join(_BASE_DIR, "snapshots.json")


def load_holiday_periods() -> List[Tuple[date, date, str]]:
    """Lädt Urlaubsperioden aus holidays.json. Kein File = kein Crash."""
    try:
        with open(HOLIDAYS_PATH) as f:
            data = json.load(f)
        return [
            (date.fromisoformat(p["start"]),
             date.fromisoformat(p["end"]),
             p["name"])
            for p in data.get("periods", [])
        ]
    except FileNotFoundError:
        return []
    except Exception:
        return []


HOLIDAY_PERIODS: List[Tuple[date, date, str]] = load_holiday_periods()


def _is_relevant_holiday(h_start: date, h_end: date) -> bool:
    """Nur Perioden >= 5 Tage (Ostern, Sommer, Herbst, Weihnachten)."""
    return (h_end - h_start).days + 1 >= 5


def count_holiday_days(period_days: int) -> Tuple[int, List[str]]:
    """Zählt Urlaubstage (nur Perioden >= 5 Tage) in den letzten period_days Tagen."""
    today   = date.today()
    start   = today - timedelta(days=period_days)
    count   = 0
    affected: set = set()

    for n in range(period_days):
        check = start + timedelta(days=n)
        for h_start, h_end, h_name in HOLIDAY_PERIODS:
            if not _is_relevant_holiday(h_start, h_end):
                continue
            if h_start <= check <= h_end:
                count += 1
                affected.add(h_name)
                break

    return count, sorted(affected)


def is_holiday_week(week_monday: date) -> bool:
    """True wenn die Woche ab week_monday >= HOLIDAY_WEEK_MIN_DAYS Urlaubstage enthält."""
    count = 0
    for n in range(7):
        check = week_monday + timedelta(days=n)
        for h_start, h_end, _ in HOLIDAY_PERIODS:
            if not _is_relevant_holiday(h_start, h_end):
                continue
            if h_start <= check <= h_end:
                count += 1
                break
    return count >= HOLIDAY_WEEK_MIN_DAYS


def normalize_clicks(clicks: int, period_days: int, holiday_days: int) -> int:
    """Hochrechnung auf Vollperiode ohne Urlaubstage."""
    if holiday_days <= 0:
        return clicks
    effective = max(1, period_days - holiday_days)
    return int(clicks * (period_days / effective))


# ============================
# DATA STRUCTURES
# ============================

@dataclass
class StrategyRow:
    resource_name: str
    strategy_id: int
    name: str
    status: str
    current_cap_micros: Optional[int]
    enabled_campaigns: int

    clicks_7d:  int = 0
    clicks_14d: int = 0
    clicks_30d: int = 0

    avg_cpc_7d_micros:  int = 0
    avg_cpc_14d_micros: int = 0
    avg_cpc_30d_micros: int = 0

    ctr_7d:  float = 0.0
    ctr_14d: float = 0.0
    ctr_30d: float = 0.0

    # Health
    budget_lost_is_30d:    float = 0.0
    rank_lost_is_30d:      float = 0.0
    impressions_30d:       int   = 0
    lost_impressions_budget: int = 0

    cap_limited_campaigns:    int = 0
    budget_limited_campaigns: int = 0

    budget_recommendation_micros: Optional[int] = None
    recommended_budget_micros:    Optional[int] = None

    basis_window:        str          = ""
    basis_avg_cpc_micros: int         = 0
    new_cap_micros:      Optional[int] = None   # avg-basiert
    cap_delta_micros:    int          = 0

    # Median-CPC
    median_cpc_micros:       int          = 0    # bereinigter 90d-Median
    new_cap_median_micros:   Optional[int] = None # median-basierter Cap
    cap_delta_median_micros: int          = 0
    median_weeks_used:       int          = 0    # wie viele Wochen flossen ein
    median_fallback:         bool         = False # True = zu wenig Daten, Fallback auf avg

    bucket: str = "SKIP"
    reason: str = ""
    recommendation: str = ""

    click_opportunity: float = 0.0
    score:             float = 0.0

    # Holiday-Normalisierung (Gate)
    clicks_14d_normalized: int  = 0
    clicks_30d_normalized: int  = 0
    holiday_normalized:    bool = False


@dataclass
class MetricsAccumulator:
    clicks: int = 0
    cpc_values: List[int] = field(default_factory=list)
    ctr_sum: float = 0.0
    campaign_count: int = 0
    budget_lost_is_sum: float = 0.0
    rank_lost_is_sum:   float = 0.0
    impressions: int = 0

    def add(self, clicks: int, avg_cpc: int, ctr: float,
            budget_lost_is: float = 0.0, rank_lost_is: float = 0.0,
            impressions: int = 0) -> None:
        self.clicks += clicks
        if avg_cpc > 0:
            self.cpc_values.append(avg_cpc)
        self.ctr_sum           += ctr
        self.campaign_count    += 1
        self.budget_lost_is_sum += budget_lost_is
        self.rank_lost_is_sum  += rank_lost_is
        self.impressions       += impressions

    def finalize(self) -> dict:
        return {
            "clicks":       self.clicks,
            "avg_cpc":      int(sum(self.cpc_values) / len(self.cpc_values)) if self.cpc_values else 0,
            "ctr":          self.ctr_sum / self.campaign_count if self.campaign_count else 0.0,
            "budget_lost_is": self.budget_lost_is_sum / self.campaign_count if self.campaign_count else 0.0,
            "rank_lost_is": self.rank_lost_is_sum  / self.campaign_count if self.campaign_count else 0.0,
            "impressions":  self.impressions,
        }


# ============================
# GOOGLE ADS CLIENT
# ============================

def get_client() -> GoogleAdsClient:
    config = {
        "developer_token": os.environ["GOOGLE_ADS_DEVELOPER_TOKEN"],
        "client_id":       os.environ["GOOGLE_ADS_CLIENT_ID"],
        "client_secret":   os.environ["GOOGLE_ADS_CLIENT_SECRET"],
        "refresh_token":   os.environ["GOOGLE_ADS_REFRESH_TOKEN"],
        "login_customer_id": os.environ.get("GOOGLE_ADS_LOGIN_CUSTOMER_ID", ""),
        "use_proto_plus": True,
    }
    return GoogleAdsClient.load_from_dict(config, version="v21")


def search(client: GoogleAdsClient, customer_id: str, query: str):
    return client.get_service("GoogleAdsService").search(
        customer_id=customer_id, query=query
    )


# ============================
# HELPERS
# ============================

def micros_to_str(micros: Optional[int]) -> str:
    return "none" if micros is None else f"{micros/1_000_000:.2f}"

def safe_int(x) -> int:
    try: return int(x)
    except: return 0

def safe_float(x) -> float:
    try: return float(x)
    except: return 0.0

def compute_new_cap_plus10(basis: int) -> int:
    return max(MIN_CAP_MICROS, int(round(basis * 1.10)))

def apply_no_decrease_near15_rule(s: StrategyRow) -> bool:
    if s.new_cap_micros is None or s.current_cap_micros is None:
        return False
    if NEAR15_NODECREASE_MIN <= s.clicks_14d <= NEAR15_NODECREASE_MAX \
       and s.new_cap_micros < s.current_cap_micros:
        s.bucket           = "SKIP"
        s.reason           = f"NO_DECREASE_NEAR15: clicks14={s.clicks_14d}, would reduce cap"
        s.new_cap_micros   = None
        s.cap_delta_micros = 0
        s.basis_window     = ""
        s.basis_avg_cpc_micros = 0
        return True
    return False


# ============================
# GAQL QUERIES
# ============================

def gaql_strategies() -> str:
    return """
    SELECT
      bidding_strategy.resource_name,
      bidding_strategy.id,
      bidding_strategy.name,
      bidding_strategy.type,
      bidding_strategy.status,
      bidding_strategy.target_spend.cpc_bid_ceiling_micros
    FROM bidding_strategy
    WHERE
      bidding_strategy.type = TARGET_SPEND
      AND bidding_strategy.status != REMOVED
    """

def gaql_metrics(during: str) -> str:
    return f"""
    SELECT
      bidding_strategy.resource_name,
      metrics.clicks,
      metrics.average_cpc,
      metrics.ctr,
      metrics.search_budget_lost_impression_share,
      metrics.search_rank_lost_impression_share,
      metrics.impressions
    FROM campaign
    WHERE
      campaign.status = ENABLED
      AND segments.date DURING {during}
      AND campaign.bidding_strategy IS NOT NULL
    """

def gaql_daily_cpc(days: int) -> str:
    """Tagesgranulare CPC-Daten für Median-Berechnung."""
    today     = date.today()
    date_from = (today - timedelta(days=days)).isoformat()
    date_to   = (today - timedelta(days=1)).isoformat()
    return f"""
    SELECT
      bidding_strategy.resource_name,
      segments.date,
      metrics.clicks,
      metrics.average_cpc
    FROM campaign
    WHERE
      campaign.status = ENABLED
      AND campaign.bidding_strategy IS NOT NULL
      AND metrics.clicks > 0
      AND segments.date BETWEEN '{date_from}' AND '{date_to}'
    """

def gaql_campaign_system_status() -> str:
    return """
    SELECT
      campaign.bidding_strategy,
      campaign.name,
      campaign.bidding_strategy_system_status,
      metrics.search_budget_lost_impression_share
    FROM campaign
    WHERE
      campaign.status = ENABLED
      AND campaign.bidding_strategy IS NOT NULL
      AND segments.date DURING LAST_30_DAYS
    """

def gaql_budget_recommendations() -> str:
    # In v21 sind current/recommended_budget_amount_micros nicht direkt selektierbar.
    # Stattdessen nur prüfen ob eine Budget-Recommendation für die Strategie existiert.
    return """
    SELECT
      recommendation.resource_name,
      recommendation.type,
      campaign.bidding_strategy,
      campaign.campaign_budget
    FROM recommendation
    WHERE recommendation.type = CAMPAIGN_BUDGET
    """


# ============================
# FETCHING
# ============================

def fetch_strategies(client, customer_id) -> Dict[str, StrategyRow]:
    rows = {}
    for r in search(client, customer_id, gaql_strategies()):
        bs  = r.bidding_strategy
        cap = bs.target_spend.cpc_bid_ceiling_micros
        rows[bs.resource_name] = StrategyRow(
            resource_name    = bs.resource_name,
            strategy_id      = bs.id,
            name             = bs.name,
            status           = str(bs.status).replace("BiddingStrategyStatus.", ""),
            current_cap_micros = int(cap) if cap else None,
            enabled_campaigns  = 0,
        )
    return rows


def fetch_enabled_campaign_counts(client, customer_id, strategies):
    q = """
    SELECT campaign.bidding_strategy, campaign.id
    FROM campaign
    WHERE campaign.status = ENABLED
      AND campaign.bidding_strategy IS NOT NULL
    """
    counts: Dict[str, int] = {}
    for r in search(client, customer_id, q):
        rn = r.campaign.bidding_strategy
        counts[rn] = counts.get(rn, 0) + 1
    for s in strategies.values():
        s.enabled_campaigns = counts.get(s.resource_name, 0)


def fetch_metrics_aggregated(client, customer_id) -> Dict[str, Dict[str, dict]]:
    accumulators: Dict[str, Dict[str, MetricsAccumulator]] = {}
    windows = {"7d": "LAST_7_DAYS", "14d": "LAST_14_DAYS", "30d": "LAST_30_DAYS"}
    for key, during in windows.items():
        for r in search(client, customer_id, gaql_metrics(during)):
            rn = r.bidding_strategy.resource_name
            accumulators.setdefault(rn, {}).setdefault(key, MetricsAccumulator()).add(
                clicks        = safe_int(r.metrics.clicks),
                avg_cpc       = safe_int(r.metrics.average_cpc),
                ctr           = safe_float(r.metrics.ctr),
                budget_lost_is= safe_float(r.metrics.search_budget_lost_impression_share),
                rank_lost_is  = safe_float(r.metrics.search_rank_lost_impression_share),
                impressions   = safe_int(r.metrics.impressions),
            )
    return {rn: {k: acc.finalize() for k, acc in wa.items()}
            for rn, wa in accumulators.items()}


def fetch_cpc_history(client, customer_id) -> Dict[str, Dict[str, int]]:
    """
    Liefert pro Strategie einen Dict {iso_week_str: weighted_avg_cpc_micros}.
    iso_week_str z.B. "2026-W03" (Montag der Woche als Anker).

    Tage werden nach ISO-Woche gruppiert; der wöchentliche avgCPC ist
    klickgewichtet über alle Kampagnentage der Woche.
    """
    # {rn: {week_key: {"clicks": int, "cost_micros": int}}}
    raw: Dict[str, Dict[str, dict]] = {}

    try:
        for r in search(client, customer_id, gaql_daily_cpc(MEDIAN_LOOKBACK_DAYS)):
            rn      = r.bidding_strategy.resource_name
            clicks  = safe_int(r.metrics.clicks)
            avg_cpc = safe_int(r.metrics.average_cpc)
            if clicks <= 0 or avg_cpc <= 0:
                continue

            day = date.fromisoformat(str(r.segments.date))
            # ISO-Woche: Montag dieser Woche
            monday   = day - timedelta(days=day.weekday())
            week_key = monday.isoformat()   # "2026-04-06"

            raw.setdefault(rn, {}).setdefault(
                week_key, {"clicks": 0, "cost_micros": 0}
            )
            raw[rn][week_key]["clicks"]      += clicks
            raw[rn][week_key]["cost_micros"] += clicks * avg_cpc

    except Exception:
        return {}

    # Zum gewichteten Wochen-CPC zusammenfassen
    result: Dict[str, Dict[str, int]] = {}
    for rn, weeks in raw.items():
        result[rn] = {}
        for week_key, data in weeks.items():
            if data["clicks"] > 0:
                result[rn][week_key] = data["cost_micros"] // data["clicks"]

    return result


def compute_median_cpc(weekly_cpcs: Dict[str, int]) -> Tuple[Optional[int], int, bool]:
    """
    Filtert Urlaubswochen heraus, berechnet Median der restlichen Wochen.
    Gibt (median_micros, weeks_used, is_fallback) zurück.
    is_fallback=True wenn zu wenig Wochen → Aufrufer soll auf avg zurückfallen.
    """
    clean_cpcs = []
    for week_monday_str, cpc in weekly_cpcs.items():
        monday = date.fromisoformat(week_monday_str)
        if not is_holiday_week(monday):
            clean_cpcs.append(cpc)

    if len(clean_cpcs) < MEDIAN_MIN_WEEKS:
        return None, len(clean_cpcs), True   # fallback

    median = int(statistics.median(clean_cpcs))
    return median, len(clean_cpcs), False


def fetch_campaign_cap_status(client, customer_id) -> Dict[str, dict]:
    CAP_LIMITED_STATUSES = {
        "TARGET_SPEND_OPTIMIZE_BIDS_TOO_LOW",
        "TARGET_SPEND_CONSTRAINED_BY_BID_CEILING",
        "BUDGET_CONSTRAINED",
        "MISCONFIGURED_BIDDING_STRATEGY",
        "PAUSED",
        "2", "3", "4", "5", "6",
    }
    result: Dict[str, dict] = {}
    all_statuses_seen: set = set()

    try:
        for r in search(client, customer_id, gaql_campaign_system_status()):
            rn = r.campaign.bidding_strategy
            if not rn:
                continue
            raw_status  = r.campaign.bidding_strategy_system_status
            status_int  = int(raw_status) if raw_status is not None else 0
            status_name = str(raw_status).split(".")[-1].strip()
            all_statuses_seen.add(f"{status_int}:{status_name}")
            budget_lost = safe_float(r.metrics.search_budget_lost_impression_share)
            camp_name   = r.campaign.name

            result.setdefault(rn, {
                "cap_limited": 0, "budget_limited": 0,
                "total": 0, "campaigns": [], "debug_statuses": []
            })
            result[rn]["total"] += 1
            result[rn]["debug_statuses"].append(f"{camp_name}:{status_int}:{status_name}")

            is_cap    = status_name in CAP_LIMITED_STATUSES or (status_int >= 2 and status_int not in (0, 1))
            is_budget = budget_lost > 0.03

            if is_cap:   result[rn]["cap_limited"]    += 1
            if is_budget: result[rn]["budget_limited"] += 1
            if is_cap or is_budget:
                result[rn]["campaigns"].append({
                    "name":            camp_name,
                    "cap_limited":     is_cap,
                    "budget_limited":  is_budget,
                    "status":          f"{status_int}:{status_name}",
                    "budget_lost_pct": round(budget_lost * 100, 1),
                })
    except Exception as e:
        return {"_error": str(e), "_debug": list(all_statuses_seen)}

    result["_debug_all_statuses"] = list(all_statuses_seen)
    return result


def fetch_budget_recommendations(client, customer_id) -> Dict[str, dict]:
    """
    Gibt pro Strategie zurück ob eine Budget-Recommendation existiert.
    Die konkreten Mikrobeträge sind in v21 nicht mehr direkt per GAQL abrufbar.
    """
    recs = {}
    try:
        for r in search(client, customer_id, gaql_budget_recommendations()):
            rn = r.campaign.bidding_strategy
            if not rn:
                continue
            # Markieren dass eine Recommendation existiert
            recs[rn] = {
                "current_budget_micros":     0,
                "recommended_budget_micros": 0,
            }
    except Exception:
        pass
    return recs


# ============================
# CLASSIFY LOGIC
# ============================

def classify_and_compute(strategies, metrics, budget_recs, cpc_history):
    holidays_14d, affected_14d = count_holiday_days(14)
    holidays_30d, affected_30d = count_holiday_days(30)
    normalization_active = holidays_14d > 0 or holidays_30d > 0

    for rn, s in strategies.items():

        # Health metrics
        m30 = metrics.get(rn, {}).get("30d", {})
        s.budget_lost_is_30d = m30.get("budget_lost_is", 0.0)
        s.rank_lost_is_30d   = m30.get("rank_lost_is",   0.0)
        s.impressions_30d    = m30.get("impressions",     0)
        if s.impressions_30d > 0 and 0 < s.budget_lost_is_30d < 1:
            s.lost_impressions_budget = int(
                s.impressions_30d * s.budget_lost_is_30d / (1 - s.budget_lost_is_30d)
            )

        rec = budget_recs.get(rn, {})
        if rec:
            s.budget_recommendation_micros = rec.get("current_budget_micros")
            s.recommended_budget_micros    = rec.get("recommended_budget_micros")

        if s.enabled_campaigns <= 0:
            s.bucket = "SKIP"; s.reason = "no enabled campaigns"; continue
        if s.current_cap_micros is None:
            s.bucket = "SKIP"; s.reason = "no CPC cap set"; continue

        m = metrics.get(rn, {})
        s.clicks_7d  = m.get("7d",  {}).get("clicks", 0)
        s.clicks_14d = m.get("14d", {}).get("clicks", 0)
        s.clicks_30d = m.get("30d", {}).get("clicks", 0)
        s.avg_cpc_7d_micros  = m.get("7d",  {}).get("avg_cpc", 0)
        s.avg_cpc_14d_micros = m.get("14d", {}).get("avg_cpc", 0)
        s.avg_cpc_30d_micros = m.get("30d", {}).get("avg_cpc", 0)
        s.ctr_7d  = m.get("7d",  {}).get("ctr", 0.0)
        s.ctr_14d = m.get("14d", {}).get("ctr", 0.0)
        s.ctr_30d = m.get("30d", {}).get("ctr", 0.0)

        # ── Median-CPC aus 90d-Tageshistorie ──
        weekly = cpc_history.get(rn, {})
        median_micros, weeks_used, fallback = compute_median_cpc(weekly)
        s.median_weeks_used = weeks_used
        s.median_fallback   = fallback
        if fallback or median_micros is None:
            # Zu wenig Wochen → Fallback auf den niedrigeren avg
            s.median_cpc_micros = min(
                s.avg_cpc_14d_micros or s.avg_cpc_30d_micros,
                s.avg_cpc_30d_micros or s.avg_cpc_14d_micros,
            )
        else:
            s.median_cpc_micros = median_micros

        # ── Holiday Gate-Normalisierung ──
        s.clicks_14d_normalized = normalize_clicks(s.clicks_14d, 14, holidays_14d)
        s.clicks_30d_normalized = normalize_clicks(s.clicks_30d, 30, holidays_30d)
        s.holiday_normalized = normalization_active and (
            s.clicks_14d_normalized != s.clicks_14d
            or s.clicks_30d_normalized != s.clicks_30d
        )

        gate_14 = s.clicks_14d_normalized >= 15
        gate_30 = s.clicks_30d_normalized >= 30
        warn    = (s.clicks_14d_normalized == 14) or (s.clicks_30d_normalized == 29)

        def set_cap(avg_basis: int):
            """Setzt beide Cap-Varianten: avg-basiert und median-basiert."""
            s.new_cap_micros   = compute_new_cap_plus10(avg_basis)
            s.cap_delta_micros = s.new_cap_micros - (s.current_cap_micros or 0)
            if s.new_cap_micros > MAX_CAP_WARNING_MICROS:
                s.reason += f" | ⚠️ avg cap > {MAX_CAP_WARNING_EUR:.2f}€"

            if s.median_cpc_micros > 0:
                s.new_cap_median_micros   = compute_new_cap_plus10(s.median_cpc_micros)
                s.cap_delta_median_micros = s.new_cap_median_micros - (s.current_cap_micros or 0)
                if s.new_cap_median_micros > MAX_CAP_WARNING_MICROS:
                    s.reason += f" | ⚠️ median cap > {MAX_CAP_WARNING_EUR:.2f}€"

        # ── Bucket-Klassifizierung (unverändert) ──

        # NEAR30_READY
        if gate_14 and (NEAR30_MIN <= s.clicks_30d_normalized <= NEAR30_MAX) and s.avg_cpc_30d_micros > 0:
            s.bucket               = "NEAR30_READY"
            s.basis_window         = "near30(30d)"
            s.basis_avg_cpc_micros = s.avg_cpc_30d_micros
            s.reason = f"clicks30={s.clicks_30d_normalized} near 30, 30d avgCPC +10%"
            if s.holiday_normalized:
                s.reason += f" [norm: {s.clicks_30d}→{s.clicks_30d_normalized}]"
            set_cap(s.basis_avg_cpc_micros)
            apply_no_decrease_near15_rule(s)
            continue

        # READY
        if gate_14 or gate_30:
            s.bucket = "READY"
            candidates = []
            if gate_14 and s.avg_cpc_14d_micros > 0:
                candidates.append(("14d", s.avg_cpc_14d_micros))
            if gate_30 and s.avg_cpc_30d_micros > 0:
                candidates.append(("30d", s.avg_cpc_30d_micros))
            if not candidates:
                s.bucket = "SKIP"; s.reason = "no valid avg CPC"; continue
            chosen_window, chosen_cpc = sorted(candidates, key=lambda x: x[1])[0]
            s.basis_window         = "14d+30d(min)" if len(candidates) == 2 else chosen_window
            s.basis_avg_cpc_micros = chosen_cpc
            s.reason = f"basis={s.basis_window}, +10%"
            if s.holiday_normalized:
                s.reason += f" [norm: 14d {s.clicks_14d}→{s.clicks_14d_normalized}, 30d {s.clicks_30d}→{s.clicks_30d_normalized}]"
            set_cap(s.basis_avg_cpc_micros)
            apply_no_decrease_near15_rule(s)
            continue

        # WARN
        if warn:
            s.bucket = "WARN"
            s.reason = "near threshold (override possible)"
            if s.holiday_normalized:
                s.reason += f" [norm: 14d {s.clicks_14d}→{s.clicks_14d_normalized}]"
            candidates = []
            if s.clicks_14d_normalized == 14 and s.avg_cpc_14d_micros > 0:
                candidates.append(("14d", s.avg_cpc_14d_micros))
            if s.clicks_30d_normalized == 29 and s.avg_cpc_30d_micros > 0:
                candidates.append(("30d", s.avg_cpc_30d_micros))
            if candidates:
                chosen_window, chosen_cpc = sorted(candidates, key=lambda x: x[1])[0]
                s.basis_window         = chosen_window
                s.basis_avg_cpc_micros = chosen_cpc
                set_cap(s.basis_avg_cpc_micros)
                apply_no_decrease_near15_rule(s)
            continue

        # LOWVOL_READY
        if s.clicks_14d_normalized < 15 and s.clicks_30d_normalized < 30 and s.ctr_30d > s.ctr_14d:
            s.bucket = "LOWVOL_READY"
            basis_cpc = s.avg_cpc_30d_micros if s.avg_cpc_30d_micros > 0 else s.avg_cpc_14d_micros
            if not basis_cpc:
                s.bucket = "SKIP"; s.reason = "LOWVOL: no avg CPC"; continue
            s.basis_window         = "lowvol(30d)" if s.avg_cpc_30d_micros > 0 else "lowvol(fallback14d)"
            s.basis_avg_cpc_micros = basis_cpc
            s.reason = f"CTR30({s.ctr_30d*100:.1f}%) > CTR14({s.ctr_14d*100:.1f}%)"
            set_cap(s.basis_avg_cpc_micros)
            apply_no_decrease_near15_rule(s)
            continue

        # LOWVOL_DECREASE
        if s.clicks_14d_normalized < 15 and s.clicks_30d_normalized < 30:
            if s.avg_cpc_30d_micros > 0 and s.avg_cpc_14d_micros > 0:
                if s.avg_cpc_30d_micros > s.avg_cpc_14d_micros:
                    target = compute_new_cap_plus10(s.avg_cpc_30d_micros)
                    if target < (s.current_cap_micros or 0):
                        s.bucket               = "LOWVOL_DECREASE"
                        s.basis_window         = "lowvol_decrease(30d)"
                        s.basis_avg_cpc_micros = s.avg_cpc_30d_micros
                        s.reason = "cap too high vs 30d avgCPC +10%"
                        set_cap(s.basis_avg_cpc_micros)
                        apply_no_decrease_near15_rule(s)
                        continue

        s.bucket = "SKIP"
        s.reason = "insufficient clicks"
        s.recommendation = "Umkreis erweitern"

    # Score
    for s in strategies.values():
        s.click_opportunity = (
            s.clicks_30d * s.rank_lost_is_30d
            if s.clicks_30d > 0 and s.rank_lost_is_30d > 0 else 0.0
        )
        s.score = s.click_opportunity * min(1.0, s.clicks_30d / 50)


# ============================
# APPLY UPDATES
# ============================

def apply_updates(client, customer_id, strategies: List[StrategyRow],
                  cap_mode: str = "avg") -> List[str]:
    """
    cap_mode: "avg"    → new_cap_micros (avg-basiert)
              "median" → new_cap_median_micros (median-basiert, Fallback auf avg)
    """
    service = client.get_service("BiddingStrategyService")
    ops, applied = [], []

    for s in strategies:
        if cap_mode == "median" and s.new_cap_median_micros is not None:
            new_cap = s.new_cap_median_micros
        else:
            new_cap = s.new_cap_micros

        if new_cap is None or s.current_cap_micros is None:
            continue
        if new_cap == s.current_cap_micros:
            continue

        bs = client.get_type("BiddingStrategy")
        bs.resource_name = s.resource_name
        bs.target_spend.cpc_bid_ceiling_micros = new_cap
        op = client.get_type("BiddingStrategyOperation")
        op.update.resource_name = bs.resource_name
        op.update.target_spend.cpc_bid_ceiling_micros = new_cap
        client.copy_from(op.update_mask, FieldMask(paths=["target_spend.cpc_bid_ceiling_micros"]))
        ops.append(op)
        mode_label = "median" if cap_mode == "median" and not s.median_fallback else "avg"
        applied.append(
            f"{s.name}: {micros_to_str(s.current_cap_micros)}€ → {micros_to_str(new_cap)}€ [{mode_label}]"
        )

    if ops:
        service.mutate_bidding_strategies(customer_id=customer_id, operations=ops)

    return applied


# ============================
# MAIN ANALYSIS FUNCTION
# ============================

def run_analysis(customer_id: str) -> dict:
    client = get_client()

    strategies = fetch_strategies(client, customer_id)
    if not strategies:
        return {"error": "No TARGET_SPEND strategies found"}

    fetch_enabled_campaign_counts(client, customer_id, strategies)
    metrics     = fetch_metrics_aggregated(client, customer_id)
    budget_recs = fetch_budget_recommendations(client, customer_id)
    cap_status  = fetch_campaign_cap_status(client, customer_id)
    cpc_history = fetch_cpc_history(client, customer_id)

    classify_and_compute(strategies, metrics, budget_recs, cpc_history)

    for rn, s in strategies.items():
        cs = cap_status.get(rn, {})
        s.cap_limited_campaigns    = cs.get("cap_limited", 0)
        s.budget_limited_campaigns = cs.get("budget_limited", 0)

    buckets = {"NEAR30_READY": [], "READY": [], "LOWVOL_READY": [],
               "LOWVOL_DECREASE": [], "WARN": [], "SKIP": []}
    for s in strategies.values():
        buckets[s.bucket].append(asdict(s))

    actionable = sorted(
        [s for s in strategies.values()
         if s.bucket in ("NEAR30_READY", "READY", "LOWVOL_READY", "LOWVOL_DECREASE")
         and s.new_cap_micros is not None
         and s.new_cap_micros != s.current_cap_micros],
        key=lambda s: s.score, reverse=True
    )

    # Summaries für beide Modi
    def delta_sum(mode: str):
        increases = decreases = 0
        inc_count = dec_count = 0
        for s in actionable:
            d = s.cap_delta_median_micros if mode == "median" and not s.median_fallback \
                else s.cap_delta_micros
            if d > 0: increases += d; inc_count += 1
            elif d < 0: decreases += d; dec_count += 1
        return increases, decreases, inc_count, dec_count

    avg_inc, avg_dec, avg_inc_c, avg_dec_c = delta_sum("avg")
    med_inc, med_dec, med_inc_c, med_dec_c = delta_sum("median")

    # Budget Health
    def rank_pct(val): return f"{round(val * 100, 1)}%"

    def get_action(s, cs):
        rank_cap_signal = s.rank_lost_is_30d > 0.30 and s.enabled_campaigns > 0
        budget_signal   = s.budget_lost_is_30d > 0.05
        if cs.get("cap_limited", 0) == 0 and not rank_cap_signal and not budget_signal:
            return None
        cap_already_optimal = (
            s.new_cap_micros is not None and s.current_cap_micros is not None
            and s.new_cap_micros == s.current_cap_micros
        )
        cap_increase_planned = (
            s.new_cap_micros is not None and s.current_cap_micros is not None
            and s.new_cap_micros > s.current_cap_micros
        )
        cap_skip = s.bucket == "SKIP"
        parts = []
        if rank_cap_signal and budget_signal:
            if cap_already_optimal:
                parts.append(f"✅ Cap optimiert — Budget prüfen | IS Rang: {rank_pct(s.rank_lost_is_30d)}, Budget: {rank_pct(s.budget_lost_is_30d)}")
            elif cap_increase_planned:
                parts.append(f"⚠️ Cap-Erhöhung geplant — danach Budget prüfen | IS Rang: {rank_pct(s.rank_lost_is_30d)}")
            else:
                parts.append(f"🔴 Cap + Budget limitiert | IS Rang: {rank_pct(s.rank_lost_is_30d)}, Budget: {rank_pct(s.budget_lost_is_30d)}")
        elif rank_cap_signal:
            if cap_already_optimal:
                parts.append(f"✅ Cap optimiert — Google braucht ~24h | IS Rang: {rank_pct(s.rank_lost_is_30d)}")
            elif cap_increase_planned:
                parts.append(f"⚠️ Cap-Erhöhung geplant | IS Rang: {rank_pct(s.rank_lost_is_30d)}")
            elif cap_skip:
                parts.append(f"⏳ Zu wenig Klickdaten | IS Rang: {rank_pct(s.rank_lost_is_30d)}")
            else:
                parts.append(f"⚠️ Cap erhöhen | IS Rang: {rank_pct(s.rank_lost_is_30d)}")
        elif budget_signal:
            parts.append(f"💰 Budget erhöhen | IS Budget: {rank_pct(s.budget_lost_is_30d)}")
        if not parts and cs.get("cap_limited", 0) > 0:
            cap_lim = cs["cap_limited"]
            total   = cs.get("total", s.enabled_campaigns) or 1
            if cap_already_optimal:
                parts.append(f"✅ Cap optimiert ({cap_lim}/{total} Kamp.)")
            elif cap_increase_planned:
                parts.append(f"⚠️ Cap-Erhöhung geplant ({cap_lim}/{total} Kamp.)")
            elif cap_skip:
                parts.append(f"⏳ Zu wenig Klickdaten ({cap_lim}/{total} Kamp.)")
            else:
                parts.append(f"⚠️ Cap erhöhen — {cap_lim}/{total} Kamp. durch Gebotslimit")
        return " | ".join(parts) if parts else None

    budget_limited = []
    for s in strategies.values():
        if s.enabled_campaigns <= 0: continue
        cs     = cap_status.get(s.resource_name, {})
        action = get_action(s, cs)
        if not action: continue
        cap_delta_str = None
        if s.new_cap_micros and s.current_cap_micros:
            delta = (s.new_cap_micros - s.current_cap_micros) / 1_000_000
            if delta != 0:
                cap_delta_str = f"{'+' if delta > 0 else ''}{delta:.2f}€"
        budget_limited.append({
            "name":             s.name,
            "bucket":           s.bucket,
            "cap_limited":      cs.get("cap_limited", 0),
            "budget_limited":   cs.get("budget_limited", 0),
            "total_campaigns":  cs.get("total", s.enabled_campaigns),
            "cap_alt":          micros_to_str(s.current_cap_micros),
            "cap_neu":          micros_to_str(s.new_cap_micros) if s.new_cap_micros else None,
            "cap_delta":        cap_delta_str,
            "action":           action,
            "campaign_details": cs.get("campaigns", []),
        })
    strategy_scores = {s.name: s.score for s in strategies.values()}
    for item in budget_limited:
        item["score"] = strategy_scores.get(item["name"], 0.0)
    budget_limited.sort(key=lambda x: x["score"], reverse=True)

    skips = [s for s in strategies.values() if s.bucket == "SKIP"]
    holidays_14d, affected_14d = count_holiday_days(14)
    holidays_30d, affected_30d = count_holiday_days(30)

    # Median-Qualität
    fallback_count = sum(1 for s in actionable if s.median_fallback)
    median_strategies_count = sum(1 for s in actionable if not s.median_fallback)

    return {
        "customer_id": customer_id,
        "buckets":     buckets,
        "debug_cap_statuses": list(cap_status.get("_debug_all_statuses", [])),
        "summary": {
            "total_actionable":    len(actionable),
            # Avg-Modus
            "total_increases_eur": round(avg_inc / 1_000_000, 2),
            "total_decreases_eur": round(avg_dec / 1_000_000, 2),
            "net_delta_eur":       round((avg_inc + avg_dec) / 1_000_000, 2),
            "increases_count":     avg_inc_c,
            "decreases_count":     avg_dec_c,
            "weighted_delta_eur":  round(
                sum(s.cap_delta_micros * s.enabled_campaigns
                    for s in actionable if s.cap_delta_micros > 0) / 1_000_000, 2
            ),
            # Median-Modus
            "median_increases_eur": round(med_inc / 1_000_000, 2),
            "median_decreases_eur": round(med_dec / 1_000_000, 2),
            "median_net_delta_eur": round((med_inc + med_dec) / 1_000_000, 2),
            "median_increases_count": med_inc_c,
            "median_decreases_count": med_dec_c,
            "skip_total":           len(skips),
            "skip_no_campaigns":    sum(1 for s in skips if s.enabled_campaigns <= 0),
            "skip_no_data":         sum(1 for s in skips if s.enabled_campaigns > 0 and s.clicks_30d == 0),
            "skip_low_data":        sum(1 for s in skips if s.enabled_campaigns > 0 and 0 < s.clicks_30d < 30),
        },
        "budget_limited": budget_limited,
        "holiday_info": {
            "holidays_in_14d":             holidays_14d,
            "holidays_in_30d":             holidays_30d,
            "affected_periods":            sorted(set(affected_14d + affected_30d)),
            "normalization_active":        holidays_14d > 0 or holidays_30d > 0,
            "normalized_strategies_count": sum(1 for s in strategies.values() if s.holiday_normalized),
        },
        "median_info": {
            "lookback_days":         MEDIAN_LOOKBACK_DAYS,
            "min_weeks_required":    MEDIAN_MIN_WEEKS,
            "strategies_with_median": median_strategies_count,
            "strategies_fallback":   fallback_count,
        },
    }


# ============================
# API ROUTES
# ============================

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates", "index.html")
    with open(path) as f:
        return f.read()


@app.get("/api/analyze")
async def analyze(customer_id: str = ""):
    cid = customer_id or CUSTOMER_ID
    if not cid:
        return JSONResponse({"error": "No customer_id provided"}, status_code=400)
    try:
        return JSONResponse(run_analysis(cid.replace("-", "")))
    except GoogleAdsException as ex:
        errors = [{"code": str(e.error_code), "message": e.message} for e in ex.failure.errors]
        return JSONResponse({"error": "Google Ads API error", "details": errors}, status_code=500)
    except Exception as ex:
        return JSONResponse({"error": str(ex)}, status_code=500)


@app.post("/api/apply")
async def apply(payload: dict):
    customer_id      = payload.get("customer_id", CUSTOMER_ID).replace("-", "")
    buckets_to_apply = payload.get("buckets", ["NEAR30_READY", "READY", "LOWVOL_READY", "LOWVOL_DECREASE"])
    include_warn     = payload.get("include_warn", False)
    excluded         = set(payload.get("excluded_resource_names", []))
    cap_mode         = payload.get("cap_mode", "avg")   # "avg" | "median"

    try:
        client      = get_client()
        strategies  = fetch_strategies(client, customer_id)
        fetch_enabled_campaign_counts(client, customer_id, strategies)
        metrics     = fetch_metrics_aggregated(client, customer_id)
        budget_recs = fetch_budget_recommendations(client, customer_id)
        cpc_history = fetch_cpc_history(client, customer_id)
        classify_and_compute(strategies, metrics, budget_recs, cpc_history)

        to_apply = [
            s for s in strategies.values()
            if s.bucket in buckets_to_apply and s.resource_name not in excluded
        ]
        if include_warn:
            to_apply += [
                s for s in strategies.values()
                if s.bucket == "WARN" and s.new_cap_micros is not None
                and s.resource_name not in excluded
            ]

        applied = apply_updates(client, customer_id, to_apply, cap_mode=cap_mode)
        return JSONResponse({"applied": applied, "count": len(applied), "cap_mode": cap_mode})

    except GoogleAdsException as ex:
        errors = [{"code": str(e.error_code), "message": e.message} for e in ex.failure.errors]
        return JSONResponse({"error": "Google Ads API error", "details": errors}, status_code=500)
    except Exception as ex:
        return JSONResponse({"error": str(ex)}, status_code=500)


# ============================
# HOLIDAY API
# ============================

@app.get("/api/holidays")
async def get_holidays():
    try:
        with open(HOLIDAYS_PATH) as f:
            return JSONResponse(json.load(f))
    except FileNotFoundError:
        return JSONResponse({"year": date.today().year, "periods": []})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/holidays")
async def save_holidays(payload: dict):
    global HOLIDAY_PERIODS
    try:
        periods = payload.get("periods", [])
        for p in periods:
            if not p.get("name", "").strip():
                raise ValueError("Name fehlt in einer Periode")
            s = date.fromisoformat(p["start"])
            e = date.fromisoformat(p["end"])
            if e < s:
                raise ValueError(f'"{p["name"]}": End-Datum liegt vor Start-Datum')
        with open(HOLIDAYS_PATH, "w", encoding="utf-8") as f:
            json.dump({"year": payload.get("year", date.today().year), "periods": periods},
                      f, indent=2, ensure_ascii=False)
        HOLIDAY_PERIODS = load_holiday_periods()
        return JSONResponse({"ok": True, "count": len(periods)})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)


# ============================
# SNAPSHOT API
# ============================

SNAPSHOTS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "snapshots.json")


def load_snapshots() -> list:
    try:
        with open(SNAPSHOTS_PATH) as f:
            return json.load(f)
    except FileNotFoundError:
        return []
    except Exception:
        return []


@app.post("/api/snapshot")
async def save_snapshot(payload: dict):
    customer_id = payload.get("customer_id", CUSTOMER_ID).replace("-", "")
    cap_mode    = payload.get("cap_mode", "avg")
    label       = payload.get("label", "").strip()  # z.B. "vor Apply", "nach Apply"

    try:
        client      = get_client()
        strategies  = fetch_strategies(client, customer_id)
        fetch_enabled_campaign_counts(client, customer_id, strategies)
        metrics     = fetch_metrics_aggregated(client, customer_id)
        budget_recs = fetch_budget_recommendations(client, customer_id)
        cpc_history = fetch_cpc_history(client, customer_id)
        classify_and_compute(strategies, metrics, budget_recs, cpc_history)

        snapshot = {
            "ts":          date.today().isoformat(),
            "cap_mode":    cap_mode,
            "label":       label or date.today().isoformat(),
            "data": [
                {
                    "name":           s.name,
                    "resource_name":  s.resource_name,
                    "bucket":         s.bucket,
                    "rank_lost_is":   round(s.rank_lost_is_30d,   4),
                    "budget_lost_is": round(s.budget_lost_is_30d, 4),
                    "current_cap":    s.current_cap_micros,
                    "avg_cpc_30d":    s.avg_cpc_30d_micros,
                    "median_cpc":     s.median_cpc_micros,
                    "clicks_30d":     s.clicks_30d,
                    "score":          round(s.score, 2),
                }
                for s in strategies.values()
                if s.enabled_campaigns > 0
            ]
        }

        existing = load_snapshots()
        existing.append(snapshot)
        existing = existing[-52:]  # max 1 Jahr wöchentlich

        with open(SNAPSHOTS_PATH, "w", encoding="utf-8") as f:
            json.dump(existing, f, indent=2, ensure_ascii=False)

        return JSONResponse({
            "ok":    True,
            "ts":    snapshot["ts"],
            "label": snapshot["label"],
            "count": len(snapshot["data"]),
        })

    except GoogleAdsException as ex:
        errors = [{"code": str(e.error_code), "message": e.message} for e in ex.failure.errors]
        return JSONResponse({"error": "Google Ads API error", "details": errors}, status_code=500)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/snapshots")
async def get_snapshots():
    return JSONResponse(load_snapshots())


@app.delete("/api/snapshots/{index}")
async def delete_snapshot(index: int):
    try:
        existing = load_snapshots()
        if index < 0 or index >= len(existing):
            return JSONResponse({"error": "Index out of range"}, status_code=400)
        removed = existing.pop(index)
        with open(SNAPSHOTS_PATH, "w", encoding="utf-8") as f:
            json.dump(existing, f, indent=2, ensure_ascii=False)
        return JSONResponse({"ok": True, "removed": removed["label"]})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port)
