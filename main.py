#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Google Ads Bidding Health Dashboard
FastAPI Web-App für Railway Deployment
"""

import os
        
import json
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Tuple

from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import uvicorn

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google.protobuf.field_mask_pb2 import FieldMask

app = FastAPI(title="Google Ads Bidding Health Dashboard")

# ============================
# CONFIG
# ============================

CUSTOMER_ID = os.environ.get("GOOGLE_ADS_CUSTOMER_ID", "")

MIN_CAP_MICROS = 10_000
NEAR30_MIN = 24
NEAR30_MAX = 29
NEAR15_NODECREASE_MIN = 12
NEAR15_NODECREASE_MAX = 14
MAX_CAP_WARNING_EUR = 6.00
MAX_CAP_WARNING_MICROS = int(MAX_CAP_WARNING_EUR * 1_000_000)

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

    clicks_7d: int = 0
    clicks_14d: int = 0
    clicks_30d: int = 0

    avg_cpc_7d_micros: int = 0
    avg_cpc_14d_micros: int = 0
    avg_cpc_30d_micros: int = 0

    ctr_7d: float = 0.0
    ctr_14d: float = 0.0
    ctr_30d: float = 0.0

    # Health metrics
    budget_lost_is_30d: float = 0.0       # Search IS lost due to budget
    rank_lost_is_30d: float = 0.0         # Search IS lost due to rank
    impressions_30d: int = 0
    lost_impressions_budget: int = 0      # estimated lost impressions due to budget

    # Budget recommendation
    budget_recommendation_micros: Optional[int] = None
    recommended_budget_micros: Optional[int] = None

    basis_window: str = ""
    basis_avg_cpc_micros: int = 0
    new_cap_micros: Optional[int] = None
    cap_delta_micros: int = 0

    bucket: str = "SKIP"
    reason: str = ""
    recommendation: str = ""


@dataclass
class MetricsAccumulator:
    clicks: int = 0
    cpc_values: List[int] = field(default_factory=list)
    ctr_sum: float = 0.0
    campaign_count: int = 0
    budget_lost_is_sum: float = 0.0
    rank_lost_is_sum: float = 0.0
    impressions: int = 0

    def add(self, clicks: int, avg_cpc: int, ctr: float,
            budget_lost_is: float = 0.0, rank_lost_is: float = 0.0,
            impressions: int = 0) -> None:
        self.clicks += clicks
        if avg_cpc > 0:
            self.cpc_values.append(avg_cpc)
        self.ctr_sum += ctr
        self.campaign_count += 1
        self.budget_lost_is_sum += budget_lost_is
        self.rank_lost_is_sum += rank_lost_is
        self.impressions += impressions

    def finalize(self) -> dict:
        avg_cpc = int(sum(self.cpc_values) / len(self.cpc_values)) if self.cpc_values else 0
        avg_ctr = self.ctr_sum / self.campaign_count if self.campaign_count > 0 else 0.0
        avg_budget_lost = self.budget_lost_is_sum / self.campaign_count if self.campaign_count > 0 else 0.0
        avg_rank_lost = self.rank_lost_is_sum / self.campaign_count if self.campaign_count > 0 else 0.0
        return {
            "clicks": self.clicks,
            "avg_cpc": avg_cpc,
            "ctr": avg_ctr,
            "budget_lost_is": avg_budget_lost,
            "rank_lost_is": avg_rank_lost,
            "impressions": self.impressions,
        }


# ============================
# GOOGLE ADS CLIENT
# ============================

def get_client() -> GoogleAdsClient:
    """Baut GoogleAdsClient aus Environment Variables."""
    config = {
        "developer_token": os.environ["GOOGLE_ADS_DEVELOPER_TOKEN"],
        "client_id": os.environ["GOOGLE_ADS_CLIENT_ID"],
        "client_secret": os.environ["GOOGLE_ADS_CLIENT_SECRET"],
        "refresh_token": os.environ["GOOGLE_ADS_REFRESH_TOKEN"],
        "login_customer_id": os.environ.get("GOOGLE_ADS_LOGIN_CUSTOMER_ID", ""),
        "use_proto_plus": True,
    }
    return GoogleAdsClient.load_from_dict(config, version="v21")


def search(client: GoogleAdsClient, customer_id: str, query: str):
    ga_service = client.get_service("GoogleAdsService")
    return ga_service.search(customer_id=customer_id, query=query)


# ============================
# HELPERS
# ============================

def micros_to_eur(micros: Optional[int]) -> float:
    if micros is None:
        return 0.0
    return micros / 1_000_000

def micros_to_str(micros: Optional[int]) -> str:
    if micros is None:
        return "none"
    return f"{micros/1_000_000:.2f}"

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
    near15 = NEAR15_NODECREASE_MIN <= s.clicks_14d <= NEAR15_NODECREASE_MAX
    if near15 and s.new_cap_micros < s.current_cap_micros:
        s.bucket = "SKIP"
        s.reason = f"NO_DECREASE_NEAR15: clicks14={s.clicks_14d}, would reduce cap"
        s.new_cap_micros = None
        s.cap_delta_micros = 0
        s.basis_window = ""
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

def gaql_enabled_campaign_count(strategy_rn: str) -> str:
    return f"""
    SELECT campaign.id
    FROM campaign
    WHERE
      campaign.status = ENABLED
      AND campaign.bidding_strategy = '{strategy_rn}'
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

def gaql_budget_recommendations() -> str:
    return """
    SELECT
      recommendation.resource_name,
      recommendation.type,
      recommendation.campaign_budget_recommendation.current_budget_amount_micros,
      recommendation.campaign_budget_recommendation.recommended_budget_amount_micros,
      recommendation.campaign_budget_recommendation.budget_options,
      campaign.bidding_strategy
    FROM recommendation
    WHERE recommendation.type = CAMPAIGN_BUDGET
    """


# ============================
# FETCHING
# ============================

def fetch_strategies(client, customer_id) -> Dict[str, StrategyRow]:
    rows = {}
    for r in search(client, customer_id, gaql_strategies()):
        bs = r.bidding_strategy
        cap = bs.target_spend.cpc_bid_ceiling_micros
        rows[bs.resource_name] = StrategyRow(
            resource_name=bs.resource_name,
            strategy_id=bs.id,
            name=bs.name,
            status=str(bs.status).replace("BiddingStrategyStatus.", ""),
            current_cap_micros=int(cap) if cap else None,
            enabled_campaigns=0,
        )
    return rows


def fetch_enabled_campaign_counts(client, customer_id, strategies):
    for s in strategies.values():
        count = sum(1 for _ in search(client, customer_id, gaql_enabled_campaign_count(s.resource_name)))
        s.enabled_campaigns = count


def fetch_metrics_aggregated(client, customer_id) -> Dict[str, Dict[str, dict]]:
    accumulators: Dict[str, Dict[str, MetricsAccumulator]] = {}
    windows = {"7d": "LAST_7_DAYS", "14d": "LAST_14_DAYS", "30d": "LAST_30_DAYS"}

    for key, during in windows.items():
        for r in search(client, customer_id, gaql_metrics(during)):
            rn = r.bidding_strategy.resource_name
            if rn not in accumulators:
                accumulators[rn] = {}
            if key not in accumulators[rn]:
                accumulators[rn][key] = MetricsAccumulator()

            accumulators[rn][key].add(
                clicks=safe_int(r.metrics.clicks),
                avg_cpc=safe_int(r.metrics.average_cpc),
                ctr=safe_float(r.metrics.ctr),
                budget_lost_is=safe_float(r.metrics.search_budget_lost_impression_share),
                rank_lost_is=safe_float(r.metrics.search_rank_lost_impression_share),
                impressions=safe_int(r.metrics.impressions),
            )

    return {rn: {k: acc.finalize() for k, acc in windows_acc.items()}
            for rn, windows_acc in accumulators.items()}


def fetch_budget_recommendations(client, customer_id) -> Dict[str, dict]:
    """Gibt budget recommendations pro bidding_strategy resource_name zurück."""
    recs = {}
    try:
        for r in search(client, customer_id, gaql_budget_recommendations()):
            strategy_rn = r.campaign.bidding_strategy
            if not strategy_rn:
                continue
            rec = r.recommendation.campaign_budget_recommendation
            recs[strategy_rn] = {
                "current_budget_micros": safe_int(rec.current_budget_amount_micros),
                "recommended_budget_micros": safe_int(rec.recommended_budget_amount_micros),
            }
    except Exception:
        pass  # Recommendations API kann leer sein
    return recs


# ============================
# CLASSIFY LOGIC
# ============================

def classify_and_compute(strategies, metrics, budget_recs):
    for rn, s in strategies.items():

        # Health metrics aus 30d
        m30 = metrics.get(rn, {}).get("30d", {})
        s.budget_lost_is_30d = m30.get("budget_lost_is", 0.0)
        s.rank_lost_is_30d = m30.get("rank_lost_is", 0.0)
        s.impressions_30d = m30.get("impressions", 0)
        if s.impressions_30d > 0 and s.budget_lost_is_30d > 0:
            s.lost_impressions_budget = int(
                s.impressions_30d * s.budget_lost_is_30d / (1 - s.budget_lost_is_30d)
                if s.budget_lost_is_30d < 1 else s.impressions_30d
            )

        # Budget recommendation
        rec = budget_recs.get(rn, {})
        if rec:
            s.budget_recommendation_micros = rec.get("current_budget_micros")
            s.recommended_budget_micros = rec.get("recommended_budget_micros")

        if s.enabled_campaigns <= 0:
            s.bucket = "SKIP"
            s.reason = "no enabled campaigns"
            continue

        if s.current_cap_micros is None:
            s.bucket = "SKIP"
            s.reason = "no CPC cap set"
            continue

        m = metrics.get(rn, {})
        s.clicks_7d = m.get("7d", {}).get("clicks", 0)
        s.clicks_14d = m.get("14d", {}).get("clicks", 0)
        s.clicks_30d = m.get("30d", {}).get("clicks", 0)
        s.avg_cpc_7d_micros = m.get("7d", {}).get("avg_cpc", 0)
        s.avg_cpc_14d_micros = m.get("14d", {}).get("avg_cpc", 0)
        s.avg_cpc_30d_micros = m.get("30d", {}).get("avg_cpc", 0)
        s.ctr_7d = m.get("7d", {}).get("ctr", 0.0)
        s.ctr_14d = m.get("14d", {}).get("ctr", 0.0)
        s.ctr_30d = m.get("30d", {}).get("ctr", 0.0)

        gate_14 = s.clicks_14d >= 15
        gate_30 = s.clicks_30d >= 30
        warn = (s.clicks_14d == 14) or (s.clicks_30d == 29)

        def set_cap(new_cap):
            s.new_cap_micros = new_cap
            s.cap_delta_micros = new_cap - (s.current_cap_micros or 0)
            if new_cap > MAX_CAP_WARNING_MICROS:
                s.reason += f" | ⚠️ new cap > {MAX_CAP_WARNING_EUR:.2f}€"

        # NEAR30_READY
        if gate_14 and (NEAR30_MIN <= s.clicks_30d <= NEAR30_MAX) and s.avg_cpc_30d_micros > 0:
            s.bucket = "NEAR30_READY"
            s.basis_window = "near30(30d)"
            s.basis_avg_cpc_micros = s.avg_cpc_30d_micros
            s.reason = f"clicks30={s.clicks_30d} near 30, 30d avgCPC +10%"
            set_cap(compute_new_cap_plus10(s.basis_avg_cpc_micros))
            if not apply_no_decrease_near15_rule(s):
                pass
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
                s.bucket = "SKIP"
                s.reason = "no valid avg CPC"
                continue
            chosen_window, chosen_cpc = sorted(candidates, key=lambda x: x[1])[0]
            s.basis_window = "14d+30d(min)" if len(candidates) == 2 else chosen_window
            s.basis_avg_cpc_micros = chosen_cpc
            s.reason = f"basis={s.basis_window}, +10%"
            set_cap(compute_new_cap_plus10(s.basis_avg_cpc_micros))
            apply_no_decrease_near15_rule(s)
            continue

        # WARN
        if warn:
            s.bucket = "WARN"
            s.reason = "near threshold (override possible)"
            candidates = []
            if s.clicks_14d == 14 and s.avg_cpc_14d_micros > 0:
                candidates.append(("14d", s.avg_cpc_14d_micros))
            if s.clicks_30d == 29 and s.avg_cpc_30d_micros > 0:
                candidates.append(("30d", s.avg_cpc_30d_micros))
            if candidates:
                chosen_window, chosen_cpc = sorted(candidates, key=lambda x: x[1])[0]
                s.basis_window = chosen_window
                s.basis_avg_cpc_micros = chosen_cpc
                set_cap(compute_new_cap_plus10(s.basis_avg_cpc_micros))
                apply_no_decrease_near15_rule(s)
            continue

        # LOWVOL_READY
        if s.clicks_14d < 15 and s.clicks_30d < 30 and s.ctr_30d > s.ctr_14d:
            s.bucket = "LOWVOL_READY"
            basis_cpc = s.avg_cpc_30d_micros if s.avg_cpc_30d_micros > 0 else s.avg_cpc_14d_micros
            if not basis_cpc:
                s.bucket = "SKIP"
                s.reason = "LOWVOL: no avg CPC"
                continue
            s.basis_window = "lowvol(30d)" if s.avg_cpc_30d_micros > 0 else "lowvol(fallback14d)"
            s.basis_avg_cpc_micros = basis_cpc
            s.reason = f"CTR30({s.ctr_30d*100:.1f}%) > CTR14({s.ctr_14d*100:.1f}%)"
            set_cap(compute_new_cap_plus10(s.basis_avg_cpc_micros))
            apply_no_decrease_near15_rule(s)
            continue

        # LOWVOL_DECREASE
        if s.clicks_14d < 15 and s.clicks_30d < 30:
            if s.avg_cpc_30d_micros > 0 and s.avg_cpc_14d_micros > 0:
                if s.avg_cpc_30d_micros > s.avg_cpc_14d_micros:
                    target = compute_new_cap_plus10(s.avg_cpc_30d_micros)
                    if target < (s.current_cap_micros or 0):
                        s.bucket = "LOWVOL_DECREASE"
                        s.basis_window = "lowvol_decrease(30d)"
                        s.basis_avg_cpc_micros = s.avg_cpc_30d_micros
                        s.reason = "cap too high vs 30d avgCPC +10%"
                        set_cap(target)
                        apply_no_decrease_near15_rule(s)
                        continue

        s.bucket = "SKIP"
        s.reason = "insufficient clicks"
        s.recommendation = "Umkreis erweitern"


# ============================
# APPLY
# ============================

def apply_updates(client, customer_id, strategies: List[StrategyRow]) -> List[str]:
    service = client.get_service("BiddingStrategyService")
    ops = []
    applied = []

    for s in strategies:
        if s.new_cap_micros is None or s.current_cap_micros is None:
            continue
        if s.new_cap_micros == s.current_cap_micros:
            continue
        bs = client.get_type("BiddingStrategy")
        bs.resource_name = s.resource_name
        bs.target_spend.cpc_bid_ceiling_micros = s.new_cap_micros
        op = client.get_type("BiddingStrategyOperation")
        op.update.CopyFrom(bs)
        op.update_mask.CopyFrom(FieldMask(paths=["target_spend.cpc_bid_ceiling_micros"]))
        ops.append(op)
        applied.append(f"{s.name}: {micros_to_str(s.current_cap_micros)}€ → {micros_to_str(s.new_cap_micros)}€")

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
    metrics = fetch_metrics_aggregated(client, customer_id)
    budget_recs = fetch_budget_recommendations(client, customer_id)
    classify_and_compute(strategies, metrics, budget_recs)

    # Buckets aufteilen
    buckets = {"NEAR30_READY": [], "READY": [], "LOWVOL_READY": [],
               "LOWVOL_DECREASE": [], "WARN": [], "SKIP": []}
    for s in strategies.values():
        buckets[s.bucket].append(asdict(s))

    # Delta-Zusammenfassung
    actionable = [s for s in strategies.values()
                  if s.bucket in ("NEAR30_READY", "READY", "LOWVOL_READY", "LOWVOL_DECREASE")
                  and s.new_cap_micros is not None
                  and s.new_cap_micros != s.current_cap_micros]

    total_increases = sum(s.cap_delta_micros for s in actionable if s.cap_delta_micros > 0)
    total_decreases = sum(s.cap_delta_micros for s in actionable if s.cap_delta_micros < 0)
    net_delta = total_increases + total_decreases

    # Health summary: budget-limitierte Strategien
    budget_limited = [
        {"name": s.name, "budget_lost_is": round(s.budget_lost_is_30d * 100, 1),
         "lost_impressions": s.lost_impressions_budget,
         "recommended_budget": micros_to_str(s.recommended_budget_micros) if s.recommended_budget_micros else None}
        for s in strategies.values()
        if s.budget_lost_is_30d > 0.05 and s.enabled_campaigns > 0
    ]
    budget_limited.sort(key=lambda x: x["budget_lost_is"], reverse=True)

    return {
        "customer_id": customer_id,
        "buckets": buckets,
        "summary": {
            "total_actionable": len(actionable),
            "total_increases_eur": round(total_increases / 1_000_000, 2),
            "total_decreases_eur": round(total_decreases / 1_000_000, 2),
            "net_delta_eur": round(net_delta / 1_000_000, 2),
            "increases_count": sum(1 for s in actionable if s.cap_delta_micros > 0),
            "decreases_count": sum(1 for s in actionable if s.cap_delta_micros < 0),
        },
        "budget_limited": budget_limited,
    }


# ============================
# API ROUTES
# ============================

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    template_path = os.path.join(os.path.dirname(__file__), "templates", "index.html")
    with open(template_path) as f:
        return f.read()


@app.get("/api/analyze")
async def analyze(customer_id: str = ""):
    cid = customer_id or CUSTOMER_ID
    if not cid:
        return JSONResponse({"error": "No customer_id provided"}, status_code=400)
    try:
        result = run_analysis(cid.replace("-", ""))
        return JSONResponse(result)
    except GoogleAdsException as ex:
        errors = [{"code": str(e.error_code), "message": e.message}
                  for e in ex.failure.errors]
        return JSONResponse({"error": "Google Ads API error", "details": errors}, status_code=500)
    except Exception as ex:
        return JSONResponse({"error": str(ex)}, status_code=500)


@app.post("/api/apply")
async def apply(payload: dict):
    customer_id = payload.get("customer_id", CUSTOMER_ID).replace("-", "")
    buckets_to_apply = payload.get("buckets", ["NEAR30_READY", "READY", "LOWVOL_READY", "LOWVOL_DECREASE"])
    include_warn = payload.get("include_warn", False)

    try:
        client = get_client()
        strategies = fetch_strategies(client, customer_id)
        fetch_enabled_campaign_counts(client, customer_id, strategies)
        metrics = fetch_metrics_aggregated(client, customer_id)
        budget_recs = fetch_budget_recommendations(client, customer_id)
        classify_and_compute(strategies, metrics, budget_recs)

        to_apply = [s for s in strategies.values() if s.bucket in buckets_to_apply]
        if include_warn:
            to_apply += [s for s in strategies.values()
                         if s.bucket == "WARN" and s.new_cap_micros is not None]

        applied = apply_updates(client, customer_id, to_apply)
        return JSONResponse({"applied": applied, "count": len(applied)})

    except GoogleAdsException as ex:
        errors = [{"code": str(e.error_code), "message": e.message}
                  for e in ex.failure.errors]
        return JSONResponse({"error": "Google Ads API error", "details": errors}, status_code=500)
    except Exception as ex:
        return JSONResponse({"error": str(ex)}, status_code=500)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port)
