"""
Cost modeling for product launch lead acquisition and conversion.
Computes CAC, CPL, ROAS, and LTV across channels and conversion stages.
"""
import warnings
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")


@dataclass_needs_dataclasses = True
try:
    from dataclasses import dataclass, field
except ImportError:
    dataclass_needs_dataclasses = False


from dataclasses import dataclass, field


@dataclass
class ChannelMetrics:
    channel: str
    spend: float             # total spend in currency units
    impressions: int
    clicks: int
    leads: int
    mqls: int
    sqls: int
    opportunities: int
    won: int
    avg_deal_value: float


class LeadCostModel:
    """
    Computes cost efficiency metrics across acquisition channels.
    Metrics: CPL, CPMQL, CPSQL, CAC, ROAS, LTV, payback period.
    """

    def __init__(self, avg_ltv: float = 5000.0, gross_margin: float = 0.70,
                 churn_rate_monthly: float = 0.03):
        self.avg_ltv = avg_ltv
        self.gross_margin = gross_margin
        self.churn_rate_monthly = churn_rate_monthly

    def compute_channel_metrics(self, channel: ChannelMetrics) -> Dict:
        """Compute all cost metrics for a single channel."""
        def safe_div(a, b):
            return round(a / b, 2) if b > 0 else None

        cpl = safe_div(channel.spend, channel.leads)
        cpmql = safe_div(channel.spend, channel.mqls)
        cpsql = safe_div(channel.spend, channel.sqls)
        cac = safe_div(channel.spend, channel.won)
        revenue = channel.won * channel.avg_deal_value
        roas = safe_div(revenue, channel.spend)
        ctr = safe_div(channel.clicks, channel.impressions) * 100 if channel.impressions > 0 else 0
        lead_rate = safe_div(channel.leads, channel.clicks) * 100 if channel.clicks > 0 else 0
        mql_rate = safe_div(channel.mqls, channel.leads) * 100 if channel.leads > 0 else 0
        sql_rate = safe_div(channel.sqls, channel.mqls) * 100 if channel.mqls > 0 else 0
        win_rate = safe_div(channel.won, channel.opportunities) * 100 if channel.opportunities > 0 else 0
        ltv_cac = safe_div(self.avg_ltv, cac) if cac and cac > 0 else None
        payback_months = safe_div(cac, channel.avg_deal_value * self.gross_margin / 12) if cac else None

        return {
            "channel": channel.channel,
            "spend": channel.spend,
            "leads": channel.leads,
            "won": channel.won,
            "cpl": cpl,
            "cpmql": cpmql,
            "cpsql": cpsql,
            "cac": cac,
            "roas": roas,
            "revenue": round(revenue, 0),
            "ctr_pct": round(ctr, 2) if ctr else 0.0,
            "lead_rate_pct": round(lead_rate, 2) if lead_rate else 0.0,
            "mql_rate_pct": round(mql_rate, 2) if mql_rate else 0.0,
            "sql_rate_pct": round(sql_rate, 2) if sql_rate else 0.0,
            "win_rate_pct": round(win_rate, 2) if win_rate else 0.0,
            "ltv_cac_ratio": ltv_cac,
            "payback_months": payback_months,
        }

    def compare_channels(self, channels: List[ChannelMetrics]) -> pd.DataFrame:
        """Return a ranked comparison DataFrame for all channels."""
        records = [self.compute_channel_metrics(ch) for ch in channels]
        df = pd.DataFrame(records)
        df["efficiency_score"] = (
            df["roas"].fillna(0) * 0.40
            + df["ltv_cac_ratio"].fillna(0) * 0.30
            + (1 / df["cac"].replace(0, np.nan).fillna(999)).fillna(0) * 0.30
        )
        df["efficiency_score"] = df["efficiency_score"].round(4)
        return df.sort_values("efficiency_score", ascending=False).reset_index(drop=True)

    def blended_cac(self, channels: List[ChannelMetrics]) -> float:
        """Total spend divided by total customers won across all channels."""
        total_spend = sum(ch.spend for ch in channels)
        total_won = sum(ch.won for ch in channels)
        return round(total_spend / total_won, 2) if total_won > 0 else 0.0

    def ltv_estimate(self, avg_monthly_revenue: float) -> Dict:
        """Compute LTV using a simple retention-based model."""
        monthly_margin = avg_monthly_revenue * self.gross_margin
        avg_lifetime_months = 1 / self.churn_rate_monthly if self.churn_rate_monthly > 0 else 0
        ltv = monthly_margin * avg_lifetime_months
        return {
            "avg_lifetime_months": round(avg_lifetime_months, 1),
            "monthly_gross_margin": round(monthly_margin, 2),
            "estimated_ltv": round(ltv, 2),
        }

    def budget_allocation_recommendation(self, channels: List[ChannelMetrics],
                                          total_budget: float) -> pd.DataFrame:
        """
        Recommend budget allocation proportional to channel efficiency score.
        """
        df = self.compare_channels(channels)
        total_score = df["efficiency_score"].sum()
        df["recommended_budget"] = (
            (df["efficiency_score"] / total_score * total_budget).round(2)
        )
        df["budget_pct"] = (df["recommended_budget"] / total_budget * 100).round(1)
        return df[["channel", "efficiency_score", "recommended_budget", "budget_pct"]]


if __name__ == "__main__":
    np.random.seed(42)
    channels = [
        ChannelMetrics("Paid Search", 50000, 500000, 25000, 1200, 480, 240, 120, 60, 8000),
        ChannelMetrics("Organic SEO", 10000, 0, 18000, 900, 450, 180, 90, 54, 9000),
        ChannelMetrics("LinkedIn Ads", 30000, 200000, 6000, 600, 300, 180, 80, 40, 12000),
        ChannelMetrics("Referral", 5000, 0, 5000, 400, 280, 180, 100, 75, 11000),
        ChannelMetrics("Cold Outreach", 8000, 0, 20000, 300, 90, 45, 20, 8, 10000),
    ]

    model = LeadCostModel(avg_ltv=50000, gross_margin=0.72, churn_rate_monthly=0.025)
    comparison = model.compare_channels(channels)
    print("Channel comparison:")
    cols = ["channel", "spend", "won", "cac", "roas", "ltv_cac_ratio", "efficiency_score"]
    print(comparison[cols].to_string(index=False))

    print(f"\nBlended CAC: ${model.blended_cac(channels):,.2f}")
    ltv_est = model.ltv_estimate(avg_monthly_revenue=2000)
    print("LTV estimate:", ltv_est)

    budget_rec = model.budget_allocation_recommendation(channels, total_budget=100000)
    print("\nBudget allocation recommendation:")
    print(budget_rec.to_string(index=False))
