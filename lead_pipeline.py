"""
Lead management pipeline for product launch campaigns.
Ingests raw leads, scores them, segments by stage, and routes to sales queues.
"""
import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class Lead:
    lead_id: str
    name: str
    email: str
    company: str
    job_title: str
    source: str                  # organic, paid, referral, event, cold_outreach
    industry: str
    company_size: str            # 1-10, 11-50, 51-200, 201-1000, 1000+
    country: str
    created_at: float = field(default_factory=time.time)
    stage: str = "new"           # new, mql, sql, opportunity, closed_won, closed_lost
    score: float = 0.0
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ScoringWeights:
    source_weights: Dict[str, float] = field(default_factory=lambda: {
        "referral": 30, "event": 25, "organic": 15, "paid": 10, "cold_outreach": 5
    })
    company_size_weights: Dict[str, float] = field(default_factory=lambda: {
        "1000+": 30, "201-1000": 25, "51-200": 18, "11-50": 10, "1-10": 5
    })
    title_keywords: Dict[str, float] = field(default_factory=lambda: {
        "ceo": 20, "cto": 20, "cfo": 20, "vp": 15, "director": 12,
        "head of": 10, "manager": 7, "engineer": 5,
    })
    industry_weights: Dict[str, float] = field(default_factory=lambda: {
        "technology": 20, "finance": 18, "healthcare": 15,
        "logistics": 15, "retail": 10, "education": 8, "other": 5
    })


class LeadScorer:
    """Assigns a numeric score (0-100) to each lead based on configurable weights."""

    def __init__(self, weights: Optional[ScoringWeights] = None):
        self.weights = weights or ScoringWeights()

    def _title_score(self, job_title: str) -> float:
        title_lower = job_title.lower()
        for keyword, score in self.weights.title_keywords.items():
            if keyword in title_lower:
                return score
        return 3.0

    def _email_quality(self, email: str) -> float:
        """Penalize generic email domains."""
        generic = ["gmail", "yahoo", "hotmail", "outlook", "icloud"]
        domain = email.split("@")[-1].lower() if "@" in email else ""
        if any(g in domain for g in generic):
            return -5.0
        return 5.0

    def score(self, lead: Lead) -> float:
        """Compute 0-100 lead score."""
        score = 0.0
        score += self.weights.source_weights.get(lead.source.lower(), 5)
        score += self.weights.company_size_weights.get(lead.company_size, 5)
        score += self._title_score(lead.job_title)
        score += self.weights.industry_weights.get(lead.industry.lower(), 5)
        score += self._email_quality(lead.email)
        return max(0.0, min(100.0, score))

    def score_bulk(self, leads: List[Lead]) -> List[Lead]:
        for lead in leads:
            lead.score = self.score(lead)
        return leads


class LeadStageRouter:
    """Transitions leads between pipeline stages based on score and engagement signals."""

    MQL_THRESHOLD = 40.0
    SQL_THRESHOLD = 65.0

    def assign_stage(self, lead: Lead) -> str:
        if lead.score >= self.SQL_THRESHOLD:
            return "sql"
        elif lead.score >= self.MQL_THRESHOLD:
            return "mql"
        else:
            return "new"

    def route_bulk(self, leads: List[Lead]) -> Dict[str, List[Lead]]:
        queues: Dict[str, List[Lead]] = {"sql": [], "mql": [], "new": []}
        for lead in leads:
            stage = self.assign_stage(lead)
            lead.stage = stage
            queues[stage].append(lead)
        return queues


class LeadDeduplicator:
    """Identifies and removes duplicate leads by email or company+name match."""

    def __init__(self, match_on: str = "email"):
        self.match_on = match_on

    def deduplicate(self, leads: List[Lead]) -> List[Lead]:
        seen = set()
        unique = []
        for lead in leads:
            key = lead.email.lower().strip() if self.match_on == "email" else (
                f"{lead.company.lower()}_{lead.name.lower()}"
            )
            if key not in seen:
                seen.add(key)
                unique.append(lead)
            else:
                logger.debug("Duplicate lead dropped: %s (%s)", lead.name, lead.email)
        return unique


class LeadPipeline:
    """
    End-to-end lead processing pipeline:
    ingest -> deduplicate -> score -> route -> optional dispatch.
    """

    def __init__(self, scoring_weights: Optional[ScoringWeights] = None,
                 dispatch_callback: Optional[Callable[[str, List[Lead]], None]] = None):
        self.scorer = LeadScorer(weights=scoring_weights)
        self.router = LeadStageRouter()
        self.dedup = LeadDeduplicator()
        self.dispatch_callback = dispatch_callback
        self._all_leads: List[Lead] = []

    def ingest(self, leads: List[Lead]) -> int:
        """Add and preprocess new leads. Returns count ingested."""
        deduped = self.dedup.deduplicate(leads)
        scored = self.scorer.score_bulk(deduped)
        self._all_leads.extend(scored)
        return len(scored)

    def process(self) -> Dict[str, List[Lead]]:
        """Route all leads and optionally dispatch to queues."""
        queues = self.router.route_bulk(self._all_leads)
        if self.dispatch_callback:
            for stage, stage_leads in queues.items():
                if stage_leads:
                    self.dispatch_callback(stage, stage_leads)
        return queues

    def pipeline_summary(self) -> Dict:
        stages = {}
        for lead in self._all_leads:
            stages[lead.stage] = stages.get(lead.stage, 0) + 1
        scores = [l.score for l in self._all_leads]
        return {
            "total_leads": len(self._all_leads),
            "stage_distribution": stages,
            "avg_score": round(float(np.mean(scores)), 2) if scores else 0.0,
            "p75_score": round(float(np.percentile(scores, 75)), 2) if scores else 0.0,
        }

    def to_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame([{
            "lead_id": l.lead_id,
            "name": l.name,
            "email": l.email,
            "company": l.company,
            "source": l.source,
            "score": l.score,
            "stage": l.stage,
            "industry": l.industry,
        } for l in self._all_leads])


if __name__ == "__main__":
    np.random.seed(42)
    sources = ["organic", "paid", "referral", "event", "cold_outreach"]
    industries = ["technology", "finance", "healthcare", "logistics", "retail"]
    sizes = ["1-10", "11-50", "51-200", "201-1000", "1000+"]
    titles = ["CEO", "CTO", "VP of Engineering", "Director of Marketing",
              "Software Engineer", "Product Manager", "Head of Sales"]

    sample_leads = []
    for i in range(50):
        email_domain = "corp.com" if i % 5 != 0 else "gmail.com"
        sample_leads.append(Lead(
            lead_id=f"L{i:04d}",
            name=f"Lead {i}",
            email=f"lead{i}@{email_domain}",
            company=f"Company {i % 15}",
            job_title=np.random.choice(titles),
            source=np.random.choice(sources),
            industry=np.random.choice(industries),
            company_size=np.random.choice(sizes),
            country="IN",
        ))

    def queue_printer(stage: str, leads: List[Lead]):
        print(f"  Queue '{stage}': {len(leads)} leads")

    pipeline = LeadPipeline(dispatch_callback=queue_printer)
    ingested = pipeline.ingest(sample_leads)
    print(f"Ingested {ingested} unique leads")
    queues = pipeline.process()
    print("Summary:", pipeline.pipeline_summary())

    df = pipeline.to_dataframe()
    print("\nTop 5 leads by score:")
    print(df.sort_values("score", ascending=False).head(5)[
        ["name", "company", "source", "score", "stage"]
    ].to_string(index=False))
