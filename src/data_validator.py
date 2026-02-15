from __future__ import annotations

import re
import json
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd


def _iqr_bounds(series: pd.Series, factor: float = 1.5) -> Tuple[float, float]:
    q1 = series.quantile(0.25)
    q3 = series.quantile(0.75)
    iqr = q3 - q1
    return q1 - factor * iqr, q3 + factor * iqr

def infer_schema(df: pd.DataFrame, sample_values: int = 5) -> Dict[str, Any]:
    """Return a lightweight schema dict describing dtypes, null% and sample values for columns.

    Good for quick validation or to seed a more formal schema.
    """
    schema: Dict[str, Any] = {}
    n = len(df)
    for col in df.columns:
        s = df[col]
        dtype = str(s.dtype)
        nulls = int(s.isna().sum())
        null_pct = float(nulls) / n if n else 0.0
        unique = int(s.nunique(dropna=True))
        sample = s.dropna().unique()[:sample_values].tolist()
        schema[col] = {
            "dtype": dtype,
            "null_count": nulls,
            "null_pct": round(null_pct, 4),
            "unique": unique,
            "sample_values": sample,
        }
    return schema

class DataValidator:
    """Collection of static utilities that perform common validation checks on DataFrames.

    Each check returns a small dict describing findings. Use validate() to run many checks at once.
    """

    @staticmethod
    def missing_summary(df: pd.DataFrame) -> Dict[str, Any]:
        n = len(df)
        col_summary = (
            df.isna().sum().rename("missing_count").to_frame().assign(
                missing_pct=lambda d: d["missing_count"] / n
            )
        )
        return {"rows": n, "columns": len(df.columns), "per_column": col_summary.to_dict(orient="index")}

    @staticmethod
    def duplicates(df: pd.DataFrame, subset: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        if subset is None:
            dup_mask = df.duplicated()
        else:
            dup_mask = df.duplicated(subset=list(subset))
        count = int(dup_mask.sum())
        sample = df[dup_mask].head(5).to_dict(orient="records") if count else []
        return {"duplicate_count": count, "sample_rows": sample}

    @staticmethod
    def dtype_issues(df: pd.DataFrame, expected: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """Compare actual dtypes against an expected mapping (if provided)."""
        actual = {c: str(t) for c, t in df.dtypes.items()}
        issues = {}
        if expected:
            for c, exp in expected.items():
                act = actual.get(c)
                if act != exp:
                    issues[c] = {"expected": exp, "actual": act}
        return {"actual_dtypes": actual, "dtype_mismatches": issues}

    @staticmethod
    def cardinality_report(df: pd.DataFrame, top_n: int = 5) -> Dict[str, Any]:
        report = {}
        for col in df.columns:
            try:
                vc = df[col].value_counts(dropna=False)
                report[col] = {
                    "unique": int(vc.shape[0]),
                    "top_values": vc.head(top_n).to_dict(),
                }
            except Exception:
                report[col] = {"error": "could not compute value_counts"}
        return report

    @staticmethod
    def outlier_summary(df: pd.DataFrame, numeric_cols: Optional[Sequence[str]] = None, factor: float = 1.5) -> Dict[str, Any]:
        if numeric_cols is None:
            numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        summary = {}
        for col in numeric_cols:
            s = df[col].dropna()
            if s.empty:
                summary[col] = {"n": 0}
                continue
            low, high = _iqr_bounds(s, factor)
            n_low = int((s < low).sum())
            n_high = int((s > high).sum())
            summary[col] = {"n": int(s.shape[0]), "low_bound": low, "high_bound": high, "below": n_low, "above": n_high}
        return summary

    @staticmethod
    def validate(df: pd.DataFrame, expected_schema: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """Run a small battery of checks and return a combined report."""
        return {
            "missing": DataValidator.missing_summary(df),
            "duplicates": DataValidator.duplicates(df),
            "dtypes": DataValidator.dtype_issues(df, expected_schema),
            "cardinality": DataValidator.cardinality_report(df, top_n=5),
            "outliers": DataValidator.outlier_summary(df),
        }