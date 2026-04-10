"""
Data contracts for KilluHub pipelines.

A contract is a formal agreement about the shape and quality of data
at a pipeline boundary. Contracts catch problems early — at ingestion
(Bronze) or at transformation (Silver) — rather than when a BI dashboard
shows nonsense to a business user.

Each pipeline stage can declare a ContractSpec. After processing, the
ContractValidator runs all checks and produces a ContractReport.

On violation:
  "warn"  — log the violations but allow the pipeline to continue
  "fail"  — raise ContractViolationError and abort the pipeline

Usage:
    spec = ContractSpec(
        columns=[
            ColumnSpec("order_id", "long",   nullable=False),
            ColumnSpec("amount",   "double", nullable=False, min_value=0),
            ColumnSpec("status",   "string", nullable=False,
                       allowed_values=["pending", "paid", "shipped"]),
        ],
        on_violation="fail",
        min_row_count=1,
    )
    report = ContractValidator(spec).validate(df, table_name="bronze.orders")
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from killuhub.core.exceptions import KilluHubError

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class ContractViolationError(KilluHubError):
    """Raised when a contract check fails and on_violation='fail'."""
    def __init__(self, table: str, violations: list["Violation"]):
        count = len(violations)
        summary = "; ".join(f"[{v.check}] {v.column}: {v.message}" for v in violations[:3])
        super().__init__(
            f"Contract violated on '{table}' — {count} error(s). First: {summary}"
        )
        self.violations = violations


# ---------------------------------------------------------------------------
# Spec
# ---------------------------------------------------------------------------

@dataclass
class ColumnSpec:
    """
    Contract definition for a single column.

    Attributes:
        name:            Column name. Must be present in the DataFrame.
        type:            Expected Spark SQL type string (e.g. "long", "double",
                         "string", "timestamp", "decimal(18,4)").
                         Type check is skipped if empty string.
        nullable:        If False, the column must have zero nulls.
        min_value:       Numeric lower bound (inclusive). None = unchecked.
        max_value:       Numeric upper bound (inclusive). None = unchecked.
        allowed_values:  Exhaustive list of valid values. None = unchecked.
        max_null_pct:    Maximum fraction of nulls allowed (0.0–1.0).
                         Ignored if nullable=False (which implies 0.0).
    """
    name: str
    type: str = ""
    nullable: bool = True
    min_value: Any = None
    max_value: Any = None
    allowed_values: list | None = None
    max_null_pct: float = 1.0

    @classmethod
    def from_dict(cls, data: dict) -> "ColumnSpec":
        return cls(
            name=data["name"],
            type=data.get("type", ""),
            nullable=data.get("nullable", True),
            min_value=data.get("min_value"),
            max_value=data.get("max_value"),
            allowed_values=data.get("allowed_values"),
            max_null_pct=float(data.get("max_null_pct", 1.0)),
        )


@dataclass
class ContractSpec:
    """
    Full contract definition for a pipeline stage.

    Attributes:
        columns:         List of column-level checks.
        on_violation:    "warn" — log and continue | "fail" — raise and abort.
        min_row_count:   Minimum number of rows required. None = unchecked.
        max_row_count:   Maximum number of rows allowed.  None = unchecked.
    """
    columns: list[ColumnSpec] = field(default_factory=list)
    on_violation: str = "warn"
    min_row_count: int | None = None
    max_row_count: int | None = None

    @classmethod
    def from_dict(cls, data: dict) -> "ContractSpec":
        columns = [ColumnSpec.from_dict(c) for c in data.get("columns", [])]
        return cls(
            columns=columns,
            on_violation=data.get("on_violation", "warn"),
            min_row_count=data.get("min_row_count"),
            max_row_count=data.get("max_row_count"),
        )


# ---------------------------------------------------------------------------
# Violation record
# ---------------------------------------------------------------------------

@dataclass
class Violation:
    column: str      # "__table__" for table-level checks
    check: str       # e.g. "nullable", "type", "min_value", "row_count"
    message: str
    severity: str    # "error" | "warning"


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

@dataclass
class ContractReport:
    """
    Result of a ContractValidator.validate() call.

    Attributes:
        table_name:      The table being validated.
        run_id:          Pipeline run identifier for traceability.
        passed:          True if no error-severity violations were found.
        violations:      All violations found (errors + warnings).
        row_count:       Actual row count in the validated DataFrame.
        checked_at:      ISO-8601 UTC timestamp of when the check ran.
    """
    table_name: str
    run_id: str
    passed: bool
    violations: list[Violation]
    row_count: int
    checked_at: str

    @property
    def errors(self) -> list[Violation]:
        return [v for v in self.violations if v.severity == "error"]

    @property
    def warnings(self) -> list[Violation]:
        return [v for v in self.violations if v.severity == "warning"]

    def log_summary(self) -> None:
        status = "PASSED" if self.passed else "FAILED"
        logger.info(
            "Contract %s | table=%s run_id=%s rows=%d errors=%d warnings=%d",
            status, self.table_name, self.run_id, self.row_count,
            len(self.errors), len(self.warnings),
        )
        for v in self.violations:
            fn = logger.error if v.severity == "error" else logger.warning
            fn("  [%s] column=%s — %s", v.check, v.column, v.message)

    def to_dict(self) -> dict:
        return {
            "table_name": self.table_name,
            "run_id": self.run_id,
            "passed": self.passed,
            "row_count": self.row_count,
            "checked_at": self.checked_at,
            "errors": len(self.errors),
            "warnings": len(self.warnings),
            "violations": [
                {"column": v.column, "check": v.check,
                 "message": v.message, "severity": v.severity}
                for v in self.violations
            ],
        }


# ---------------------------------------------------------------------------
# Validator
# ---------------------------------------------------------------------------

class ContractValidator:
    """
    Runs all checks defined in a ContractSpec against a Spark DataFrame.

    Checks are intentionally batched into as few Spark actions as possible
    to avoid triggering multiple full DataFrame scans.
    """

    def __init__(self, spec: ContractSpec):
        self.spec = spec

    def validate(self, df: Any, table_name: str, run_id: str = "") -> ContractReport:
        """
        Execute all contract checks and return a ContractReport.

        Raises ContractViolationError if on_violation='fail' and errors exist.
        """
        from pyspark.sql import functions as F

        violations: list[Violation] = []
        severity = "error" if self.spec.on_violation == "fail" else "warning"

        # --- Row count ---
        row_count = df.count()
        if self.spec.min_row_count is not None and row_count < self.spec.min_row_count:
            violations.append(Violation(
                column="__table__", check="min_row_count", severity=severity,
                message=f"Expected >= {self.spec.min_row_count} rows, got {row_count}",
            ))
        if self.spec.max_row_count is not None and row_count > self.spec.max_row_count:
            violations.append(Violation(
                column="__table__", check="max_row_count", severity=severity,
                message=f"Expected <= {self.spec.max_row_count} rows, got {row_count}",
            ))

        if row_count == 0:
            # No data to check further
            return self._build_report(table_name, run_id, violations, row_count)

        actual_columns = set(df.columns)
        actual_types = {f.name: f.dataType.simpleString() for f in df.schema.fields}

        # Collect all null counts in one Spark action
        null_agg_exprs = [
            F.sum(F.col(spec.name).isNull().cast("int")).alias(f"_null_{spec.name}")
            for spec in self.spec.columns
            if spec.name in actual_columns
        ]
        null_counts: dict[str, int] = {}
        if null_agg_exprs:
            row = df.agg(*null_agg_exprs).collect()[0]
            for spec in self.spec.columns:
                if spec.name in actual_columns:
                    null_counts[spec.name] = row[f"_null_{spec.name}"] or 0

        # Per-column checks
        for col_spec in self.spec.columns:
            col = col_spec.name

            # Presence
            if col not in actual_columns:
                violations.append(Violation(
                    column=col, check="presence", severity=severity,
                    message=f"Column '{col}' not found in DataFrame.",
                ))
                continue  # remaining checks are meaningless

            # Type
            if col_spec.type:
                actual = actual_types.get(col, "")
                expected = col_spec.type.lower().replace(" ", "")
                actual_clean = actual.lower().replace(" ", "")
                if actual_clean != expected:
                    violations.append(Violation(
                        column=col, check="type", severity=severity,
                        message=f"Expected type '{col_spec.type}', got '{actual}'.",
                    ))

            # Nullability
            null_count = null_counts.get(col, 0)
            if not col_spec.nullable and null_count > 0:
                violations.append(Violation(
                    column=col, check="nullable", severity=severity,
                    message=f"{null_count} null value(s) found in non-nullable column.",
                ))
            elif col_spec.nullable and col_spec.max_null_pct < 1.0:
                null_pct = null_count / row_count
                if null_pct > col_spec.max_null_pct:
                    violations.append(Violation(
                        column=col, check="max_null_pct", severity=severity,
                        message=(
                            f"Null % is {null_pct:.1%}, exceeds "
                            f"max_null_pct={col_spec.max_null_pct:.1%}."
                        ),
                    ))

            # Value range and allowed values (one Spark action per column with constraints)
            range_violations = self._check_value_constraints(
                df, col_spec, row_count, severity
            )
            violations.extend(range_violations)

        return self._build_report(table_name, run_id, violations, row_count)

    def _check_value_constraints(
        self, df: Any, col_spec: ColumnSpec, row_count: int, severity: str
    ) -> list[Violation]:
        from pyspark.sql import functions as F

        violations: list[Violation] = []
        col = col_spec.name
        agg_exprs = []

        if col_spec.min_value is not None:
            agg_exprs.append(
                F.sum((F.col(col) < col_spec.min_value).cast("int")).alias("_below_min")
            )
        if col_spec.max_value is not None:
            agg_exprs.append(
                F.sum((F.col(col) > col_spec.max_value).cast("int")).alias("_above_max")
            )
        if col_spec.allowed_values is not None:
            agg_exprs.append(
                F.sum((~F.col(col).isin(*col_spec.allowed_values)).cast("int")).alias("_invalid_vals")
            )

        if not agg_exprs:
            return violations

        row = df.agg(*agg_exprs).collect()[0]

        if col_spec.min_value is not None:
            bad = row["_below_min"] or 0
            if bad > 0:
                violations.append(Violation(
                    column=col, check="min_value", severity=severity,
                    message=f"{bad} row(s) have {col} < {col_spec.min_value}.",
                ))
        if col_spec.max_value is not None:
            bad = row["_above_max"] or 0
            if bad > 0:
                violations.append(Violation(
                    column=col, check="max_value", severity=severity,
                    message=f"{bad} row(s) have {col} > {col_spec.max_value}.",
                ))
        if col_spec.allowed_values is not None:
            bad = row["_invalid_vals"] or 0
            if bad > 0:
                violations.append(Violation(
                    column=col, check="allowed_values", severity=severity,
                    message=(
                        f"{bad} row(s) have values outside "
                        f"allowed set {col_spec.allowed_values}."
                    ),
                ))
        return violations

    def _build_report(
        self,
        table_name: str,
        run_id: str,
        violations: list[Violation],
        row_count: int,
    ) -> ContractReport:
        errors = [v for v in violations if v.severity == "error"]
        report = ContractReport(
            table_name=table_name,
            run_id=run_id,
            passed=len(errors) == 0,
            violations=violations,
            row_count=row_count,
            checked_at=datetime.now(timezone.utc).isoformat(),
        )
        report.log_summary()

        if not report.passed and self.spec.on_violation == "fail":
            raise ContractViolationError(table_name, errors)

        return report
