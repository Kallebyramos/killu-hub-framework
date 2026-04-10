"""
Standard Silver-layer transformations for KilluHub.

Each function is a pure, stateless DataFrame → DataFrame transform.
They can be used individually or composed via apply_transforms().

All functions are defensive:
- Unknown column names are silently skipped.
- An empty list / empty dict means "do nothing", not "crash".
- No direct imports of killuhub internals — only PySpark.
"""
from __future__ import annotations

from typing import Any, Callable


# ---------------------------------------------------------------------------
# Composer
# ---------------------------------------------------------------------------

def apply_transforms(df: Any, steps: list[Callable]) -> Any:
    """
    Pipe a DataFrame through an ordered list of transform functions.

    Each step is a callable that accepts and returns a DataFrame.
    None entries in the list are skipped — this makes it convenient to
    pass optional transforms without if/else at the call site.

    Example:
        df = apply_transforms(df, [
            lambda d: add_metadata_columns(d),
            lambda d: parse_date_columns(d, ["created_at"]),
            my_custom_transform,
            None,   # skipped
        ])
    """
    for step in steps:
        if step is not None:
            df = step(df)
    return df


# ---------------------------------------------------------------------------
# Metadata
# ---------------------------------------------------------------------------

def add_metadata_columns(
    df: Any,
    layer: str = "silver",
    run_id: str = "",
) -> Any:
    """
    Stamp Silver processing metadata onto every row.

    Added columns:
        _processed_at  TIMESTAMP  — when this Silver run executed
        _layer         STRING     — literal "silver" (or provided value)
        _run_id        STRING     — the Silver pipeline run UUID

    These columns let you trace any row in Silver back to the exact
    pipeline run that produced it.
    """
    from pyspark.sql import functions as F

    return (
        df
        .withColumn("_processed_at", F.current_timestamp())
        .withColumn("_layer", F.lit(layer))
        .withColumn("_run_id", F.lit(run_id))
    )


# ---------------------------------------------------------------------------
# Date / timestamp handling
# ---------------------------------------------------------------------------

def parse_date_columns(
    df: Any,
    columns: list[str],
    input_format: str = "yyyy-MM-dd HH:mm:ss",
) -> Any:
    """
    Parse string columns to TimestampType using the given format.

    Columns not present in the DataFrame are silently skipped so this
    transform is safe to apply even after schema evolution.

    Args:
        columns:      List of column names to parse.
        input_format: Java SimpleDateFormat pattern (Spark's native format).
                      Common values:
                        "yyyy-MM-dd HH:mm:ss"       → "2024-01-15 10:30:00"
                        "yyyy-MM-dd'T'HH:mm:ss"     → ISO 8601
                        "yyyy-MM-dd"                → date-only strings
                        "dd/MM/yyyy HH:mm:ss"       → European format
    """
    from pyspark.sql import functions as F

    existing = set(df.columns)
    for col in columns:
        if col in existing:
            df = df.withColumn(col, F.to_timestamp(F.col(col), input_format))
    return df


def add_date_dimensions(
    df: Any,
    source_column: str,
    prefix: str = "",
) -> Any:
    """
    Derive year / month / day / hour partition/filter columns from a timestamp.

    Added columns (with optional prefix):
        {prefix}year   INT   — e.g. 2024
        {prefix}month  INT   — 1-12
        {prefix}day    INT   — 1-31
        {prefix}hour   INT   — 0-23
        {prefix}date   DATE  — date-only truncation

    These are the most common date metadata columns used in Silver layer
    partitioning and downstream BI tooling.

    Args:
        source_column: Name of a TIMESTAMP or DATE column to derive from.
        prefix:        Optional prefix for generated column names,
                       e.g. "order_" → "order_year", "order_month", ...
    """
    from pyspark.sql import functions as F

    if source_column not in df.columns:
        return df

    p = prefix
    return (
        df
        .withColumn(f"{p}year",  F.year(source_column))
        .withColumn(f"{p}month", F.month(source_column))
        .withColumn(f"{p}day",   F.dayofmonth(source_column))
        .withColumn(f"{p}hour",  F.hour(source_column))
        .withColumn(f"{p}date",  F.to_date(source_column))
    )


# ---------------------------------------------------------------------------
# Type casting
# ---------------------------------------------------------------------------

def cast_types(df: Any, type_map: dict[str, str]) -> Any:
    """
    Cast columns to the specified Spark SQL types.

    Args:
        type_map: dict mapping column names → Spark SQL type string.
                  Examples:
                    {"amount": "double", "quantity": "int"}
                    {"price": "decimal(18,4)", "active": "boolean"}
                    {"event_time": "timestamp"}

    Columns not present in the DataFrame are silently skipped.
    """
    from pyspark.sql import functions as F

    existing = set(df.columns)
    for col, spark_type in type_map.items():
        if col in existing:
            df = df.withColumn(col, F.col(col).cast(spark_type))
    return df


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

def deduplicate(
    df: Any,
    key_columns: list[str],
    order_column: str = "_ingested_at",
    keep: str = "latest",
) -> Any:
    """
    Remove duplicate rows keeping one record per key_columns combination.

    Uses a window function to rank rows within each key partition,
    then keeps only the top-ranked row. This is more reliable than
    DataFrame.dropDuplicates() because it allows you to control
    *which* duplicate to keep based on an ordering column.

    Args:
        key_columns:   Columns that together uniquely identify a record.
        order_column:  Column used to rank duplicates.
        keep:          "latest" keeps the row with the highest order_column.
                       "earliest" keeps the row with the lowest.

    If key_columns is empty, this function is a no-op.

    Example:
        # Keep the most recently ingested version of each order
        deduplicate(df, key_columns=["order_id"], order_column="_ingested_at")
    """
    if not key_columns:
        return df

    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    order_expr = (
        F.col(order_column).desc()
        if keep == "latest"
        else F.col(order_column).asc()
    )
    window = Window.partitionBy(*key_columns).orderBy(order_expr)

    return (
        df
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


# ---------------------------------------------------------------------------
# Column utilities
# ---------------------------------------------------------------------------

def rename_columns(df: Any, rename_map: dict[str, str]) -> Any:
    """
    Rename columns using a {old_name: new_name} mapping.
    Columns not present in the DataFrame are silently skipped.
    """
    for old, new in rename_map.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)
    return df


def drop_columns(df: Any, columns: list[str]) -> Any:
    """
    Drop columns that are present in the DataFrame.
    Columns not present are silently ignored.
    """
    to_drop = [c for c in columns if c in df.columns]
    return df.drop(*to_drop) if to_drop else df


def filter_nulls(df: Any, columns: list[str]) -> Any:
    """
    Drop rows where any of the specified columns is null.
    Useful for enforcing NOT NULL constraints on key columns before writing.
    """
    from pyspark.sql import functions as F

    condition = None
    for col in columns:
        if col in df.columns:
            expr = F.col(col).isNotNull()
            condition = expr if condition is None else condition & expr

    return df.filter(condition) if condition is not None else df
