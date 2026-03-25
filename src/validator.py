"""
validator.py — Post-pipeline validation checks.

Runs against the SQLite database:
  • Primary-key uniqueness
  • Foreign-key consistency
  • Null-sanity (non-nullable PK columns)

Prints a validation report and returns a summary dict.
"""

import sqlite3
from pathlib import Path
from typing import Any

from src.schema_inference import ENTITY_SCHEMAS


# ──────────────────────────────────────────────────────────────────
# Core checks
# ──────────────────────────────────────────────────────────────────

def _check_pk_uniqueness(
    cur: sqlite3.Cursor,
    table: str,
    pk_cols: list[str],
) -> dict:
    """Check that PK columns form a unique key."""
    pk_expr = ", ".join(f'"{c}"' for c in pk_cols)
    sql = (
        f'SELECT {pk_expr}, COUNT(*) AS cnt '
        f'FROM "{table}" GROUP BY {pk_expr} HAVING cnt > 1 LIMIT 5;'
    )
    try:
        cur.execute(sql)
        violations = cur.fetchall()
    except sqlite3.OperationalError as exc:
        return {"status": "ERROR", "message": str(exc)}

    if violations:
        return {
            "status": "FAIL",
            "duplicate_count": len(violations),
            "sample_duplicates": [dict(zip(pk_cols + ["count"], v)) for v in violations],
        }
    return {"status": "PASS"}


def _check_fk_consistency(
    cur: sqlite3.Cursor,
    child_table: str,
    child_cols: list[str],
    parent_table: str,
    parent_cols: list[str],
    confidence: str,
) -> dict:
    """Check that FK values exist in the referenced table."""
    child_expr = ", ".join(f'c."{col}"' for col in child_cols)
    parent_expr = ", ".join(f'p."{col}"' for col in parent_cols)
    join_cond = " AND ".join(
        f'c."{cc}" = p."{pc}"' for cc, pc in zip(child_cols, parent_cols)
    )

    sql = (
        f'SELECT DISTINCT {child_expr} FROM "{child_table}" c '
        f'LEFT JOIN "{parent_table}" p ON {join_cond} '
        f'WHERE p."{parent_cols[0]}" IS NULL '
        f'AND c."{child_cols[0]}" IS NOT NULL '
        f'LIMIT 10;'
    )
    try:
        cur.execute(sql)
        orphans = cur.fetchall()
    except sqlite3.OperationalError as exc:
        return {"status": "ERROR", "message": str(exc), "confidence": confidence}

    if orphans:
        return {
            "status": "WARN" if confidence == "uncertain" else "FAIL",
            "orphan_count": len(orphans),
            "sample_orphans": [dict(zip(child_cols, o)) for o in orphans],
            "confidence": confidence,
        }
    return {"status": "PASS", "confidence": confidence}


def _check_pk_nulls(
    cur: sqlite3.Cursor,
    table: str,
    pk_cols: list[str],
) -> dict:
    """Check that PK columns have no NULLs."""
    issues = []
    for col in pk_cols:
        cur.execute(f'SELECT COUNT(*) FROM "{table}" WHERE "{col}" IS NULL;')
        null_ct = cur.fetchone()[0]
        if null_ct > 0:
            issues.append({"column": col, "null_count": null_ct})
    if issues:
        return {"status": "FAIL", "null_pk_columns": issues}
    return {"status": "PASS"}


# ──────────────────────────────────────────────────────────────────
# Full validation
# ──────────────────────────────────────────────────────────────────

def validate_database(db_path: Path) -> dict:
    """
    Run all validation checks against the SQLite database.

    Returns a dict with per-entity results.
    """
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    results: dict[str, Any] = {}

    for schema in ENTITY_SCHEMAS:
        table = schema["table_name"]
        pk = schema["primary_key"]

        entity_result: dict[str, Any] = {"table": table}

        # Row count
        try:
            cur.execute(f'SELECT COUNT(*) FROM "{table}";')
            entity_result["row_count"] = cur.fetchone()[0]
        except sqlite3.OperationalError:
            entity_result["row_count"] = "TABLE MISSING"
            results[table] = entity_result
            continue

        # PK uniqueness
        entity_result["pk_uniqueness"] = _check_pk_uniqueness(cur, table, pk)

        # PK nulls
        entity_result["pk_nulls"] = _check_pk_nulls(cur, table, pk)

        # FK consistency
        fk_results = []
        for fk in schema.get("foreign_keys", []):
            fk_results.append(
                _check_fk_consistency(
                    cur,
                    child_table=table,
                    child_cols=fk["columns"],
                    parent_table=fk["references_table"],
                    parent_cols=fk["references_columns"],
                    confidence=fk["confidence"],
                )
            )
        entity_result["fk_checks"] = fk_results

        results[table] = entity_result

    conn.close()
    return results


def print_validation_report(results: dict) -> None:
    """Pretty-print the validation report."""
    print("\n" + "=" * 72)
    print("VALIDATION REPORT")
    print("=" * 72)

    total_pass = 0
    total_warn = 0
    total_fail = 0

    for table, info in results.items():
        print(f"\n{'─' * 60}")
        print(f"  {table}  (rows: {info.get('row_count', '?')})")
        print(f"{'─' * 60}")

        # PK uniqueness
        pk = info.get("pk_uniqueness", {})
        status = pk.get("status", "N/A")
        print(f"  PK uniqueness: {status}")
        if status == "PASS":
            total_pass += 1
        elif status == "FAIL":
            total_fail += 1
            print(f"    → duplicates found: {pk.get('duplicate_count')}")

        # PK nulls
        pk_null = info.get("pk_nulls", {})
        status = pk_null.get("status", "N/A")
        print(f"  PK null check: {status}")
        if status == "PASS":
            total_pass += 1
        elif status == "FAIL":
            total_fail += 1
            for issue in pk_null.get("null_pk_columns", []):
                print(f"    → {issue['column']}: {issue['null_count']} NULLs")

        # FK checks
        for i, fk in enumerate(info.get("fk_checks", [])):
            status = fk.get("status", "N/A")
            conf = fk.get("confidence", "")
            label = f"FK check #{i + 1}"
            print(f"  {label}: {status} (confidence: {conf})")
            if status == "PASS":
                total_pass += 1
            elif status == "WARN":
                total_warn += 1
                print(f"    → orphan values: {fk.get('orphan_count')}")
            elif status == "FAIL":
                total_fail += 1
                print(f"    → orphan values: {fk.get('orphan_count')}")

    print(f"\n{'=' * 72}")
    print(f"SUMMARY:  {total_pass} PASS  |  {total_warn} WARN  |  {total_fail} FAIL")
    print(f"{'=' * 72}\n")


# ──────────────────────────────────────────────────────────────────
# Stand-alone entry point
# ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys, os
    # Allow running from project root or src/
    sys.path.insert(0, os.path.dirname(__file__))

    from src.config import DB_PATH

    db = Path(DB_PATH)
    if not db.exists():
        print(f"Database not found at {db}. Run main.py first.")
        sys.exit(1)

    results = validate_database(db)
    print_validation_report(results)
