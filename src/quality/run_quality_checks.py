# ============================================================
# run_quality_checks.py — Data Quality Validation with Great Expectations
# Validates all 3 main tables against expectation suites.
# Generates HTML reports in docs/quality/.
# Runs locally only — not part of the Kestra pipeline.
# ============================================================

import great_expectations as gx
from great_expectations.checkpoint import Checkpoint
from great_expectations.expectations import (
    ExpectColumnValuesToBeInSet,
    ExpectColumnValuesToNotBeNull,
    ExpectColumnValuesToBeUnique,
    ExpectColumnValuesToBeBetween,
    ExpectTableRowCountToBeBetween,
)
from sqlalchemy import create_engine
from pathlib import Path

from src.utils.db import DATABASE_URL
from src.utils.logger import get_logger
from datetime import datetime


logger = get_logger(__name__)

ROOT_DIR = Path(__file__).resolve().parents[2]
QUALITY_DIR = ROOT_DIR / "docs" / "quality"
QUALITY_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# Create GE context and PostgreSQL datasource
# ============================================================
def setup_context():
    logger.info("Setting up Great Expectations context")

    context = gx.get_context(mode="ephemeral")

    datasource = context.data_sources.add_or_update_sql(
        name="sportdb",
        connection_string=DATABASE_URL,
    )

    return context, datasource


# ============================================================
# SUITE 1 — Employees table validation
# ============================================================
def validate_employees(context, datasource):
    logger.info("Validating employees table")

    asset = datasource.add_table_asset(
        name="employees_asset",
        table_name="employees",
    )

    batch_definition = asset.add_batch_definition_whole_table(
        name="employees_full",
    )

    suite = context.suites.add_or_update(
        gx.ExpectationSuite(name="employees_suite")
    )

    # Required columns not null
    suite.add_expectation(ExpectColumnValuesToNotBeNull(column="rh_employee_id"))
    suite.add_expectation(ExpectColumnValuesToNotBeNull(column="rh_bu"))
    suite.add_expectation(ExpectColumnValuesToNotBeNull(column="rh_transport_mode"))
    suite.add_expectation(ExpectColumnValuesToNotBeNull(column="rh_hire_date"))
    suite.add_expectation(ExpectColumnValuesToNotBeNull(column="rh_is_active"))

    # Valid ENUM values
    suite.add_expectation(ExpectColumnValuesToBeInSet(
        column="rh_transport_mode",
        value_set=["walking", "cycling", "motorized"],
    ))

    suite.add_expectation(ExpectColumnValuesToBeInSet(
        column="rh_contract_type",
        value_set=["CDI", "CDD"],
    ))

    # CP days within legal range
    suite.add_expectation(ExpectColumnValuesToBeBetween(
        column="rh_cp_days",
        min_value=25,
        max_value=29,
    ))

    # Employee IDs are unique
    suite.add_expectation(ExpectColumnValuesToBeUnique(column="rh_employee_id"))

    # Minimum row count
    suite.add_expectation(ExpectTableRowCountToBeBetween(min_value=100))

    # Run validation
    validation = gx.ValidationDefinition(
        name="employees_validation",
        data=batch_definition,
        suite=suite,
    )

    validation = context.validation_definitions.add_or_update(validation)
    result = validation.run()

    _log_results("employees", result)
    return result


# ============================================================
# SUITE 2 — Sport activities table validation
# ============================================================
def validate_activities(context, datasource):
    logger.info("Validating sport_activities table")

    asset = datasource.add_table_asset(
        name="activities_asset",
        table_name="sport_activities",
    )

    batch_definition = asset.add_batch_definition_whole_table(
        name="activities_full",
    )

    suite = context.suites.add_or_update(
        gx.ExpectationSuite(name="activities_suite")
    )

    # Required columns not null
    suite.add_expectation(ExpectColumnValuesToNotBeNull(column="sp_employee_id"))
    suite.add_expectation(ExpectColumnValuesToNotBeNull(column="sp_activity_type"))
    suite.add_expectation(ExpectColumnValuesToNotBeNull(column="sp_start_date"))
    suite.add_expectation(ExpectColumnValuesToNotBeNull(column="sp_elapsed_time"))
    suite.add_expectation(ExpectColumnValuesToNotBeNull(column="sp_data_source"))

    # Valid ENUM values
    suite.add_expectation(ExpectColumnValuesToBeInSet(
        column="sp_activity_type",
        value_set=[
            "running", "walking", "cycling", "hiking",
            "swimming", "racket_sports", "combat_sports",
            "team_sports", "outdoor_sports", "other",
        ],
    ))

    suite.add_expectation(ExpectColumnValuesToBeInSet(
        column="sp_data_source",
        value_set=["simulated", "strava"],
    ))

    # Physical coherence checks
    suite.add_expectation(ExpectColumnValuesToBeBetween(
        column="sp_elapsed_time",
        min_value=1,
        max_value=86400,
    ))

    # Minimum row count
    suite.add_expectation(ExpectTableRowCountToBeBetween(min_value=1000))

    # Run validation
    validation = gx.ValidationDefinition(
        name="activities_validation",
        data=batch_definition,
        suite=suite,
    )

    validation = context.validation_definitions.add_or_update(validation)
    result = validation.run()

    _log_results("sport_activities", result)
    return result


# ============================================================
# SUITE 3 — Employee benefits table validation
# ============================================================
def validate_benefits(context, datasource):
    logger.info("Validating employee_benefits table")

    asset = datasource.add_table_asset(
        name="benefits_asset",
        table_name="employee_benefits",
    )

    batch_definition = asset.add_batch_definition_whole_table(
        name="benefits_full",
    )

    suite = context.suites.add_or_update(
        gx.ExpectationSuite(name="benefits_suite")
    )

    # Required columns not null
    suite.add_expectation(ExpectColumnValuesToNotBeNull(column="be_employee_id"))
    suite.add_expectation(ExpectColumnValuesToNotBeNull(column="be_flg_prime"))
    suite.add_expectation(ExpectColumnValuesToNotBeNull(column="be_flg_well_being"))
    suite.add_expectation(ExpectColumnValuesToNotBeNull(column="be_prime_amount"))
    suite.add_expectation(ExpectColumnValuesToNotBeNull(column="be_activity_count"))

    # Valid ranges
    suite.add_expectation(ExpectColumnValuesToBeBetween(
        column="be_prime_amount",
        min_value=0,
    ))

    suite.add_expectation(ExpectColumnValuesToBeBetween(
        column="be_activity_count",
        min_value=0,
    ))

    suite.add_expectation(ExpectColumnValuesToBeInSet(
        column="be_well_being_days",
        value_set=[0, 5],
    ))

    # Boolean flags
    suite.add_expectation(ExpectColumnValuesToBeInSet(
        column="be_flg_prime",
        value_set=[True, False],
    ))

    suite.add_expectation(ExpectColumnValuesToBeInSet(
        column="be_flg_well_being",
        value_set=[True, False],
    ))

    # Employee IDs are unique per period
    suite.add_expectation(ExpectColumnValuesToBeUnique(column="be_employee_id"))

    # Exact row count
    suite.add_expectation(ExpectTableRowCountToBeBetween(
        min_value=161,
        max_value=161,
    ))

    # Run validation
    validation = gx.ValidationDefinition(
        name="benefits_validation",
        data=batch_definition,
        suite=suite,
    )

    validation = context.validation_definitions.add_or_update(validation)
    result = validation.run()

    _log_results("employee_benefits", result)
    return result


# ============================================================
# Log validation results summary
# ============================================================
def _log_results(table_name: str, result):
    stats = result.describe_dict()

    success = stats.get("success", False)
    evaluated = stats.get("statistics", {}).get("evaluated_expectations", 0)
    successful = stats.get("statistics", {}).get("successful_expectations", 0)
    failed = evaluated - successful

    status = "PASSED" if success else "FAILED"

    logger.info(
        f"{table_name}: {status} — "
        f"{successful}/{evaluated} expectations passed"
        f"{f' ({failed} failed)' if failed > 0 else ''}"
    )


# ============================================================
# Generate HTML quality report from validation results
# ============================================================
def _generate_html_report(results: dict):
    from datetime import datetime

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    total_passed = 0
    total_expectations = 0

    # Build table sections
    table_sections = []

    for table_name, result in results.items():
        desc = result.describe_dict()
        stats = desc.get("statistics", {})
        evaluated = stats.get("evaluated_expectations", 0)
        successful = stats.get("successful_expectations", 0)
        suite_passed = desc.get("success", False)

        total_passed += successful
        total_expectations += evaluated

        status_color = "#0f6e56" if suite_passed else "#993c1d"
        status_bg = "#e1f5ee" if suite_passed else "#faece7"
        status_text = "PASSED" if suite_passed else "FAILED"

        rows_html = ""
        for exp in desc.get("expectations", []):
            exp_type = exp.get("expectation_type", "unknown")
            column = exp.get("kwargs", {}).get("column", "-")
            success = exp.get("success", False)
            res = exp.get("result", {})
            element_count = res.get("element_count", None)
            unexpected_count = res.get("unexpected_count", None)
            unexpected_pct = res.get("unexpected_percent", None)
            observed_value = res.get("observed_value", None)

            if element_count is None and observed_value is not None:
                element_count = f"{observed_value} rows"
                unexpected_count = "-"
                unexpected_pct = None
            elif element_count is None:
                element_count = "-"
                unexpected_count = "-"
                unexpected_pct = None

            # Clean up expectation type for display
            display_type = exp_type.replace("expect_", "").replace("_", " ").title()

            if success:
                badge = '<span style="background:#e1f5ee;color:#0f6e56;padding:3px 10px;border-radius:4px;font-weight:bold;">PASS</span>'
            else:
                badge = '<span style="background:#faece7;color:#993c1d;padding:3px 10px;border-radius:4px;font-weight:bold;">FAIL</span>'

            rows_html += f"""
                <tr>
                    <td style="padding:10px;border-bottom:1px solid #eee;">{display_type}</td>
                    <td style="padding:10px;border-bottom:1px solid #eee;font-family:monospace;">{column}</td>
                    <td style="padding:10px;border-bottom:1px solid #eee;text-align:center;">{element_count}</td>
                    <td style="padding:10px;border-bottom:1px solid #eee;text-align:center;">{unexpected_count if unexpected_count is not None else '-'}</td>
                    <td style="padding:10px;border-bottom:1px solid #eee;text-align:center;">{f'{unexpected_pct:.1f}%' if unexpected_pct is not None else '-'}</td>
                    <td style="padding:10px;border-bottom:1px solid #eee;text-align:center;">{badge}</td>
                </tr>"""

        table_sections.append(f"""
        <div style="background:white;padding:25px;border-radius:10px;margin:25px 0;box-shadow:0 2px 8px rgba(0,0,0,0.08);">
            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:15px;">
                <h2 style="margin:0;color:#333;">{table_name}</h2>
                <span style="background:{status_bg};color:{status_color};padding:6px 16px;border-radius:6px;font-weight:bold;font-size:0.95em;">
                    {status_text} — {successful}/{evaluated}
                </span>
            </div>
            <table style="width:100%;border-collapse:collapse;">
                <tr style="background:#f8f7ff;">
                    <th style="padding:10px;text-align:left;border-bottom:2px solid #ddd;">Expectation</th>
                    <th style="padding:10px;text-align:left;border-bottom:2px solid #ddd;">Column</th>
                    <th style="padding:10px;text-align:center;border-bottom:2px solid #ddd;">Rows</th>
                    <th style="padding:10px;text-align:center;border-bottom:2px solid #ddd;">Failures</th>
                    <th style="padding:10px;text-align:center;border-bottom:2px solid #ddd;">Fail %</th>
                    <th style="padding:10px;text-align:center;border-bottom:2px solid #ddd;">Status</th>
                </tr>
                {rows_html}
            </table>
        </div>""")

    all_passed = total_passed == total_expectations
    summary_bg = "#e1f5ee" if all_passed else "#faece7"
    summary_color = "#0f6e56" if all_passed else "#993c1d"
    summary_text = "ALL CHECKS PASSED" if all_passed else "SOME CHECKS FAILED"

    html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Sport Data Solution — Data Quality Report</title>
</head>
<body style="font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif;margin:0;padding:40px;background:#f5f5f5;color:#333;">

    <div style="max-width:1000px;margin:0 auto;">

        <h1 style="margin-bottom:5px;">Data Quality Report</h1>
        <p style="color:#888;margin-top:0;">Sport Data Solution — Generated {timestamp}</p>

        <div style="background:{summary_bg};color:{summary_color};padding:20px;border-radius:10px;margin:25px 0;font-size:1.3em;font-weight:bold;text-align:center;border:2px solid {summary_color};">
            {summary_text}: {total_passed}/{total_expectations} expectations across {len(results)} tables
        </div>

        {"".join(table_sections)}

        <p style="color:#aaa;text-align:center;margin-top:40px;font-size:0.85em;">
            Validated with Great Expectations v1.15 — PostgreSQL sportdb
        </p>

    </div>
</body>
</html>"""

    report_path = QUALITY_DIR / "quality_report.html"
    report_path.write_text(html, encoding="utf-8")
    logger.info(f"HTML report generated: {report_path}")

# ============================================================
# MAIN — Run all 3 suites
# ============================================================
def main():
    logger.info("=" * 60)
    logger.info("DATA QUALITY CHECKS — Starting")
    logger.info("=" * 60)

    context, datasource = setup_context()

    results = {
        "employees": validate_employees(context, datasource),
        "activities": validate_activities(context, datasource),
        "benefits": validate_benefits(context, datasource),
    }

    # Generate HTML report
    _generate_html_report(results)

    logger.info("=" * 60)

    all_passed = all(r.describe_dict().get("success", False) for r in results.values())

    if all_passed:
        logger.info("DATA QUALITY CHECKS — ALL PASSED")
    else:
        logger.error("DATA QUALITY CHECKS — SOME FAILURES DETECTED")

    logger.info(f"HTML report saved to: {QUALITY_DIR / 'quality_report.html'}")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()