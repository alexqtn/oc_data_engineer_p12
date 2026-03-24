# ============================================================
# compute_benefits.py — Benefit Pipeline (Phase 3)
# Reads active rules + employees + activities, computes
# prime and well-being eligibility, upserts into employee_benefits.
# ============================================================

import pandas as pd
from datetime import date
from sqlalchemy import text

from src.utils.db import get_engine, get_session
from src.utils.encryption import ENCRYPTION_KEY
from src.utils.logger import get_logger


logger = get_logger(__name__)


# ============================================================
# STEP 1 — Read active benefit rules (latest per rule name).
# Uses DISTINCT ON to get most recent effective_date per rule.
# ============================================================
def load_rules() -> dict:
    logger.info("Step 1 — Loading active benefit rules")

    query = """
        SELECT DISTINCT ON (ru_name)
            ru_name, ru_value
        FROM benefit_rules
        WHERE ru_effective_date <= NOW()
        ORDER BY ru_name, ru_effective_date DESC
    """

    df = pd.read_sql(query, get_engine())

    rules = {}
    for _, row in df.iterrows():
        rules[row["ru_name"]] = float(row["ru_value"])

    logger.info(f"Loaded {len(rules)} rules: {rules}")
    return rules


# ============================================================
# STEP 2 — Define the benefit period (current year).
# ============================================================
def get_period() -> tuple[date, date]:
    from dateutil.relativedelta import relativedelta

    period_end = date.today()
    period_start = period_end - relativedelta(months=12)

    logger.info(f"Step 2 — Period: {period_start} to {period_end}")
    return period_start, period_end


# ============================================================
# STEP 3 — Read employees with their activity counts.
# LEFT JOIN ensures employees without activities appear (count=0).
# Salary is decrypted for prime calculation.
# ============================================================
def load_employee_data(period_start: date, period_end: date) -> pd.DataFrame:
    logger.info("Step 3 — Loading employees with activity counts")

    query = f"""
        SELECT
            e.rh_employee_id,
            pgp_sym_decrypt(e.rh_gross_salary, '{ENCRYPTION_KEY}')::FLOAT AS gross_salary,
            e.rh_transport_mode,
            e.be_declaration_valid,
            COUNT(a.sp_activity_id) AS activity_count,
            COALESCE(SUM(a.sp_distance), 0) AS total_distance
        FROM employees e
        LEFT JOIN sport_activities a
            ON e.rh_employee_id = a.sp_employee_id
            AND a.sp_start_date BETWEEN '{period_start}' AND '{period_end}'
            AND a.sp_is_active = TRUE
        WHERE e.rh_is_active = TRUE
        GROUP BY e.rh_employee_id, gross_salary,
                 e.rh_transport_mode, e.be_declaration_valid
    """

    df = pd.read_sql(query, get_engine())

    logger.info(
        f"Loaded {len(df)} employees — "
        f"{len(df[df['activity_count'] > 0])} with activities, "
        f"{len(df[df['activity_count'] == 0])} without"
    )

    return df


# ============================================================
# STEP 4 — Compute prime and well-being eligibility per employee.
# ============================================================
def compute_eligibility(df: pd.DataFrame, rules: dict) -> pd.DataFrame:
    logger.info("Step 4 — Computing eligibility")

    prime_rate = rules.get("prime_rate", 0.05)
    min_activities = int(rules.get("min_activities", 15))
    well_being_days = int(rules.get("well_being_days", 5))

    # Prime: walking/cycling with valid declaration
    df["be_flg_prime"] = (
        df["rh_transport_mode"].isin(["walking", "cycling"])
        & (df["be_declaration_valid"] == True)
    )
    df["be_prime_amount"] = df.apply(
        lambda row: round(row["gross_salary"] * prime_rate, 2)
        if row["be_flg_prime"] else 0,
        axis=1,
    )

    # Well-being: enough activities in the period
    df["be_flg_well_being"] = df["activity_count"] >= min_activities
    df["be_well_being_days"] = df["be_flg_well_being"].apply(
        lambda x: well_being_days if x else 0
    )

    prime_count = df["be_flg_prime"].sum()
    wellbeing_count = df["be_flg_well_being"].sum()
    total_prime = df["be_prime_amount"].sum()

    logger.info(
        f"Prime eligible: {prime_count} employees "
        f"(total cost: {total_prime:,.2f} euros)"
    )
    logger.info(
        f"Well-being eligible: {wellbeing_count} employees "
        f"({wellbeing_count * well_being_days} days total)"
    )

    return df


# ============================================================
# STEP 5 — Upsert results into employee_benefits table.
# One row per employee per period. Re-run updates existing rows.
# ============================================================
UPSERT_QUERY = text("""
    INSERT INTO employee_benefits (
        be_employee_id,
        be_period_start,
        be_period_end,
        be_activity_count,
        be_distance,
        be_prime_amount,
        be_well_being_days,
        be_flg_prime,
        be_flg_well_being,
        be_declaration_valid,
        be_created_at,
        be_updated_at
    ) VALUES (
        :be_employee_id,
        :be_period_start,
        :be_period_end,
        :be_activity_count,
        :be_distance,
        :be_prime_amount,
        :be_well_being_days,
        :be_flg_prime,
        :be_flg_well_being,
        :be_declaration_valid,
        NOW(),
        NOW()
    )
    ON CONFLICT (be_employee_id, be_period_start, be_period_end) DO UPDATE SET
        be_activity_count    = :be_activity_count,
        be_distance          = :be_distance,
        be_prime_amount      = :be_prime_amount,
        be_well_being_days   = :be_well_being_days,
        be_flg_prime         = :be_flg_prime,
        be_flg_well_being    = :be_flg_well_being,
        be_declaration_valid = :be_declaration_valid,
        be_updated_at        = NOW()
""")


def upsert_benefits(df: pd.DataFrame, period_start: date, period_end: date) -> None:
    logger.info("Step 5 — Upserting into employee_benefits")

    with get_session() as session:
        for _, row in df.iterrows():
            session.execute(UPSERT_QUERY, {
                "be_employee_id":      row["rh_employee_id"],
                "be_period_start":     period_start,
                "be_period_end":       period_end,
                "be_activity_count":   int(row["activity_count"]),
                "be_distance":         float(row["total_distance"]),
                "be_prime_amount":     float(row["be_prime_amount"]),
                "be_well_being_days":  int(row["be_well_being_days"]),
                "be_flg_prime":        bool(row["be_flg_prime"]),
                "be_flg_well_being":   bool(row["be_flg_well_being"]),
                "be_declaration_valid": bool(row["be_declaration_valid"]),
            })

        session.commit()

    logger.info(f"Upserted {len(df)} benefit records")


# ============================================================
# MAIN — Orchestrates all 5 steps.
# ============================================================
def main():
    logger.info("=" * 60)
    logger.info("BENEFIT PIPELINE — Starting")
    logger.info("=" * 60)

    rules = load_rules()
    period_start, period_end = get_period()
    df = load_employee_data(period_start, period_end)
    df = compute_eligibility(df, rules)
    upsert_benefits(df, period_start, period_end)

    logger.info("=" * 60)
    logger.info("BENEFIT PIPELINE — Complete")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()