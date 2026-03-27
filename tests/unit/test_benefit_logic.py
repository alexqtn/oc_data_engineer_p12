# ============================================================
# test_benefit_logic.py — Unit tests for benefit computation logic.
# Tests prime and well-being eligibility rules
# without any database or external dependency.
# ============================================================

import pytest
import pandas as pd
from src.pipelines.compute_benefits import compute_eligibility


# ============================================================
# FIXTURES
# Creates test DataFrames mimicking what load_employee_data() returns.
# ============================================================
@pytest.fixture
def default_rules():
    return {
        "prime_rate": 0.05,
        "min_activities": 15,
        "well_being_days": 5,
    }


@pytest.fixture
def make_employee():
    """Factory fixture — call it with overrides to create custom employees."""
    def _make(
        employee_id=1,
        gross_salary=40000.0,
        transport_mode="walking",
        declaration_valid=True,
        activity_count=20,
        total_distance=5000.0,
    ):
        return {
            "rh_employee_id": employee_id,
            "gross_salary": gross_salary,
            "rh_transport_mode": transport_mode,
            "be_declaration_valid": declaration_valid,
            "activity_count": activity_count,
            "total_distance": total_distance,
        }
    return _make


def build_df(employees: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(employees)


# ============================================================
# PRIME ELIGIBILITY TESTS
# Prime requires: walking/cycling + valid declaration.
# ============================================================
class TestPrimeEligibility:

    def test_walking_valid_gets_prime(self, default_rules, make_employee):
        df = build_df([make_employee(transport_mode="walking", declaration_valid=True)])
        result = compute_eligibility(df, default_rules)
        assert result.iloc[0]["be_flg_prime"] == True

    def test_cycling_valid_gets_prime(self, default_rules, make_employee):
        df = build_df([make_employee(transport_mode="cycling", declaration_valid=True)])
        result = compute_eligibility(df, default_rules)
        assert result.iloc[0]["be_flg_prime"] == True

    def test_motorized_no_prime(self, default_rules, make_employee):
        df = build_df([make_employee(transport_mode="motorized", declaration_valid=True)])
        result = compute_eligibility(df, default_rules)
        assert result.iloc[0]["be_flg_prime"] == False

    def test_invalid_declaration_no_prime(self, default_rules, make_employee):
        df = build_df([make_employee(transport_mode="walking", declaration_valid=False)])
        result = compute_eligibility(df, default_rules)
        assert result.iloc[0]["be_flg_prime"] == False

    def test_prime_amount_calculation(self, default_rules, make_employee):
        df = build_df([make_employee(gross_salary=50000.0)])
        result = compute_eligibility(df, default_rules)
        assert result.iloc[0]["be_prime_amount"] == 2500.0

    def test_prime_amount_zero_when_not_eligible(self, default_rules, make_employee):
        df = build_df([make_employee(transport_mode="motorized")])
        result = compute_eligibility(df, default_rules)
        assert result.iloc[0]["be_prime_amount"] == 0

    def test_prime_with_custom_rate(self, make_employee):
        rules = {"prime_rate": 0.07, "min_activities": 15, "well_being_days": 5}
        df = build_df([make_employee(gross_salary=40000.0)])
        result = compute_eligibility(df, rules)
        assert result.iloc[0]["be_prime_amount"] == 2800.0

    def test_prime_amount_rounds_to_two_decimals(self, default_rules, make_employee):
        df = build_df([make_employee(gross_salary=33333.0)])
        result = compute_eligibility(df, default_rules)
        assert result.iloc[0]["be_prime_amount"] == 1666.65


# ============================================================
# WELL-BEING ELIGIBILITY TESTS
# Well-being requires: activity_count >= min_activities.
# ============================================================
class TestWellBeingEligibility:

    def test_enough_activities_gets_wellbeing(self, default_rules, make_employee):
        df = build_df([make_employee(activity_count=20)])
        result = compute_eligibility(df, default_rules)
        assert result.iloc[0]["be_flg_well_being"] == True

    def test_exact_threshold_gets_wellbeing(self, default_rules, make_employee):
        df = build_df([make_employee(activity_count=15)])
        result = compute_eligibility(df, default_rules)
        assert result.iloc[0]["be_flg_well_being"] == True

    def test_below_threshold_no_wellbeing(self, default_rules, make_employee):
        df = build_df([make_employee(activity_count=14)])
        result = compute_eligibility(df, default_rules)
        assert result.iloc[0]["be_flg_well_being"] == False

    def test_zero_activities_no_wellbeing(self, default_rules, make_employee):
        df = build_df([make_employee(activity_count=0)])
        result = compute_eligibility(df, default_rules)
        assert result.iloc[0]["be_flg_well_being"] == False

    def test_wellbeing_days_when_eligible(self, default_rules, make_employee):
        df = build_df([make_employee(activity_count=20)])
        result = compute_eligibility(df, default_rules)
        assert result.iloc[0]["be_well_being_days"] == 5

    def test_wellbeing_days_zero_when_not_eligible(self, default_rules, make_employee):
        df = build_df([make_employee(activity_count=5)])
        result = compute_eligibility(df, default_rules)
        assert result.iloc[0]["be_well_being_days"] == 0

    def test_custom_min_activities(self, make_employee):
        rules = {"prime_rate": 0.05, "min_activities": 10, "well_being_days": 5}
        df = build_df([make_employee(activity_count=12)])
        result = compute_eligibility(df, rules)
        assert result.iloc[0]["be_flg_well_being"] == True

    def test_custom_wellbeing_days(self, make_employee):
        rules = {"prime_rate": 0.05, "min_activities": 15, "well_being_days": 7}
        df = build_df([make_employee(activity_count=20)])
        result = compute_eligibility(df, rules)
        assert result.iloc[0]["be_well_being_days"] == 7


# ============================================================
# COMBINED SCENARIOS — Tests with multiple employees.
# Verifies batch computation and edge cases together.
# ============================================================
class TestCombinedScenarios:

    def test_both_benefits(self, default_rules, make_employee):
        df = build_df([make_employee(
            transport_mode="cycling",
            declaration_valid=True,
            activity_count=20,
        )])
        result = compute_eligibility(df, default_rules)
        assert result.iloc[0]["be_flg_prime"] == True
        assert result.iloc[0]["be_flg_well_being"] == True

    def test_prime_only(self, default_rules, make_employee):
        df = build_df([make_employee(
            transport_mode="walking",
            declaration_valid=True,
            activity_count=5,
        )])
        result = compute_eligibility(df, default_rules)
        assert result.iloc[0]["be_flg_prime"] == True
        assert result.iloc[0]["be_flg_well_being"] == False

    def test_wellbeing_only(self, default_rules, make_employee):
        df = build_df([make_employee(
            transport_mode="motorized",
            activity_count=30,
        )])
        result = compute_eligibility(df, default_rules)
        assert result.iloc[0]["be_flg_prime"] == False
        assert result.iloc[0]["be_flg_well_being"] == True

    def test_no_benefits(self, default_rules, make_employee):
        df = build_df([make_employee(
            transport_mode="motorized",
            activity_count=5,
        )])
        result = compute_eligibility(df, default_rules)
        assert result.iloc[0]["be_flg_prime"] == False
        assert result.iloc[0]["be_flg_well_being"] == False

    def test_multiple_employees(self, default_rules, make_employee):
        df = build_df([
            make_employee(employee_id=1, transport_mode="walking", activity_count=20),
            make_employee(employee_id=2, transport_mode="motorized", activity_count=30),
            make_employee(employee_id=3, transport_mode="cycling", activity_count=5),
            make_employee(employee_id=4, transport_mode="motorized", activity_count=3),
        ])
        result = compute_eligibility(df, default_rules)
        assert result["be_flg_prime"].sum() == 2
        assert result["be_flg_well_being"].sum() == 2

    def test_total_prime_cost(self, default_rules, make_employee):
        df = build_df([
            make_employee(employee_id=1, gross_salary=40000.0, transport_mode="walking"),
            make_employee(employee_id=2, gross_salary=60000.0, transport_mode="cycling"),
            make_employee(employee_id=3, gross_salary=50000.0, transport_mode="motorized"),
        ])
        result = compute_eligibility(df, default_rules)
        total = result["be_prime_amount"].sum()
        assert total == 5000.0