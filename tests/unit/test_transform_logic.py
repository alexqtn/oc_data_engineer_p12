# ============================================================
# test_transform_logic.py — Unit tests for HR transformation logic.
# Tests transport mode mapping and transform function output
# without any database or external dependency.
# ============================================================

import pytest
from src.pipelines.load_employees import (
    TRANSPORT_MODE_MAPPING,
    transform,
)


# ============================================================
# TRANSPORT MODE MAPPING TESTS
# Verifies every French label maps to the correct ENUM value.
# ============================================================
class TestTransportModeMapping:

    def test_walking_mode(self):
        assert TRANSPORT_MODE_MAPPING["marche/running"] == "walking"

    def test_cycling_mode(self):
        assert TRANSPORT_MODE_MAPPING["vélo/trottinette/autres"] == "cycling"

    def test_motorized_car(self):
        assert TRANSPORT_MODE_MAPPING["véhicule thermique/électrique"] == "motorized"

    def test_motorized_transit(self):
        assert TRANSPORT_MODE_MAPPING["transports en commun"] == "motorized"

    def test_all_modes_covered(self):
        expected_keys = {
            "marche/running",
            "vélo/trottinette/autres",
            "véhicule thermique/électrique",
            "transports en commun",
        }
        assert set(TRANSPORT_MODE_MAPPING.keys()) == expected_keys

    def test_no_unmapped_values(self):
        valid_enums = {"walking", "cycling", "motorized"}
        for key, value in TRANSPORT_MODE_MAPPING.items():
            assert value in valid_enums, f"{key} maps to invalid ENUM: {value}"


# ============================================================
# TRANSFORM FUNCTION TESTS
# Verifies transform() produces correct database-ready dicts.
# Uses a minimal fake employee — no database, no API calls.
# ============================================================
class TestTransformFunction:

    @pytest.fixture
    def valid_employee(self):
        return {
            "employee_id": 12345,
            "last_name": "Dupont",
            "first_name": "Jean",
            "birth_date": "1990-01-15",
            "bu": "Marketing",
            "hire_date": "2020-06-01",
            "gross_salary": 45000,
            "contract_type": "CDI",
            "cp_days": 25,
            "transport_mode": "Marche/running",
            "home_address": "10 Rue de la Paix, 34000 Montpellier",
            "sport": "Tennis",
        }

    def test_transform_renames_columns(self, valid_employee):
        result = transform([valid_employee])
        assert len(result) == 1
        emp = result[0]
        assert emp["rh_employee_id"] == 12345
        assert emp["rh_last_name"] == "Dupont"
        assert emp["rh_first_name"] == "Jean"
        assert emp["rh_bu"] == "Marketing"

    def test_transform_maps_transport_mode(self, valid_employee):
        result = transform([valid_employee])
        assert result[0]["rh_transport_mode"] == "walking"

    def test_transform_cycling_mode(self, valid_employee):
        valid_employee["transport_mode"] = "Vélo/Trottinette/Autres"
        result = transform([valid_employee])
        assert result[0]["rh_transport_mode"] == "cycling"

    def test_transform_motorized_mode(self, valid_employee):
        valid_employee["transport_mode"] = "véhicule thermique/électrique"
        result = transform([valid_employee])
        assert result[0]["rh_transport_mode"] == "motorized"

    def test_transform_sets_active_true(self, valid_employee):
        result = transform([valid_employee])
        assert result[0]["rh_is_active"] is True

    def test_transform_preserves_sport(self, valid_employee):
        result = transform([valid_employee])
        assert result[0]["rh_sport"] == "Tennis"

    def test_transform_handles_null_sport(self, valid_employee):
        valid_employee["sport"] = None
        result = transform([valid_employee])
        assert result[0]["rh_sport"] is None

    def test_transform_skips_unknown_mode(self, valid_employee):
        valid_employee["transport_mode"] = "helicoptere"
        result = transform([valid_employee])
        assert len(result) == 0

    def test_transform_case_insensitive(self, valid_employee):
        valid_employee["transport_mode"] = "MARCHE/RUNNING"
        result = transform([valid_employee])
        assert len(result) == 1
        assert result[0]["rh_transport_mode"] == "walking"

    def test_transform_salary_as_string(self, valid_employee):
        result = transform([valid_employee])
        assert isinstance(result[0]["rh_gross_salary"], str)
        assert result[0]["rh_gross_salary"] == "45000"