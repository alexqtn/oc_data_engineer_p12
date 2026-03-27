# ============================================================
# test_validators.py — Unit tests for Pydantic validation schemas.
# Tests both EmployeeSchema and ActivitySchema with valid data,
# invalid data, and edge cases. No database needed.
# ============================================================

import pytest
from datetime import date, datetime, timedelta
from src.validators.schema_employee import EmployeeSchema
from src.validators.schema_activity import ActivitySchema


# ============================================================
# EMPLOYEE SCHEMA — VALID DATA
# ============================================================
class TestEmployeeSchemaValid:

    @pytest.fixture
    def valid_employee(self):
        return {
            "employee_id": 12345,
            "last_name": "Dupont",
            "first_name": "Jean",
            "birth_date": date(1990, 5, 15),
            "bu": "Marketing",
            "hire_date": date(2020, 6, 1),
            "gross_salary": 45000,
            "contract_type": "CDI",
            "cp_days": 25,
            "home_address": "10 Rue de la Paix, 34000 Montpellier",
            "transport_mode": "Marche/running",
        }

    def test_valid_employee_passes(self, valid_employee):
        result = EmployeeSchema(**valid_employee)
        assert result.employee_id == "12345"

    def test_employee_id_coerced_to_string(self, valid_employee):
        valid_employee["employee_id"] = 99999
        result = EmployeeSchema(**valid_employee)
        assert isinstance(result.employee_id, str)
        assert result.employee_id == "99999"

    def test_datetime_converted_to_date(self, valid_employee):
        valid_employee["birth_date"] = datetime(1990, 5, 15, 10, 30)
        result = EmployeeSchema(**valid_employee)
        assert result.birth_date == date(1990, 5, 15)

    def test_all_transport_modes_accepted(self, valid_employee):
        modes = [
            "Marche/running",
            "Vélo/Trottinette/Autres",
            "véhicule thermique/électrique",
            "Transports en commun",
        ]
        for mode in modes:
            valid_employee["transport_mode"] = mode
            result = EmployeeSchema(**valid_employee)
            assert result is not None

    def test_both_contract_types_accepted(self, valid_employee):
        for ct in ["CDI", "CDD", "cdi", "cdd"]:
            valid_employee["contract_type"] = ct
            result = EmployeeSchema(**valid_employee)
            assert result is not None

    def test_cp_days_boundary_low(self, valid_employee):
        valid_employee["cp_days"] = 25
        result = EmployeeSchema(**valid_employee)
        assert result.cp_days == 25

    def test_cp_days_boundary_high(self, valid_employee):
        valid_employee["cp_days"] = 29
        result = EmployeeSchema(**valid_employee)
        assert result.cp_days == 29


# ============================================================
# EMPLOYEE SCHEMA — INVALID DATA
# Each test verifies that bad input raises ValidationError.
# ============================================================
class TestEmployeeSchemaInvalid:

    @pytest.fixture
    def valid_employee(self):
        return {
            "employee_id": 12345,
            "last_name": "Dupont",
            "first_name": "Jean",
            "birth_date": date(1990, 5, 15),
            "bu": "Marketing",
            "hire_date": date(2020, 6, 1),
            "gross_salary": 45000,
            "contract_type": "CDI",
            "cp_days": 25,
            "home_address": "10 Rue de la Paix, 34000 Montpellier",
            "transport_mode": "Marche/running",
        }

    def test_null_employee_id_rejected(self, valid_employee):
        valid_employee["employee_id"] = None
        with pytest.raises(Exception):
            EmployeeSchema(**valid_employee)

    def test_negative_salary_rejected(self, valid_employee):
        valid_employee["gross_salary"] = -5000
        with pytest.raises(Exception):
            EmployeeSchema(**valid_employee)

    def test_zero_salary_rejected(self, valid_employee):
        valid_employee["gross_salary"] = 0
        with pytest.raises(Exception):
            EmployeeSchema(**valid_employee)

    def test_unknown_transport_mode_rejected(self, valid_employee):
        valid_employee["transport_mode"] = "helicoptere"
        with pytest.raises(Exception):
            EmployeeSchema(**valid_employee)

    def test_unknown_contract_type_rejected(self, valid_employee):
        valid_employee["contract_type"] = "freelance"
        with pytest.raises(Exception):
            EmployeeSchema(**valid_employee)

    def test_cp_days_too_low_rejected(self, valid_employee):
        valid_employee["cp_days"] = 24
        with pytest.raises(Exception):
            EmployeeSchema(**valid_employee)

    def test_cp_days_too_high_rejected(self, valid_employee):
        valid_employee["cp_days"] = 30
        with pytest.raises(Exception):
            EmployeeSchema(**valid_employee)

    def test_null_like_address_rejected(self, valid_employee):
        valid_employee["home_address"] = "N/A"
        with pytest.raises(Exception):
            EmployeeSchema(**valid_employee)

    def test_short_address_rejected(self, valid_employee):
        valid_employee["home_address"] = "abc"
        with pytest.raises(Exception):
            EmployeeSchema(**valid_employee)

    def test_empty_last_name_rejected(self, valid_employee):
        valid_employee["last_name"] = ""
        with pytest.raises(Exception):
            EmployeeSchema(**valid_employee)

    def test_extra_field_rejected(self, valid_employee):
        valid_employee["unknown_field"] = "something"
        with pytest.raises(Exception):
            EmployeeSchema(**valid_employee)


# ============================================================
# EMPLOYEE SCHEMA — CROSS-FIELD VALIDATION
# Tests the model_validator that checks hire > birth and age >= 16.
# ============================================================
class TestEmployeeCrossField:

    @pytest.fixture
    def valid_employee(self):
        return {
            "employee_id": 12345,
            "last_name": "Dupont",
            "first_name": "Jean",
            "birth_date": date(1990, 5, 15),
            "bu": "Marketing",
            "hire_date": date(2020, 6, 1),
            "gross_salary": 45000,
            "contract_type": "CDI",
            "cp_days": 25,
            "home_address": "10 Rue de la Paix, 34000 Montpellier",
            "transport_mode": "Marche/running",
        }

    def test_hire_before_birth_rejected(self, valid_employee):
        valid_employee["hire_date"] = date(1985, 1, 1)
        valid_employee["birth_date"] = date(1990, 5, 15)
        with pytest.raises(Exception):
            EmployeeSchema(**valid_employee)

    def test_too_young_at_hire_rejected(self, valid_employee):
        valid_employee["birth_date"] = date(2010, 1, 1)
        valid_employee["hire_date"] = date(2020, 6, 1)
        with pytest.raises(Exception):
            EmployeeSchema(**valid_employee)

        valid_employee["birth_date"] = date(2010, 1, 1)
        valid_employee["hire_date"] = date(2020, 6, 1)
        with pytest.raises(Exception):
            EmployeeSchema(**valid_employee)

    def test_exactly_16_accepted(self, valid_employee):
        valid_employee["birth_date"] = date(2004, 6, 1)
        valid_employee["hire_date"] = date(2020, 6, 1)
        result = EmployeeSchema(**valid_employee)
        assert result is not None


# ============================================================
# ACTIVITY SCHEMA — VALID DATA
# ============================================================
class TestActivitySchemaValid:

    @pytest.fixture
    def valid_activity(self):
        return {
            "employee_id": "12345",
            "activity_type": "running",
            "start_date": datetime.now() - timedelta(days=10),
            "elapsed_time": 3600,
            "distance": 10000.0,
            "avg_speed": 2.78,
            "max_speed": 3.50,
            "climb": 150.0,
            "comment": "Morning run",
            "data_source": "simulated",
        }

    def test_valid_activity_passes(self, valid_activity):
        result = ActivitySchema(**valid_activity)
        assert result.employee_id == "12345"

    def test_all_activity_types_accepted(self, valid_activity):
        types = [
            "running", "walking", "cycling", "hiking",
            "swimming", "racket_sports", "combat_sports",
            "team_sports", "outdoor_sports", "other",
        ]
        for t in types:
            valid_activity["activity_type"] = t
            result = ActivitySchema(**valid_activity)
            assert result.activity_type == t

    def test_optional_fields_can_be_none(self, valid_activity):
        valid_activity["distance"] = None
        valid_activity["avg_speed"] = None
        valid_activity["max_speed"] = None
        valid_activity["climb"] = None
        valid_activity["comment"] = None
        result = ActivitySchema(**valid_activity)
        assert result.distance is None

    def test_employee_id_coerced_from_int(self, valid_activity):
        valid_activity["employee_id"] = 99999
        result = ActivitySchema(**valid_activity)
        assert result.employee_id == "99999"


# ============================================================
# ACTIVITY SCHEMA — INVALID DATA
# ============================================================
class TestActivitySchemaInvalid:

    @pytest.fixture
    def valid_activity(self):
        return {
            "employee_id": "12345",
            "activity_type": "running",
            "start_date": datetime.now() - timedelta(days=10),
            "elapsed_time": 3600,
            "distance": 10000.0,
            "avg_speed": 2.78,
            "max_speed": 3.50,
            "data_source": "simulated",
        }

    def test_null_employee_id_rejected(self, valid_activity):
        valid_activity["employee_id"] = None
        with pytest.raises(Exception):
            ActivitySchema(**valid_activity)

    def test_unknown_activity_type_rejected(self, valid_activity):
        valid_activity["activity_type"] = "skydiving"
        with pytest.raises(Exception):
            ActivitySchema(**valid_activity)

    def test_future_date_rejected(self, valid_activity):
        valid_activity["start_date"] = datetime.now() + timedelta(days=30)
        with pytest.raises(Exception):
            ActivitySchema(**valid_activity)

    def test_negative_elapsed_time_rejected(self, valid_activity):
        valid_activity["elapsed_time"] = -100
        with pytest.raises(Exception):
            ActivitySchema(**valid_activity)

    def test_zero_elapsed_time_rejected(self, valid_activity):
        valid_activity["elapsed_time"] = 0
        with pytest.raises(Exception):
            ActivitySchema(**valid_activity)

    def test_exceeds_24h_rejected(self, valid_activity):
        valid_activity["elapsed_time"] = 86401
        with pytest.raises(Exception):
            ActivitySchema(**valid_activity)

    def test_negative_distance_rejected(self, valid_activity):
        valid_activity["distance"] = -500
        with pytest.raises(Exception):
            ActivitySchema(**valid_activity)

    def test_negative_speed_rejected(self, valid_activity):
        valid_activity["avg_speed"] = -1.5
        with pytest.raises(Exception):
            ActivitySchema(**valid_activity)

    def test_unknown_data_source_rejected(self, valid_activity):
        valid_activity["data_source"] = "garmin"
        with pytest.raises(Exception):
            ActivitySchema(**valid_activity)

    def test_extra_field_rejected(self, valid_activity):
        valid_activity["unknown"] = "something"
        with pytest.raises(Exception):
            ActivitySchema(**valid_activity)


# ============================================================
# ACTIVITY SCHEMA — CROSS-FIELD VALIDATION
# Tests max_speed >= avg_speed physical constraint.
# ============================================================
class TestActivityCrossField:

    @pytest.fixture
    def valid_activity(self):
        return {
            "employee_id": "12345",
            "activity_type": "running",
            "start_date": datetime.now() - timedelta(days=10),
            "elapsed_time": 3600,
            "distance": 10000.0,
            "avg_speed": 2.78,
            "max_speed": 3.50,
            "data_source": "simulated",
        }

    def test_max_speed_below_avg_rejected(self, valid_activity):
        valid_activity["avg_speed"] = 5.0
        valid_activity["max_speed"] = 3.0
        with pytest.raises(Exception):
            ActivitySchema(**valid_activity)

    def test_max_speed_equal_avg_accepted(self, valid_activity):
        valid_activity["avg_speed"] = 3.0
        valid_activity["max_speed"] = 3.0
        result = ActivitySchema(**valid_activity)
        assert result.max_speed == result.avg_speed

    def test_max_speed_above_avg_accepted(self, valid_activity):
        valid_activity["avg_speed"] = 2.0
        valid_activity["max_speed"] = 5.0
        result = ActivitySchema(**valid_activity)
        assert result.max_speed > result.avg_speed