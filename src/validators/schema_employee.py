from pydantic import BaseModel, field_validator, model_validator
from datetime import date, datetime
from typing import Optional


NULL_LIKE_VALUES = {
    'n/a', 'na', '-', '?', '', 'none', 'null', 'inconnu'
}

VALID_TRANSPORT_MODES = {
    'marche/running',
    'vélo/trottinette/autres',
    'véhicule thermique/électrique',
    'transports en commun'
}

VALID_CONTRACT_TYPES = {'cdi', 'cdd'}


class EmployeeSchema(BaseModel):

    employee_id:    str
    last_name:      str
    first_name:     str
    birth_date:     date
    bu:             str
    hire_date:      date
    gross_salary:   float
    contract_type:  str
    cp_days:        int
    home_address:   str
    transport_mode: str

    model_config = {
        'str_strip_whitespace': True,
        'extra': 'forbid'
    }

    @field_validator('employee_id', mode='before')
    @classmethod
    def coerce_id_to_string(cls, v):
        if v is None:
            raise ValueError('employee_id is required')
        return str(int(v))

    @field_validator('birth_date', 'hire_date', mode='before')
    @classmethod
    def parse_date(cls, v):
        if isinstance(v, datetime):
            return v.date()
        if isinstance(v, date):
            return v
        raise ValueError(f'invalid date format: {v}')

    @field_validator('gross_salary', mode='before')
    @classmethod
    def validate_salary(cls, v):
        try:
            salary = float(v)
        except (TypeError, ValueError):
            raise ValueError(f'salary must be numeric, got: {v}')
        if salary <= 0:
            raise ValueError(f'salary must be positive, got: {salary}')
        return salary

    @field_validator('contract_type', mode='before')
    @classmethod
    def validate_contract_type(cls, v):
        normalized = str(v).strip().lower()
        if normalized not in VALID_CONTRACT_TYPES:
            raise ValueError(
                f'unknown contract type: {v}. '
                f'Expected one of: {VALID_CONTRACT_TYPES}'
            )
        return str(v).strip()

    @field_validator('cp_days', mode='before')
    @classmethod
    def validate_cp_days(cls, v):
        try:
            days = int(v)
        except (TypeError, ValueError):
            raise ValueError(f'cp_days must be integer, got: {v}')
        if not 25 <= days <= 29:
            raise ValueError(
                f'cp_days must be between 25 and 29, got: {days}'
            )
        return days

    @field_validator('home_address', mode='before')
    @classmethod
    def validate_address(cls, v):
        if v is None:
            raise ValueError('home_address is required')
        cleaned = str(v).strip()
        if cleaned.lower() in NULL_LIKE_VALUES:
            raise ValueError(f'home_address looks null-like: {v}')
        if len(cleaned) < 5:
            raise ValueError(f'home_address too short: {v}')
        return cleaned

    @field_validator('transport_mode', mode='before')
    @classmethod
    def validate_transport_mode(cls, v):
        if v is None:
            raise ValueError('transport_mode is required')
        normalized = str(v).strip().lower()
        if normalized not in VALID_TRANSPORT_MODES:
            raise ValueError(
                f'unknown transport mode: {v}. '
                f'Expected one of: {VALID_TRANSPORT_MODES}'
            )
        return str(v).strip()

    @field_validator('last_name', 'first_name', 'bu', mode='before')
    @classmethod
    def validate_not_empty(cls, v):
        if v is None:
            raise ValueError('field is required')
        cleaned = str(v).strip()
        if not cleaned:
            raise ValueError('field cannot be empty')
        return cleaned

    @model_validator(mode='after')
    def validate_hire_after_birth(self):
        if self.hire_date <= self.birth_date:
            raise ValueError(
                f'hire_date {self.hire_date} must be after '
                f'birth_date {self.birth_date}'
            )
        age_at_hire = (self.hire_date - self.birth_date).days / 365.25
        if age_at_hire < 16:
            raise ValueError(
                f'employee was {age_at_hire:.1f} years old at hire date '
                f'— too young to be legally employed'
            )
        return self