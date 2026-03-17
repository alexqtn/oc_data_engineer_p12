"""
schema_activity.py
------------------
Pydantic validation schema for generated sport activity data.

Purpose:
    Validation gate for the activity generator output.
    Ensures every generated activity is physically coherent
    and compatible with the sport_activities database schema
    before insertion into PostgreSQL and publication to Redpanda.

Validates:
    - Employee ID presence
    - Activity type against 10 ENUM categories
    - Start date not in the future
    - Elapsed time: positive, max 24 hours
    - Optional metrics: distance, speed, climb (positive if present)
    - Data source against allowed values
    - Cross-field: max_speed >= avg_speed (physically impossible otherwise)

Usage:
    from src.validators.schema_activity import ActivitySchema
    validated = ActivitySchema(**activity_dict)
"""


from pydantic import BaseModel, field_validator, model_validator
from datetime import datetime
from typing import Optional


VALID_ACTIVITY_TYPES = {
    'running',
    'walking',
    'cycling',
    'hiking',
    'swimming',
    'racket_sports',
    'combat_sports',
    'team_sports',
    'outdoor_sports',
    'other'
}

VALID_DATA_SOURCES = {'simulated', 'strava'}


class ActivitySchema(BaseModel):

    employee_id:    str
    activity_type:  str
    start_date:     datetime
    elapsed_time:   int
    distance:       Optional[float] = None
    avg_speed:      Optional[float] = None
    max_speed:      Optional[float] = None
    climb:          Optional[float] = None
    comment:        Optional[str]   = None
    data_source:    str             = 'simulated'

    model_config = {
        'str_strip_whitespace': True,
        'extra': 'forbid'
    }

    @field_validator('employee_id', mode='before')
    @classmethod
    def validate_employee_id(cls, v):
        if v is None:
            raise ValueError('employee_id is required')
        cleaned = str(v).strip()
        if not cleaned:
            raise ValueError('employee_id cannot be empty')
        return cleaned

    @field_validator('activity_type', mode='before')
    @classmethod
    def validate_activity_type(cls, v):
        if v is None:
            raise ValueError('activity_type is required')
        normalized = str(v).strip().lower()
        if normalized not in VALID_ACTIVITY_TYPES:
            raise ValueError(
                f'unknown activity type: {v}. '
                f'Expected one of: {VALID_ACTIVITY_TYPES}'
            )
        return normalized

    @field_validator('start_date', mode='before')
    @classmethod
    def validate_start_date(cls, v):
        if isinstance(v, datetime):
            if v > datetime.now():
                raise ValueError(
                    f'start_date cannot be in the future: {v}'
                )
            return v
        raise ValueError(f'invalid datetime format: {v}')

    @field_validator('elapsed_time', mode='before')
    @classmethod
    def validate_elapsed_time(cls, v):
        try:
            seconds = int(v)
        except (TypeError, ValueError):
            raise ValueError(
                f'elapsed_time must be integer seconds, got: {v}'
            )
        if seconds <= 0:
            raise ValueError(
                f'elapsed_time must be positive, got: {seconds}'
            )
        if seconds > 86400:
            raise ValueError(
                f'elapsed_time exceeds 24 hours: {seconds}s '
                f'— likely a data error'
            )
        return seconds

    @field_validator('distance', mode='before')
    @classmethod
    def validate_distance(cls, v):
        if v is None:
            return None
        try:
            dist = float(v)
        except (TypeError, ValueError):
            raise ValueError(f'distance must be numeric, got: {v}')
        if dist < 0:
            raise ValueError(f'distance cannot be negative, got: {dist}')
        return dist

    @field_validator('avg_speed', 'max_speed', mode='before')
    @classmethod
    def validate_speed(cls, v):
        if v is None:
            return None
        try:
            speed = float(v)
        except (TypeError, ValueError):
            raise ValueError(f'speed must be numeric, got: {v}')
        if speed < 0:
            raise ValueError(f'speed cannot be negative, got: {speed}')
        return speed

    @field_validator('climb', mode='before')
    @classmethod
    def validate_climb(cls, v):
        if v is None:
            return None
        try:
            elevation = float(v)
        except (TypeError, ValueError):
            raise ValueError(f'climb must be numeric, got: {v}')
        if elevation < 0:
            raise ValueError(
                f'climb cannot be negative, got: {elevation}'
            )
        return elevation

    @field_validator('data_source', mode='before')
    @classmethod
    def validate_data_source(cls, v):
        normalized = str(v).strip().lower()
        if normalized not in VALID_DATA_SOURCES:
            raise ValueError(
                f'unknown data source: {v}. '
                f'Expected one of: {VALID_DATA_SOURCES}'
            )
        return normalized

    @model_validator(mode='after')
    def validate_speed_consistency(self):
        if self.max_speed is not None and self.avg_speed is not None:
            if self.max_speed < self.avg_speed:
                raise ValueError(
                    f'max_speed ({self.max_speed}) cannot be lower '
                    f'than avg_speed ({self.avg_speed}) '
                    f'— physically impossible'
                )
        return self