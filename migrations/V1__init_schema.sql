-- ==========================================================
-- v1__init_schema.sql
-- Initial schema for Sport Data Solution
-- ==========================================================

-- ==========================================================
-- EXTENSIONS
-- ==========================================================

BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- =========================================================
-- ENUM TYPES
-- =========================================================

CREATE TYPE transport_mode AS ENUM(
    'walking',
    'cycling',
    'motorized'
);

CREATE TYPE activity_type as ENUM(
    'running',
    'walking',
    'cycling',
    'hiking',
    'swimming',
    'other'
);

CREATE TYPE data_source AS ENUM(
    'simulated',
    'strava'
);


-- =====================================================
-- CREATE BENEFIT RULES TABLES
-- =====================================================

CREATE TABLE benefit_rules(
    ru_id SERIAL PRIMARY KEY,
    ru_name VARCHAR(100) NOT NULL,
    ru_activity VARCHAR(100) NULL,
    ru_metrics VARCHAR(50) NOT NULL,
    ru_value NUMERIC(10, 4) NOT NULL,
    ru_benefit VARCHAR(50) NOT NULL,
    ru_effective_date TIMESTAMP NOT NULL,
    ru_created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_ru_name_effective
    ON benefit_rules(ru_name, ru_effective_date DESC);

-- =====================================================
-- CREATE EMPLOYEES TABLES
-- =====================================================

CREATE TABLE employees (
    rh_employee_id VARCHAR(50) PRIMARY KEY,
    rh_last_name BYTEA NOT NULL,
    rh_first_name BYTEA NOT NULL,
    rh_birth_date BYTEA NOT NULL,
    rh_bu VARCHAR(5) NOT NULL,
    rh_hire_date DATE NOT NULL,
    rh_gross_salary BYTEA NOT NULL,
    rh_contract_type VARCHAR(50) NOT NULL,
    rh_cp_days INTEGER NOT NULL,
    rh_street_number BYTEA NOT NULL,
    rh_street_name BYTEA NOT NULL,
    rh_postal_code BYTEA NOT NULL,
    rh_city BYTEA NOT NULL,
    rh_transport_mode transport_mode NOT NULL,
    rh_created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    rh_updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_rh_transport_mode
    ON employees(rh_transport_mode);

-- =====================================================
-- CREATE SPORT ACTIVITIES TABLES
-- =====================================================

CREATE TABLE sport_activities (
    sp_activity_id serial PRIMARY KEY,
    sp_employee_id VARCHAR(50) NOT NULL,
    sp_activity_type activity_type NOT NULL,
    sp_start_date TIMESTAMP NOT NULL,
    sp_elapsed_time INTEGER NOT NULL,
    sp_distance NUMERIC(10, 2) NULL,
    sp_avg_speed NUMERIC(6, 3) NULL,
    sp_max_speed NUMERIC(6, 3) NULL,
    sp_climb NUMERIC(8, 2) NULL,
    sp_comment TEXT NULL,
    sp_data_source data_source NOT NULL DEFAULT 'simulated',
    sp_created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    sp_updated_at TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_sp_employee
        FOREIGN KEY (sp_employee_id)
        REFERENCES employees(rh_employee_id)
        ON DELETE RESTRICT
);

CREATE INDEX idx_sp_employee_id
    ON sport_activities(sp_employee_id);

CREATE INDEX idx_sp_start_date
    ON sport_activities(sp_start_date DESC);

CREATE INDEX idx_sp_employee_date
    ON sport_activities(sp_employee_id, sp_start_date DESC);

CREATE TABLE employee_benefits (
    be_benefit_id SERIAL PRIMARY KEY,
    be_employee_id VARCHAR(50) NOT NULL,
    be_rules_id INTEGER NOT NULL,
    be_period_start DATE NOT NULL,
    be_period_end DATE NOT NULL,
    be_activity_count INTEGER NOT NULL DEFAULT 0,
    be_distance NUMERIC(10, 2) NULL,
    be_prime_amount NUMERIC(10, 2) NULL,
    be_well_being_days INTEGER NOT NULL DEFAULT 0,
    be_flg_prime BOOLEAN NOT NULL DEFAULT FALSE,
    be_flg_well_being BOOLEAN NOT NULL DEFAULT FALSE,
    be_created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    be_updated_at TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_be_employee
        FOREIGN KEY (be_employee_id)
        REFERENCES employees(rh_employee_id)
        ON DELETE RESTRICT,
    
    CONSTRAINT fk_be_rules
        FOREIGN KEY (be_rules_id)
        REFERENCES benefit_rules(ru_id)
        ON DELETE RESTRICT,
    
    CONSTRAINT fk_uq_be_employee_period
        UNIQUE (be_employee_id, be_period_start, be_period_end)
);

CREATE INDEX idx_be_employee_id
    ON employee_benefits(be_employee_id);

CREATE INDEX idx_be_period
    ON employee_benefits(be_period_start, be_period_end);

CREATE INDEX idx_be_flags
    ON employee_benefits(be_flg_prime, be_flg_well_being);

COMMIT;