-- ============================================================
-- V6__fix_bu_length.sql
-- Increase rh_bu from VARCHAR(5) to VARCHAR(50)
-- Original length too short for actual BU names
-- ============================================================

ALTER TABLE employees
ALTER COLUMN rh_bu TYPE VARCHAR(50);