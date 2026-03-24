-- ============================================================
-- V9__make_rules_id_nullable.sql
-- Benefit computation uses multiple rules simultaneously.
-- No single rule_id represents the full calculation.
-- ============================================================

ALTER TABLE employee_benefits
ALTER COLUMN be_rules_id DROP NOT NULL;