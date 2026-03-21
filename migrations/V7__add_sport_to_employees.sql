-- ============================================================
-- V7__add_sport_to_employees.sql
-- Stores employee declared sport from DonneesSportive.xlsx.
-- Used by activity generator to know what sport to simulate.
-- Not PII — no encryption needed.
-- ============================================================

ALTER TABLE employees
ADD COLUMN rh_sport VARCHAR(50) DEFAULT NULL;