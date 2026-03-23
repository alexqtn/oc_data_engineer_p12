-- ============================================================
-- V8__add_unique_activity_constraint.sql
-- Prevents duplicate activities for same employee at same time.
-- Enables UPSERT in consumer_postgres.py for safe re-processing.
-- ============================================================

ALTER TABLE sport_activities
ADD CONSTRAINT uq_employee_start_date
UNIQUE (sp_employee_id, sp_start_date);