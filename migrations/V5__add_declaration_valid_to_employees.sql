-- ============================================================
-- V5__add_declaration_valid_to_employees.sql
-- Adds commute declaration validation flag to employees table.
-- Set by Google Maps distance check during HR pipeline.
-- ============================================================

ALTER TABLE employees
ADD COLUMN be_declaration_valid BOOLEAN DEFAULT TRUE;