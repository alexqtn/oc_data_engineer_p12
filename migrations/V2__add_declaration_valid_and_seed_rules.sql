BEGIN;

ALTER TABLE employee_benefits
    ADD COLUMN be_declaration_valid BOOLEAN NOT NULL DEFAULT TRUE;

INSERT INTO benefit_rules
    (ru_name, ru_activity, ru_metrics, ru_value, ru_benefit, ru_effective_date)
VALUES
    ('prime_rate', NULL, 'percentage', 0.005, 'prime', '2026-01-01'),
    ('min_activities', NULL, 'count', 15, 'well_being', '2026-01-01'),
    ('well_being_days', NULL, 'days', 5, 'well_being', '2026-01-01'),
    ('validation_distance_walking', 'walking', 'km', 15, 'prime', '2026-01-01'),
    ('validation_distance_cycling', 'cycling', 'km', 25, 'prime', '2026-01-01');

COMMIT;