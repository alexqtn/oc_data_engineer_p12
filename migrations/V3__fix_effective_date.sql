BEGIN;

UPDATE benefit_rules
    SET ru_effective_date = '2025-01-01'
    WHERE ru_effective_date = '2026-01-01';

COMMIT;