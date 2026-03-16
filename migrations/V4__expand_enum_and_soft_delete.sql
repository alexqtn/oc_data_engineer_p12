ALTER TYPE activity_type ADD VALUE IF NOT EXISTS 'racket_sports';
ALTER TYPE activity_type ADD VALUE IF NOT EXISTS 'combat_sports';
ALTER TYPE activity_type ADD VALUE IF NOT EXISTS 'team_sports';
ALTER TYPE activity_type ADD VALUE IF NOT EXISTS 'outdoor_sports';

BEGIN;

ALTER TABLE employees
    ADD COLUMN rh_is_active BOOLEAN NOT NULL DEFAULT TRUE;

ALTER TABLE sport_activities
    ADD COLUMN sp_is_active BOOLEAN NOT NULL DEFAULT TRUE;

COMMIT;

