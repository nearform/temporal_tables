-- No-op Update Test
CREATE TABLE versioning_noop (a bigint, "b b" date, sys_period tstzrange);

-- Insert initial data.
INSERT INTO versioning_noop (a, "b b", sys_period) VALUES (1, '2020-01-01', tstzrange('2000-01-01', NULL));

CREATE TABLE versioning_noop_history (a bigint, "b b" date, sys_period tstzrange);

CREATE TRIGGER versioning_noop_trigger
BEFORE INSERT OR UPDATE OR DELETE ON versioning_noop
FOR EACH ROW EXECUTE PROCEDURE versioning('sys_period', 'versioning_noop_history', false, true, true);

-- Test no-op update (the row is updated without any value changes).
BEGIN;

UPDATE versioning_noop SET a = 1, "b b" = '2020-01-01' WHERE a = 1;

-- Check that no history record was created.
SELECT * FROM versioning_noop_history;  -- Expecting 0 rows in history.

COMMIT;

-- Cleanup
DROP TABLE versioning_noop;
DROP TABLE versioning_noop_history;
