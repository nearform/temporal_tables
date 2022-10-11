CREATE TABLE versioning (a bigint, b bigint, sys_period tstzrange);

-- Insert some data before versioning is enabled.
INSERT INTO versioning (a, b, sys_period) VALUES (2, 2, tstzrange('2000-01-01', NULL));

CREATE TABLE versioning_history (b bigint, sys_period tstzrange);

CREATE TRIGGER versioning_trigger
BEFORE INSERT OR UPDATE OR DELETE ON versioning
FOR EACH ROW EXECUTE PROCEDURE versioning('sys_period', 'versioning_history', false, true);

-- Update with no changes that would affect history
BEGIN;

UPDATE versioning SET a = 3;

SELECT a, b FROM versioning ORDER BY a, sys_period;

SELECT b, upper(sys_period) = CURRENT_TIMESTAMP FROM versioning_history ORDER BY b, sys_period;

SELECT a, b FROM versioning WHERE lower(sys_period) = CURRENT_TIMESTAMP ORDER BY a, sys_period;

COMMIT;

-- Make sure that the next transaction's CURRENT_TIMESTAMP is different.
SELECT pg_sleep(0.1);

-- Update with changes that would affect history.
BEGIN;

UPDATE versioning SET b = 3;

SELECT a, b, lower(sys_period) = CURRENT_TIMESTAMP FROM versioning ORDER BY a, sys_period;

SELECT b, upper(sys_period) = CURRENT_TIMESTAMP FROM versioning_history ORDER BY b, sys_period;

SELECT a, b FROM versioning WHERE lower(sys_period) = CURRENT_TIMESTAMP ORDER BY a, sys_period;

COMMIT;

DROP TABLE versioning;
DROP TABLE versioning_history;