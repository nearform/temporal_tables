-- Rollback Behavior Test
CREATE TABLE versioning_rollback (a bigint, "b b" date, sys_period tstzrange);

-- Insert initial data.
INSERT INTO versioning_rollback (a, "b b", sys_period) VALUES (1, '2020-01-01', tstzrange('2000-01-01', NULL));

CREATE TABLE versioning_rollback_history (a bigint, "b b" date, sys_period tstzrange);

CREATE TRIGGER versioning_rollback_trigger
BEFORE INSERT OR UPDATE OR DELETE ON versioning_rollback
FOR EACH ROW EXECUTE PROCEDURE versioning('sys_period', 'versioning_rollback_history', false, false, true);

-- Test rollback during update.
BEGIN;

UPDATE versioning_rollback SET a = 2 WHERE a = 1;

-- Rollback the transaction.
ROLLBACK;

-- Ensure no history record was created.
SELECT * FROM versioning_rollback_history;  -- Expecting 0 rows in history.

-- Ensure original data is unchanged.
SELECT * FROM versioning_rollback;  -- Expecting original value a = 1.

-- Cleanup
DROP TABLE versioning_rollback;
DROP TABLE versioning_rollback_history;
