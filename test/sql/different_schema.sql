CREATE SCHEMA IF NOT EXISTS other_schema;

CREATE TABLE other_schema.versioning (a bigint, "b b" date, sys_period tstzrange);

-- Insert some data before versioning is enabled.
INSERT INTO other_schema.versioning (a, sys_period) VALUES (1, tstzrange('-infinity', NULL));
INSERT INTO other_schema.versioning (a, sys_period) VALUES (2, tstzrange('2000-01-01', NULL));

CREATE TABLE other_schema.versioning_history (a bigint, c date, sys_period tstzrange);

CREATE TRIGGER versioning_trigger
BEFORE INSERT OR UPDATE OR DELETE ON other_schema.versioning
FOR EACH ROW EXECUTE PROCEDURE versioning('sys_period', 'other_schema.versioning_history', false);

-- Insert.
BEGIN;

INSERT INTO other_schema.versioning (a) VALUES (3);

SELECT a, "b b", lower(sys_period) = CURRENT_TIMESTAMP FROM other_schema.versioning ORDER BY a, sys_period;

SELECT * FROM other_schema.versioning_history ORDER BY a, sys_period;

COMMIT;

-- Make sure that the next transaction's CURRENT_TIMESTAMP is different.
SELECT pg_sleep(0.1);

-- Update.
BEGIN;

UPDATE other_schema.versioning SET a = 4 WHERE a = 3;

SELECT a, "b b", lower(sys_period) = CURRENT_TIMESTAMP FROM other_schema.versioning ORDER BY a, sys_period;

SELECT a, c, upper(sys_period) = CURRENT_TIMESTAMP FROM other_schema.versioning_history ORDER BY a, sys_period;

SELECT a, "b b" FROM other_schema.versioning WHERE lower(sys_period) = CURRENT_TIMESTAMP ORDER BY a, sys_period;

COMMIT;

-- Make sure that the next transaction's CURRENT_TIMESTAMP is different.
SELECT pg_sleep(0.1);

-- Multiple updates.
BEGIN;

UPDATE other_schema.versioning SET a = 5 WHERE a = 4;
UPDATE other_schema.versioning SET "b b" = '2012-01-01' WHERE a = 5;

SELECT a, "b b", lower(sys_period) = CURRENT_TIMESTAMP FROM other_schema.versioning ORDER BY a, sys_period;

SELECT a, c, upper(sys_period) = CURRENT_TIMESTAMP FROM other_schema.versioning_history ORDER BY a, sys_period;

SELECT a, "b b" FROM other_schema.versioning WHERE lower(sys_period) = CURRENT_TIMESTAMP ORDER BY a, sys_period;

COMMIT;

-- Make sure that the next transaction's CURRENT_TIMESTAMP is different.
SELECT pg_sleep(0.1);

-- Delete.
BEGIN;

DELETE FROM other_schema.versioning WHERE a = 4;

SELECT a, "b b", lower(sys_period) = CURRENT_TIMESTAMP FROM other_schema.versioning ORDER BY a, sys_period;

SELECT a, c, upper(sys_period) = CURRENT_TIMESTAMP FROM other_schema.versioning_history ORDER BY a, sys_period;

SELECT a, "b b" FROM other_schema.versioning WHERE lower(sys_period) = CURRENT_TIMESTAMP ORDER BY a, sys_period;

END;

-- Make sure that the next transaction's CURRENT_TIMESTAMP is different.
SELECT pg_sleep(0.1);

-- Delete.
BEGIN;

DELETE FROM other_schema.versioning;

SELECT * FROM other_schema.versioning;

SELECT a, c, upper(sys_period) = CURRENT_TIMESTAMP FROM other_schema.versioning_history ORDER BY a, sys_period;

SELECT a, "b b" FROM other_schema.versioning WHERE lower(sys_period) = CURRENT_TIMESTAMP ORDER BY a, sys_period;

END;

DROP TABLE other_schema.versioning;
DROP TABLE other_schema.versioning_history;
DROP SCHEMA other_schema;