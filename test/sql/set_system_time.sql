CREATE TABLE versioning (a bigint, "b b" date, sys_period tstzrange);

-- Insert some data before versioning is enabled.
INSERT INTO versioning (a, sys_period) VALUES (1, tstzrange('-infinity', NULL));
INSERT INTO versioning (a, sys_period) VALUES (2, tstzrange('2000-01-01', NULL));

CREATE TABLE versioning_history (a bigint, c date, sys_period tstzrange);

CREATE TRIGGER versioning_trigger
BEFORE INSERT OR UPDATE OR DELETE ON versioning
FOR EACH ROW EXECUTE PROCEDURE versioning('sys_period', 'versioning_history', false);

-- Insert.
BEGIN;

SELECT set_system_time('2001-01-01 22:59:59');

INSERT INTO versioning (a) VALUES (3);

SELECT * FROM versioning_history;

COMMIT;

-- Update.
BEGIN;

UPDATE versioning SET a = 4 WHERE a = 3;

SELECT * FROM versioning_history;

COMMIT;

-- Reset system time and do multiple updates.
BEGIN;

UPDATE versioning SET a = 5 WHERE a = 4;
UPDATE versioning SET "b b" = '2012-01-01' WHERE a = 5;

SELECT * FROM versioning_history;

COMMIT;

-- Delete.
BEGIN;

SELECT set_system_time('2022-01-11 12:00:00');

DELETE FROM versioning WHERE a = 4;

SELECT * FROM versioning_history;

END;

-- Delete.
BEGIN;

DELETE FROM versioning;

SELECT * FROM versioning_history;

END;

DROP TABLE versioning;
DROP TABLE versioning_history;