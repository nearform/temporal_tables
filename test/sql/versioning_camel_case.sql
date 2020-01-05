CREATE TABLE "Versioning" (a bigint, "b b" date, sys_period tstzrange);

-- Insert some data before versioning is enabled.
INSERT INTO "Versioning" (a, sys_period) VALUES (1, tstzrange('-infinity', NULL));
INSERT INTO "Versioning" (a, sys_period) VALUES (2, tstzrange('2000-01-01', NULL));

CREATE TABLE "VersioningHistory" (a bigint, c date, sys_period tstzrange);

CREATE TRIGGER "VersioningTrigger"
BEFORE INSERT OR UPDATE OR DELETE ON "Versioning"
FOR EACH ROW EXECUTE PROCEDURE versioning('sys_period', '"VersioningHistory"', false);

-- Insert.
BEGIN;

INSERT INTO "Versioning" (a) VALUES (3);

SELECT a, "b b", lower(sys_period) = CURRENT_TIMESTAMP FROM "Versioning" ORDER BY a, sys_period;

SELECT * FROM "VersioningHistory" ORDER BY a, sys_period;

COMMIT;

-- Make sure that the next transaction's CURRENT_TIMESTAMP is different.
SELECT pg_sleep(0.1);

-- Update.
BEGIN;

UPDATE "Versioning" SET a = 4 WHERE a = 3;

SELECT a, "b b", lower(sys_period) = CURRENT_TIMESTAMP FROM "Versioning" ORDER BY a, sys_period;

SELECT a, c, upper(sys_period) = CURRENT_TIMESTAMP FROM "VersioningHistory" ORDER BY a, sys_period;

SELECT a, "b b" FROM "Versioning" WHERE lower(sys_period) = CURRENT_TIMESTAMP ORDER BY a, sys_period;

COMMIT;

-- Make sure that the next transaction's CURRENT_TIMESTAMP is different.
SELECT pg_sleep(0.1);

-- Multiple updates.
BEGIN;

UPDATE "Versioning" SET a = 5 WHERE a = 4;
UPDATE "Versioning" SET "b b" = '2012-01-01' WHERE a = 5;

SELECT a, "b b", lower(sys_period) = CURRENT_TIMESTAMP FROM "Versioning" ORDER BY a, sys_period;

SELECT a, c, upper(sys_period) = CURRENT_TIMESTAMP FROM "VersioningHistory" ORDER BY a, sys_period;

SELECT a, "b b" FROM "Versioning" WHERE lower(sys_period) = CURRENT_TIMESTAMP ORDER BY a, sys_period;

COMMIT;

-- Make sure that the next transaction's CURRENT_TIMESTAMP is different.
SELECT pg_sleep(0.1);

-- Delete.
BEGIN;

DELETE FROM "Versioning" WHERE a = 4;

SELECT a, "b b", lower(sys_period) = CURRENT_TIMESTAMP FROM "Versioning" ORDER BY a, sys_period;

SELECT a, c, upper(sys_period) = CURRENT_TIMESTAMP FROM "VersioningHistory" ORDER BY a, sys_period;

SELECT a, "b b" FROM "Versioning" WHERE lower(sys_period) = CURRENT_TIMESTAMP ORDER BY a, sys_period;

END;

-- Make sure that the next transaction's CURRENT_TIMESTAMP is different.
SELECT pg_sleep(0.1);

-- Delete.
BEGIN;

DELETE FROM "Versioning";

SELECT * FROM "Versioning";

SELECT a, c, upper(sys_period) = CURRENT_TIMESTAMP FROM "VersioningHistory" ORDER BY a, sys_period;

SELECT a, "b b" FROM "Versioning" WHERE lower(sys_period) = CURRENT_TIMESTAMP ORDER BY a, sys_period;

END;

DROP TABLE "Versioning";
DROP TABLE "VersioningHistory";