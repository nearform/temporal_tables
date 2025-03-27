-- Migration Mode Test
CREATE TABLE versioning_migration (a bigint, "b b" date, sys_period tstzrange);

-- Insert some data before versioning is enabled.
INSERT INTO versioning_migration (a, "b b", sys_period) VALUES (1, '2020-01-01', tstzrange('2000-01-01', NULL));
INSERT INTO versioning_migration (a, "b b", sys_period) VALUES (2, '2020-02-01', tstzrange('2000-01-01', NULL));

CREATE TABLE versioning_migration_history (a bigint, "b b" date, sys_period tstzrange);

-- Create trigger with migration mode enabled
CREATE TRIGGER versioning_migration_trigger
BEFORE INSERT OR UPDATE OR DELETE ON versioning_migration
FOR EACH ROW EXECUTE PROCEDURE versioning('sys_period', 'versioning_migration_history', false, false, true, true);

-- Test migration during update
BEGIN;

UPDATE versioning_migration SET "b b" = '2020-03-01' WHERE a = 1;

-- Verify that the current record was migrated to history
SELECT a, "b b", lower(sys_period) = CURRENT_TIMESTAMP FROM versioning_migration ORDER BY a, sys_period;
SELECT a, "b b", upper(sys_period) = CURRENT_TIMESTAMP FROM versioning_migration_history ORDER BY a, sys_period;

COMMIT;

-- Make sure that the next transaction's CURRENT_TIMESTAMP is different.
SELECT pg_sleep(0.1);

-- Test migration during delete
BEGIN;

DELETE FROM versioning_migration WHERE a = 2;

-- Verify that the current record was migrated to history before deletion
SELECT a, "b b", lower(sys_period) = CURRENT_TIMESTAMP FROM versioning_migration ORDER BY a, sys_period;
SELECT a, "b b", upper(sys_period) = CURRENT_TIMESTAMP FROM versioning_migration_history ORDER BY a, sys_period;

COMMIT;

-- Cleanup
DROP TABLE versioning_migration;
DROP TABLE versioning_migration_history; 