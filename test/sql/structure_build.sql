CREATE TABLE structure (a bigint, "b b" date, d text, sys_period tstzrange);

CREATE TABLE structure_history (like structure);

SELECT build_versioning('versioning', 'structure', 'structure_history', 'sys_period', false);

CREATE TRIGGER versioning_trigger
BEFORE INSERT OR UPDATE OR DELETE ON structure
FOR EACH ROW EXECUTE PROCEDURE versioning();

-- Insert.
BEGIN;

INSERT INTO structure (a, "b b", d) VALUES (1, '2000-01-01', 'test');

SELECT a, "b b", d FROM structure ORDER BY a, sys_period;

SELECT * FROM structure_history ORDER BY a, sys_period;

COMMIT;

-- Make sure that the next transaction's CURRENT_TIMESTAMP is different.
SELECT pg_sleep(0.1);

-- Update.
BEGIN;

UPDATE structure SET d = 'blah' WHERE a = 1;

SELECT a, "b b", d FROM structure ORDER BY a, sys_period;

SELECT a, "b b", d FROM structure_history ORDER BY a, sys_period;

COMMIT;