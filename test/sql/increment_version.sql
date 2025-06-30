-- Test for the increment_version feature

CREATE TABLE increment_version_test (
  id serial primary key,
  data text,
  version integer,
  sys_period tstzrange
);

CREATE TABLE increment_version_test_history (
  id integer,
  data text,
  version integer,
  sys_period tstzrange
);

-- Enable the versioning trigger with increment_version set to true
CREATE TRIGGER versioning_trigger
BEFORE INSERT OR UPDATE OR DELETE ON increment_version_test
FOR EACH ROW EXECUTE PROCEDURE versioning('sys_period', 'increment_version_test_history', 'false', 'false', 'false', 'false', 'true', 'version');

-- Test INSERT
BEGIN;
INSERT INTO increment_version_test (data) VALUES ('initial version');
SELECT data, version FROM increment_version_test;
SELECT data, version FROM increment_version_test_history;
COMMIT;

-- Test UPDATE
BEGIN;
UPDATE increment_version_test SET data = 'second version' WHERE id = 1;
SELECT data, version FROM increment_version_test;
SELECT data, version, upper(sys_period) IS NOT NULL as history_ended FROM increment_version_test_history;
COMMIT;

-- Test another UPDATE
BEGIN;
UPDATE increment_version_test SET data = 'third version' WHERE id = 1;
SELECT data, version FROM increment_version_test;
SELECT data, version, upper(sys_period) IS NOT NULL as history_ended FROM increment_version_test_history ORDER BY version;
COMMIT;

-- Test DELETE
BEGIN;
DELETE FROM increment_version_test WHERE id = 1;
SELECT * FROM increment_version_test;
SELECT data, version, upper(sys_period) IS NOT NULL as history_ended FROM increment_version_test_history ORDER BY version;
COMMIT;

DROP TABLE increment_version_test;
DROP TABLE increment_version_test_history;
