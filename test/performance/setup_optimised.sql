\timing off

CREATE TABLE subscriptions
(
  name text NOT NULL,
  state text NOT NULL,
  sys_period tstzrange NOT NULL DEFAULT tstzrange(current_timestamp, null)
);

CREATE TABLE subscriptions_history (LIKE subscriptions);

SELECT build_versioning('optimised_versioning', 'subscriptions', 'subscriptions_history', 'sys_period', true);

CREATE TRIGGER versioning_trigger
BEFORE INSERT OR UPDATE OR DELETE ON subscriptions
FOR EACH ROW EXECUTE PROCEDURE optimised_versioning();

