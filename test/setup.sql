\timing off

CREATE TABLE subscriptions
(
  name text NOT NULL,
  state text NOT NULL,
  sys_period tstzrange NOT NULL DEFAULT tstzrange(current_timestamp, null)
);

CREATE TABLE subscriptions_history (LIKE subscriptions);

CREATE TRIGGER versioning_trigger
BEFORE INSERT OR UPDATE OR DELETE ON subscriptions
FOR EACH ROW EXECUTE PROCEDURE versioning2(
  'sys_period', 'subscriptions_history', true
);

