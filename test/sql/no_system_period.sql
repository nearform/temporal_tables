SET client_min_messages TO error;

CREATE TABLE no_system_period ();

CREATE TRIGGER versioning_trigger
BEFORE INSERT OR UPDATE OR DELETE ON no_system_period
FOR EACH ROW EXECUTE PROCEDURE versioning(NULL, NULL, false);

INSERT INTO no_system_period DEFAULT VALUES;