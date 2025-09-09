-- version 1.2.1

CREATE OR REPLACE FUNCTION set_system_time(user_timestamp timestamptz)
RETURNS void AS $$
DECLARE
  custom_system_time text;
BEGIN
  IF user_timestamp IS NULL THEN
    custom_system_time := null;
  ELSE
    custom_system_time := TO_CHAR(user_timestamp, 'YYYY-MM-DD HH24:MI:SS');
  END IF;

  PERFORM set_config('user_defined.system_time', custom_system_time, false);

END;
$$ LANGUAGE plpgsql;
