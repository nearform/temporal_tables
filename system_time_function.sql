CREATE OR REPLACE FUNCTION set_system_time(user_timestamp text)
RETURNS text AS $$
DECLARE
  custom_system_time text;
BEGIN
  IF user_timestamp IS NULL THEN
    custom_system_time := null;
  ELSE
    PERFORM 
      REGEXP_MATCHES(user_timestamp, 
          '(\d){4}-(0[1-9]|1[0-2])-(0[1-9]|[1-2]\d|3[0-1]) ([0-1]\d|2[0-3]):[0-5]\d:[0-5]\d(\.\d{1,3})?(\.\d{1,6})?', 
          'g');
    IF NOT FOUND THEN
      RAISE 'You must enter a timestamp in the following format: YYYY-MM-DD HH24:MI:SS.MS.US (hours are in 24-hour format 00-23, MS and US are optional)';
    ELSE
      custom_system_time := user_timestamp;
    END IF;
  END IF;
  
  PERFORM set_config('user_defined.system_time', custom_system_time, false);

  return custom_system_time;

END; 
$$ LANGUAGE plpgsql;
