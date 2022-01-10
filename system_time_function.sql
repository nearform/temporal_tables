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
          '([0-9]){4}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1]) ([0-1][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]', 
          'g');
    IF NOT FOUND THEN
      RAISE 'You must enter a timestamp in the following format: YYYY-MM-DD HH:MI:SS (hours are in 24-hour format 00-23)';
    ELSE
      custom_system_time := user_timestamp;
    END IF;
  END IF;
  
  PERFORM set_config('user_defined.system_time', custom_system_time, false);

  return custom_system_time;

END; 
$$ LANGUAGE plpgsql;
