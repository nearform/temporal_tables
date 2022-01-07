CREATE OR REPLACE FUNCTION set_system_time(custom_system_time text)
RETURNS timestamptz AS $$
DECLARE
  custom_system_timestamptz timestamptz;
BEGIN

  IF custom_system_time IS NULL THEN
    custom_system_timestamptz = CURRENT_TIMESTAMP;
  ELSE
    SELECT TO_TIMESTAMP(
        custom_system_time,
        'YYYY-MM-DD HH24:MI:SS'
    ) INTO custom_system_timestamptz;
  END IF;

  SET user_defined.system_time = custom_system_timestamptz;

  return custom_system_timestamptz;

END; 
$$ LANGUAGE plpgsql;
