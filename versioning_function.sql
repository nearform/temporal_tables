CREATE OR REPLACE FUNCTION versioning()
RETURNS TRIGGER AS $$
DECLARE
  sys_period text;
  history_table text;
  manipulate jsonb;
  new_period tstzrange;
BEGIN
  IF TG_WHEN != 'BEFORE' OR TG_LEVEL != 'ROW' THEN
    RAISE TRIGGER_PROTOCOL_VIOLATED USING
    MESSAGE = 'function "versioning" must be fired BEFORE ROW';
  END IF;

  IF TG_OP != 'INSERT' AND TG_OP != 'UPDATE' AND TG_OP != 'DELETE' THEN
    RAISE TRIGGER_PROTOCOL_VIOLATED USING
    MESSAGE = 'function "versioning" must be fired for INSERT or UPDATE or DELETE';
  END IF;

  IF TG_NARGS != 3 THEN
    RAISE INVALID_PARAMETER_VALUE USING
    MESSAGE = 'wrong number of parameters for function "versioning"',
    HINT = 'expected 3 parameters but got ' || TG_NARGS;
  END IF;

  sys_period := TG_ARGV[0];
  history_table := TG_ARGV[1];

  IF TG_OP = 'UPDATE' OR TG_OP = 'DELETE' THEN
    EXECUTE format('SELECT tstzrange(lower($1.%I), current_timestamp, ''[)'')', sys_period) USING OLD INTO new_period;
    manipulate := jsonb_set('{}'::jsonb, ('{' || sys_period || '}')::text[], to_jsonb(new_period));

    EXECUTE format('INSERT INTO %I VALUES($1.*)', history_table) USING jsonb_populate_record(OLD, manipulate);
  END IF;

  IF TG_OP = 'UPDATE' OR TG_OP = 'INSERT' THEN
    manipulate := jsonb_set('{}'::jsonb, ('{' || sys_period || '}')::text[], to_jsonb(tstzrange(current_timestamp, null, '[)')));

    RETURN jsonb_populate_record(NEW, manipulate);
  END IF;

  RETURN OLD;
END;
$$ LANGUAGE plpgsql;