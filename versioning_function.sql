CREATE OR REPLACE FUNCTION versioning()
RETURNS TRIGGER AS $$
DECLARE
  sys_period text;
  history_table text;
  manipulate jsonb;
  new_period tstzrange;
BEGIN

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