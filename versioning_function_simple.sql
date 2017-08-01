CREATE OR REPLACE FUNCTION versioning2()
RETURNS TRIGGER AS $$
DECLARE
  sys_period text;
  history_table text;
BEGIN

  sys_period := TG_ARGV[0];
  history_table := TG_ARGV[1];

  IF TG_OP = 'UPDATE' OR TG_OP = 'DELETE' THEN
    OLD.sys_period = tstzrange(lower(OLD.sys_period), current_timestamp, '[)');
    EXECUTE format('INSERT INTO %I VALUES($1.*)', history_table) USING OLD;
  END IF;

  IF TG_OP = 'UPDATE' OR TG_OP = 'INSERT' THEN
    NEW.sys_period = tstzrange(current_timestamp, null, '[)');
    RETURN NEW;
  END IF;

  RETURN OLD;
END;
$$ LANGUAGE plpgsql;