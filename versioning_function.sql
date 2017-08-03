
CREATE OR REPLACE FUNCTION versioning()
RETURNS TRIGGER AS $$
DECLARE
  sys_period text;
  history_table text;
  manipulate jsonb;
  new_period tstzrange;
  commonColumns text[];
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


  IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN
    IF NOT EXISTS(SELECT * FROM pg_attribute WHERE attrelid = TG_TABLE_NAME::regclass AND attname = sys_period) THEN
      RAISE 'column "%" of relation "%" does not exist', sys_period, TG_TABLE_NAME;
    END IF;
  END IF;

  IF TG_OP = 'UPDATE' OR TG_OP = 'DELETE' THEN
    IF NOT EXISTS(SELECT * FROM pg_tables WHERE tablename = history_table AND schemaname = TG_TABLE_SCHEMA) THEN
      RAISE 'relation "%" does not exist', history_table;
    END IF;

    IF NOT EXISTS(SELECT * FROM pg_attribute WHERE attrelid = history_table::regclass AND attname = sys_period) THEN
      RAISE 'history relation "%" does not contain system period column "%"', history_table, sys_period USING
      HINT = 'history relation must contain system period column with the same name and data type as the versioned one';
    END IF;

    IF TG_ARGV[2] = 'true' THEN
      -- mitigate update conflicts
      EXECUTE format('
        SELECT CASE WHEN lower($1.%I) < current_timestamp
        THEN tstzrange(lower($1.%I), current_timestamp, ''[)'')
        ELSE tstzrange(lower($1.%I), lower($1.%I) + interval ''1 microseconds'', ''[)'')
        END', sys_period, sys_period, sys_period, sys_period) USING OLD INTO new_period;
    ELSE
      EXECUTE format('SELECT tstzrange(lower($1.%I), current_timestamp, ''[)'')', sys_period) USING OLD INTO new_period;
    END IF;
    -- manipulate := jsonb_set('{}'::jsonb, ('{' || sys_period || '}')::text[], to_jsonb(new_period));

    EXECUTE 'WITH history AS
      (SELECT attname
      FROM   pg_attribute
      WHERE  attrelid = $1::regclass
      AND    attnum > 0
      AND    NOT attisdropped),
      main AS
      (SELECT attname
      FROM   pg_attribute
      WHERE  attrelid = $2::regclass
      AND    attnum > 0
      AND    NOT attisdropped)
      SELECT array_agg(quote_ident(history.attname))
      FROM history
      INNER JOIN main
      ON history.attname = main.attname
      AND history.attname != $3'
    USING history_table, TG_TABLE_NAME, sys_period
    INTO commonColumns;

    EXECUTE ('INSERT INTO ' ||
      quote_ident(history_table) ||
      '(' ||
      array_to_string(commonColumns , ',') ||
      ',' ||
      quote_ident(sys_period) ||
      ') VALUES ($1.' ||
      array_to_string(commonColumns, ',$1.') ||
      ',$2)')
       USING OLD, new_period;
  END IF;

  IF TG_OP = 'UPDATE' OR TG_OP = 'INSERT' THEN


    manipulate := jsonb_set('{}'::jsonb, ('{' || sys_period || '}')::text[], to_jsonb(tstzrange(current_timestamp, null, '[)')));

    RETURN jsonb_populate_record(NEW, manipulate);
  END IF;

  RETURN OLD;
END;
$$ LANGUAGE plpgsql;