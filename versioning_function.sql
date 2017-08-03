
CREATE OR REPLACE FUNCTION versioning()
RETURNS TRIGGER AS $$
DECLARE
  sys_period text;
  history_table text;
  manipulate jsonb;
  commonColumns text[];
  time_stamp_to_use timestamptz := current_timestamp;
  range_lower timestamptz;
  transaction_info txid_snapshot;
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

  -- check if sys_period exists on original table
  IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN
    IF NOT EXISTS(SELECT * FROM pg_attribute WHERE attrelid = TG_TABLE_NAME::regclass AND attname = sys_period AND NOT attisdropped) THEN
      RAISE 'column "%" of relation "%" does not exist', sys_period, TG_TABLE_NAME;
    END IF;
  END IF;

  IF TG_OP = 'UPDATE' OR TG_OP = 'DELETE' THEN
    -- Ignore rows already modified in this transaction
    transaction_info := txid_current_snapshot();
    IF OLD.xmin::text >= (txid_snapshot_xmin(transaction_info) % (2^32)::bigint)::text
    AND OLD.xmin::text <= (txid_snapshot_xmax(transaction_info) % (2^32)::bigint)::text THEN
      RETURN NEW;
    END IF;

    -- check if history table exits
    IF to_regclass(history_table) IS NULL THEN
      RAISE 'relation "%" does not exist', history_table;
    END IF;

    -- check if history table has sys_period
    IF NOT EXISTS(SELECT * FROM pg_attribute WHERE attrelid = history_table::regclass AND attname = sys_period AND NOT attisdropped) THEN
      RAISE 'history relation "%" does not contain system period column "%"', history_table, sys_period USING
      HINT = 'history relation must contain system period column with the same name and data type as the versioned one';
    END IF;

    IF TG_ARGV[2] = 'true' THEN
      -- mitigate update conflicts
      EXECUTE format('SELECT lower($1.%I)', sys_period) USING OLD INTO range_lower;

      IF range_lower >= time_stamp_to_use THEN
        time_stamp_to_use := range_lower + interval '1 microseconds';
      END IF;
    END IF;

    WITH history AS
      (SELECT attname
      FROM   pg_attribute
      WHERE  attrelid = history_table::regclass
      AND    attnum > 0
      AND    NOT attisdropped),
      main AS
      (SELECT attname
      FROM   pg_attribute
      WHERE  attrelid = TG_TABLE_NAME::regclass
      AND    attnum > 0
      AND    NOT attisdropped)
    SELECT array_agg(quote_ident(history.attname)) INTO commonColumns
      FROM history
      INNER JOIN main
      ON history.attname = main.attname
      AND history.attname != sys_period;

    EXECUTE ('INSERT INTO ' ||
      quote_ident(history_table) ||
      '(' ||
      array_to_string(commonColumns , ',') ||
      ',' ||
      quote_ident(sys_period) ||
      ') VALUES ($1.' ||
      array_to_string(commonColumns, ',$1.') ||
      ',tstzrange($2, $3, ''[)''))')
       USING OLD, range_lower, time_stamp_to_use;
  END IF;

  IF TG_OP = 'UPDATE' OR TG_OP = 'INSERT' THEN
    manipulate := jsonb_set('{}'::jsonb, ('{' || sys_period || '}')::text[], to_jsonb(tstzrange(time_stamp_to_use, null, '[)')));

    RETURN jsonb_populate_record(NEW, manipulate);
  END IF;

  RETURN OLD;
END;
$$ LANGUAGE plpgsql;