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
  existing_range tstzrange;
BEGIN
  -- version 0.0.1

  sys_period := TG_ARGV[0];
  history_table := TG_ARGV[1];

  IF TG_OP = 'UPDATE' OR TG_OP = 'DELETE' THEN
    -- Ignore rows already modified in this transaction
    transaction_info := txid_current_snapshot();
    IF OLD.xmin::text >= (txid_snapshot_xmin(transaction_info) % (2^32)::bigint)::text
    AND OLD.xmin::text <= (txid_snapshot_xmax(transaction_info) % (2^32)::bigint)::text THEN
      IF TG_OP = 'DELETE' THEN
        RETURN OLD;
      END IF;

      RETURN NEW;
    END IF;

    EXECUTE format('SELECT $1.%I', sys_period) USING OLD INTO existing_range;

    range_lower := lower(existing_range);

    IF TG_OP = 'UPDATE' THEN
      -- On UPDATE we always have a NEW.sys_period, which equals OLD.sys_period [range_lower] if not explicitly set
      IF NEW.sys_period IS NOT NULL AND lower(NEW.sys_period) > range_lower THEN
        -- If given a sys_period, use it's lower bound instead of current_timestamp
        time_stamp_to_use := lower(NEW.sys_period);
      END IF;
    END IF;

    IF TG_ARGV[2] = 'true' THEN
      -- mitigate update conflicts
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
      WHERE  attrelid = TG_RELID
      AND    attnum > 0
      AND    NOT attisdropped)
    SELECT array_agg(quote_ident(history.attname)) INTO commonColumns
      FROM history
      INNER JOIN main
      ON history.attname = main.attname
      AND history.attname != sys_period;

    EXECUTE ('INSERT INTO ' ||
      CASE split_part(history_table, '.', 2)
      WHEN '' THEN
        quote_ident(history_table)
      ELSE
        quote_ident(split_part(history_table, '.', 1)) || '.' || quote_ident(split_part(history_table, '.', 2))
      END ||
      '(' ||
      array_to_string(commonColumns , ',') ||
      ',' ||
      quote_ident(sys_period) ||
      ') VALUES ($1.' ||
      array_to_string(commonColumns, ',$1.') ||
      ',tstzrange($2, $3, ''[)''))')
       USING OLD, range_lower, time_stamp_to_use;
  END IF;

  IF TG_OP = 'INSERT' THEN
    -- On INSERT we have a NEW.sys_period if explicitly given (or by default value for column)
    IF NEW.sys_period IS NOT NULL THEN
      time_stamp_to_use := lower(NEW.sys_period);
    END IF;
  END IF;

  IF TG_OP = 'UPDATE' OR TG_OP = 'INSERT' THEN
    manipulate := jsonb_set('{}'::jsonb, ('{' || sys_period || '}')::text[], to_jsonb(tstzrange(time_stamp_to_use, null, '[)')));

    RETURN jsonb_populate_record(NEW, manipulate);
  END IF;

  RETURN OLD;
END;
$$ LANGUAGE plpgsql;