CREATE OR REPLACE FUNCTION build_versioning(function_name text, original_table text, history_table text, sys_period text, adjust_time boolean)
RETURNS VOID AS $outer$
DECLARE
  commonColumns text[];
  holder record;
  holder2 record;
  queryTemplate text;
  adjust_time_check text;
BEGIN
  -- version 0.0.1

  -- check if original table exits
  IF to_regclass(original_table) IS NULL THEN
    RAISE 'relation "%" does not exist', original_table;
  END IF;

  -- check if history table exits
  IF to_regclass(history_table) IS NULL THEN
    RAISE 'relation "%" does not exist', history_table;
  END IF;

  -- check if sys_period exists on original table
  SELECT atttypid, attndims INTO holder FROM pg_attribute WHERE attrelid = original_table::regclass AND attname = sys_period AND NOT attisdropped;
  IF NOT FOUND THEN
    RAISE 'column "%" of relation "%" does not exist', sys_period, original_table USING
    ERRCODE = 'undefined_column';
  END IF;
  IF holder.atttypid != to_regtype('tstzrange') THEN
    IF holder.attndims > 0 THEN
      RAISE 'system period column "%" of relation "%" is not a range but an array', sys_period, original_table USING
      ERRCODE = 'datatype_mismatch';
    END IF;

    SELECT rngsubtype INTO holder2 FROM pg_range WHERE rngtypid = holder.atttypid;
    IF FOUND THEN
      RAISE 'system period column "%" of relation "%" is not a range of timestamp with timezone but of type %', sys_period, original_table, format_type(holder2.rngsubtype, null) USING
      ERRCODE = 'datatype_mismatch';
    END IF;

    RAISE 'system period column "%" of relation "%" is not a range but type %', sys_period, original_table, format_type(holder.atttypid, null) USING
    ERRCODE = 'datatype_mismatch';
  END IF;

  -- check if history table has sys_period
  IF NOT EXISTS(SELECT * FROM pg_attribute WHERE attrelid = history_table::regclass AND attname = sys_period AND NOT attisdropped) THEN
    RAISE 'history relation "%" does not contain system period column "%"', history_table, sys_period USING
    HINT = 'history relation must contain system period column with the same name and data type as the versioned one';
  END IF;

  -- check if column types on original table are different from ones in history table
  WITH history AS
    (SELECT attname, atttypid
    FROM   pg_attribute
    WHERE  attrelid = history_table::regclass
    AND    attnum > 0
    AND    NOT attisdropped),
    main AS
    (SELECT attname, atttypid
    FROM   pg_attribute
    WHERE  attrelid = original_table::regclass
    AND    attnum > 0
    AND    NOT attisdropped)
  SELECT
    history.attname AS history_name,
    main.attname AS main_name,
    history.atttypid AS history_type,
    main.atttypid AS main_type
  INTO holder
    FROM history
    INNER JOIN main
    ON history.attname = main.attname
  WHERE
    history.atttypid != main.atttypid;

  IF FOUND THEN
    RAISE 'column "%" of relation "%" is of type % but column "%" of history relation "%" is of type %',
      holder.main_name, original_table, format_type(holder.main_type, null), holder.history_name, history_table, format_type(holder.history_type, null)
    USING ERRCODE = 'datatype_mismatch';
  END IF;

  -- load common columns
  WITH history AS
    (SELECT attname
    FROM   pg_attribute
    WHERE  attrelid = history_table::regclass
    AND    attnum > 0
    AND    NOT attisdropped),
    main AS
    (SELECT attname
    FROM   pg_attribute
    WHERE  attrelid = original_table::regclass
    AND    attnum > 0
    AND    NOT attisdropped)
  SELECT array_agg(quote_ident(history.attname)) INTO commonColumns
    FROM history
    INNER JOIN main
    ON history.attname = main.attname
    AND history.attname != sys_period;

  -- create function
  queryTemplate := $template$
    CREATE OR REPLACE FUNCTION {function_name}()
    RETURNS TRIGGER AS $inner$
    DECLARE
      time_stamp_to_use timestamptz := current_timestamp;
      range_lower timestamptz;
      transaction_info txid_snapshot;
      existing_range tstzrange;
    BEGIN
      -- version 0.0.1

      IF TG_TABLE_NAME != '{original_table}' THEN
        RAISE TRIGGER_PROTOCOL_VIOLATED USING
        MESSAGE = 'function "{function_name}" to wrong table';
      END IF;

      IF TG_WHEN != 'BEFORE' OR TG_LEVEL != 'ROW' THEN
        RAISE TRIGGER_PROTOCOL_VIOLATED USING
        MESSAGE = 'function "{function_name}" must be fired BEFORE ROW';
      END IF;

      IF TG_OP != 'INSERT' AND TG_OP != 'UPDATE' AND TG_OP != 'DELETE' THEN
        RAISE TRIGGER_PROTOCOL_VIOLATED USING
        MESSAGE = 'function "{function_name}" must be fired for INSERT or UPDATE or DELETE';
      END IF;

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

        SELECT OLD.{sys_period} INTO existing_range;

        IF existing_range IS NULL THEN
          RAISE 'system period column "{sys_period}" of relation "{original_table}" must not be null' USING
          ERRCODE = 'null_value_not_allowed';
        END IF;

        IF isempty(existing_range) OR NOT upper_inf(existing_range) THEN
          RAISE 'system period column "{sys_period}" of relation "{original_table}" contains invalid value' USING
          ERRCODE = 'data_exception',
          DETAIL = 'valid ranges must be non-empty and unbounded on the high side';
        END IF;

        range_lower := lower(existing_range);
        {adjust_time_check}

        INSERT INTO {history_table} ({original_table_columns}, {quoted_sys_period}) VALUES ({query_values}, tstzrange(range_lower, time_stamp_to_use, '[)'));
      END IF;

      IF TG_OP = 'UPDATE' OR TG_OP = 'INSERT' THEN
        NEW.{sys_period} = tstzrange(time_stamp_to_use, null, '[)');

        RETURN NEW;
      END IF;

      RETURN OLD;
    END;
    $inner$ LANGUAGE plpgsql;
  $template$;


  adjust_time_check := '';
  IF adjust_time THEN
    adjust_time_check := $time_check$
      -- mitigate update conflicts
      IF range_lower >= time_stamp_to_use THEN
        time_stamp_to_use := range_lower + interval '1 microseconds';
      END IF;
    $time_check$;
  END IF;

  queryTemplate = replace(queryTemplate, '{function_name}', function_name);
  queryTemplate = replace(queryTemplate, '{original_table}', original_table);
  queryTemplate = replace(queryTemplate, '{history_table}', history_table);
  queryTemplate = replace(queryTemplate, '{sys_period}', sys_period);
  queryTemplate = replace(queryTemplate, '{quoted_sys_period}', quote_ident(sys_period));
  queryTemplate = replace(queryTemplate, '{original_table_columns}', array_to_string(commonColumns , ','));
  queryTemplate = replace(queryTemplate, '{query_values}', 'OLD.' || array_to_string(commonColumns, ',OLD.'));
  queryTemplate = replace(queryTemplate, '{adjust_time_check}', adjust_time_check);

  EXECUTE queryTemplate;

  RETURN;
END;
$outer$ LANGUAGE plpgsql;