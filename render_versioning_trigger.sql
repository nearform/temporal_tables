CREATE OR REPLACE PROCEDURE render_versioning_trigger(
  p_table_name text, 
  p_history_table text, 
  p_sys_period text,
  p_ignore_unchanged_values boolean DEFAULT false,
  p_include_current_version_in_history boolean DEFAULT false,
  p_mitigate_update_conflicts boolean DEFAULT false,
  p_enable_migration_mode boolean DEFAULT false,
  p_increment_version boolean DEFAULT false,
  p_version_column_name text DEFAULT 'version'
) 
AS $$
DECLARE
  table_name text;
  table_schema text;
  history_table_name text;
  history_table_schema text;
  trigger_func_name text;
  trigger_name text;
  trigger_sql text;
  func_sql text;
  common_columns text;
  sys_period_type text;
  history_sys_period_type text;
  new_row_compare text;
  old_row_compare text;
  version_declare_var text := '';
  version_init_logic text := '';
  version_column_insert text := '';
  version_old_value text := '';
  version_new_value text := '';
  version_increment_logic text := '';
BEGIN
  IF POSITION('.' IN p_table_name) > 0 THEN
    table_schema := split_part(p_table_name, '.', 1);
    table_name := split_part(p_table_name, '.', 2);
  ELSE
    table_schema := COALESCE(current_schema, 'public');
    table_name := p_table_name;
  END IF;
  p_table_name := format('%I.%I', table_schema, table_name);

  IF POSITION('.' IN p_history_table) > 0 THEN
    history_table_schema := split_part(p_history_table, '.', 1);
    history_table_name := split_part(p_history_table, '.', 2);
  ELSE
    history_table_schema := COALESCE(current_schema, 'public');
    history_table_name := p_history_table;
  END IF;
  p_history_table := format('%I.%I', history_table_schema, history_table_name);

  trigger_func_name := table_name || '_versioning';
  trigger_name := table_name || '_versioning_trigger';

  -- Get columns common to both source and history tables, excluding sys_period
  SELECT string_agg(quote_ident(main.attname), ',')
    INTO common_columns
    FROM (
      SELECT attname
      FROM pg_attribute
      WHERE attrelid = p_table_name::regclass
        AND attnum > 0 AND NOT attisdropped
        AND attname != p_sys_period
        AND (NOT p_increment_version OR attname != p_version_column_name)
    ) main
    INNER JOIN (
      SELECT attname
      FROM pg_attribute
      WHERE attrelid = p_history_table::regclass
        AND attnum > 0 AND NOT attisdropped
        AND attname != p_sys_period
        AND (NOT p_increment_version OR attname != p_version_column_name)
    ) hist
    ON main.attname = hist.attname;

  -- For row comparison (unchanged values)
  SELECT string_agg('NEW.' || quote_ident(main.attname), ',')
    INTO new_row_compare
    FROM (
      SELECT attname
      FROM pg_attribute
      WHERE attrelid = p_table_name::regclass
        AND attnum > 0 AND NOT attisdropped
        AND attname != p_sys_period
        AND (NOT p_increment_version OR attname != p_version_column_name)
    ) main
    INNER JOIN (
      SELECT attname
      FROM pg_attribute
      WHERE attrelid = p_history_table::regclass
        AND attnum > 0 AND NOT attisdropped
        AND attname != p_sys_period
        AND (NOT p_increment_version OR attname != p_version_column_name)
    ) hist
    ON main.attname = hist.attname;
  SELECT string_agg('OLD.' || quote_ident(main.attname), ',')
    INTO old_row_compare
    FROM (
      SELECT attname
      FROM pg_attribute
      WHERE attrelid = p_table_name::regclass
        AND attnum > 0 AND NOT attisdropped
        AND attname != p_sys_period
        AND (NOT p_increment_version OR attname != p_version_column_name)
    ) main
    INNER JOIN (
      SELECT attname
      FROM pg_attribute
      WHERE attrelid = p_history_table::regclass
        AND attnum > 0 AND NOT attisdropped
        AND attname != p_sys_period
        AND (NOT p_increment_version OR attname != p_version_column_name)
    ) hist
    ON main.attname = hist.attname;

  -- Get sys_period type for validation
  SELECT format_type(atttypid, null) INTO sys_period_type
    FROM pg_attribute
   WHERE attrelid = p_table_name::regclass AND attname = p_sys_period AND NOT attisdropped;
  SELECT format_type(atttypid, null) INTO history_sys_period_type
    FROM pg_attribute
   WHERE attrelid = p_history_table::regclass AND attname = p_sys_period AND NOT attisdropped;

  -- Check sys_period type at render time
  IF COALESCE(sys_period_type, 'invalid') != 'tstzrange' THEN
    RAISE 'system period column % does not have type tstzrange', sys_period_type;
  END IF;
  IF COALESCE(history_sys_period_type, 'invalid') != 'tstzrange' THEN
    RAISE 'history system period column % does not have type tstzrange', history_sys_period_type;
  END IF;

  -- Check version column if increment_version is enabled
  IF p_increment_version THEN
    -- Check if version column exists in main table
    IF NOT EXISTS(SELECT FROM pg_attribute WHERE attrelid = p_table_name::regclass AND attname = p_version_column_name AND NOT attisdropped) THEN
      RAISE 'relation "%" does not contain version column "%"', p_table_name, p_version_column_name;
    END IF;
    
    -- Check if version column exists in history table
    IF NOT EXISTS(SELECT FROM pg_attribute WHERE attrelid = p_history_table::regclass AND attname = p_version_column_name AND NOT attisdropped) THEN
      RAISE 'history relation "%" does not contain version column "%"', p_history_table, p_version_column_name;
    END IF;
    
    -- Check version column type is integer
    IF NOT EXISTS(SELECT FROM pg_attribute WHERE attrelid = p_table_name::regclass AND attname = p_version_column_name AND atttypid = 'integer'::regtype AND NOT attisdropped) THEN
      RAISE 'version column "%" of relation "%" is not an integer', p_version_column_name, p_table_name;
    END IF;
    
    -- Remove version column from common columns to handle it separately
    SELECT string_agg(quote_ident(main.attname), ',')
      INTO common_columns
      FROM (
        SELECT attname
        FROM pg_attribute
        WHERE attrelid = p_table_name::regclass
          AND attnum > 0 AND NOT attisdropped
          AND attname != p_sys_period
          AND attname != p_version_column_name
      ) main
      INNER JOIN (
        SELECT attname
        FROM pg_attribute
        WHERE attrelid = p_history_table::regclass
          AND attnum > 0 AND NOT attisdropped
          AND attname != p_sys_period
          AND attname != p_version_column_name
      ) hist
      ON main.attname = hist.attname;
  END IF;

  -- Prepare version-related variables for the format function
  IF p_increment_version THEN
    version_declare_var := E'\n  existing_version integer;';
    version_init_logic := format(E'  -- Initialize version handling\n  IF TG_OP = ''INSERT'' THEN\n    existing_version := 0;\n  ELSIF TG_OP = ''UPDATE'' OR TG_OP = ''DELETE'' THEN\n    existing_version := OLD.%I;\n    IF existing_version IS NULL THEN\n      RAISE ''version column "%%" of relation "%%" must not be null'', %L, TG_TABLE_NAME;\n    END IF;\n  END IF;\n', p_version_column_name, p_version_column_name);
    version_column_insert := ', ' || quote_ident(p_version_column_name);
    version_old_value := ', existing_version';
    version_new_value := ', existing_version + 1';
    version_increment_logic := format(E'\n    NEW.%I := existing_version + 1;', p_version_column_name);
  END IF;

  func_sql := format($outer$
CREATE OR REPLACE FUNCTION %1$I()
RETURNS TRIGGER AS $func$
DECLARE
  time_stamp_to_use timestamptz;
  range_lower timestamptz;
  existing_range tstzrange;
  newVersion record;
  oldVersion record;
  record_exists bool;%11$s
BEGIN
  -- set custom system time if exists
  BEGIN
    SELECT current_setting('user_defined.system_time') INTO STRICT time_stamp_to_use;
    time_stamp_to_use := TO_TIMESTAMP(time_stamp_to_use::text, 'YYYY-MM-DD HH24:MI:SS.US');
  EXCEPTION WHEN OTHERS THEN
    time_stamp_to_use := CURRENT_TIMESTAMP;
  END;

  IF TG_WHEN != 'BEFORE' OR TG_LEVEL != 'ROW' THEN
    RAISE TRIGGER_PROTOCOL_VIOLATED USING MESSAGE = 'function must be fired BEFORE ROW';
  END IF;

  IF TG_OP != 'INSERT' AND TG_OP != 'UPDATE' AND TG_OP != 'DELETE' THEN
    RAISE TRIGGER_PROTOCOL_VIOLATED USING MESSAGE = 'function must be fired for INSERT or UPDATE or DELETE';
  END IF;

  IF %3$L AND TG_OP = 'UPDATE' THEN 
    IF (%4$s) IS NOT DISTINCT FROM (%5$s) THEN
      RETURN OLD;
    END IF;
  END IF;

%12$s

  IF TG_OP = 'UPDATE' OR TG_OP = 'DELETE' OR (%6$L AND TG_OP = 'INSERT') THEN
    IF NOT %6$L THEN
      -- Ignore rows already modified in the current transaction
      IF OLD.xmin::TEXT = (txid_current() %% (2^32)::BIGINT)::TEXT THEN
        IF TG_OP = 'DELETE' THEN
          RETURN OLD;
        END IF;
        RETURN NEW;
      END IF;    
    END IF;
    
    IF TG_OP = 'UPDATE' OR TG_OP = 'DELETE' THEN
      existing_range := OLD.%2$I;
      IF existing_range IS NULL THEN
        RAISE 'system period column %% must not be null', %2$L;
      END IF;
      IF isempty(existing_range) 
      OR NOT upper_inf(existing_range) THEN
        RAISE 'system period column %% contains invalid value', %2$L;
      END IF;
      range_lower := lower(existing_range);
      
      IF %9$L THEN
        -- mitigate update conflicts
        IF range_lower >= time_stamp_to_use THEN
          time_stamp_to_use := range_lower + interval '1 microseconds';
        END IF;
      END IF;
      IF range_lower >= time_stamp_to_use THEN
        RAISE 'system period value of relation "%%" cannot be set to a valid period because a row that is attempted to modify was also modified by another transaction', TG_TABLE_NAME USING
        ERRCODE = 'data_exception',
        DETAIL = 'the start time of the system period is the greater than or equal to the time of the current transaction ';
      END IF;
    END IF;

    -- Check if record exists in history table for migration mode
    IF %10$L AND %6$L AND (TG_OP = 'UPDATE' OR TG_OP = 'DELETE') THEN
      SELECT EXISTS (
        SELECT FROM %7$s WHERE ROW(%8$s) IS NOT DISTINCT FROM ROW(%5$s)
      ) INTO record_exists;

      IF NOT record_exists THEN
        -- Insert current record into history table with its original range
        INSERT INTO %7$s (%8$s, %2$I%13$s) VALUES (%5$s, tstzrange(range_lower, time_stamp_to_use, '[)')%14$s);
      END IF;
    END IF;

    IF %6$L THEN
      IF TG_OP = 'UPDATE' OR TG_OP = 'DELETE' THEN
        UPDATE %7$s SET %2$I = tstzrange(range_lower, time_stamp_to_use, '[)')
        WHERE (%8$s) = (%8$s) AND %2$I = OLD.%2$I;
      END IF;
      IF TG_OP = 'UPDATE' OR TG_OP = 'INSERT' THEN
        INSERT INTO %7$s (%8$s, %2$I%13$s) VALUES (%4$s, tstzrange(time_stamp_to_use, NULL, '[)')%15$s);
      END IF;
    ELSE
      INSERT INTO %7$s (%8$s, %2$I%13$s) VALUES (%5$s, tstzrange(range_lower, time_stamp_to_use, '[)')%14$s);
    END IF;
  END IF;

  IF TG_OP = 'UPDATE' OR TG_OP = 'INSERT' THEN
    NEW.%2$I := tstzrange(time_stamp_to_use, NULL, '[)');%16$s
    RETURN NEW;
  END IF;

  RETURN OLD;
END;
$func$ LANGUAGE plpgsql;
$outer$,
  trigger_func_name,                    -- %1$s
  p_sys_period,                         -- %2$s
  p_ignore_unchanged_values,            -- %3$s
  new_row_compare,                      -- %4$s
  old_row_compare,                      -- %5$s
  p_include_current_version_in_history, -- %6$s
  p_history_table,                      -- %7$s
  common_columns,                       -- %8$s
  p_mitigate_update_conflicts,          -- %9$s
  p_enable_migration_mode,              -- %10$s
  version_declare_var,                  -- %11$s
  version_init_logic,                   -- %12$s
  version_column_insert,                -- %13$s
  version_old_value,                    -- %14$s
  version_new_value,                    -- %15$s
  version_increment_logic               -- %16$s
);

  trigger_sql := format($t$
DROP TRIGGER IF EXISTS %1$I ON %2$s;
CREATE TRIGGER %1$I
BEFORE INSERT OR UPDATE OR DELETE ON %2$s
FOR EACH ROW EXECUTE FUNCTION %3$I();
$t$,
  trigger_name,     
  p_table_name,     
  trigger_func_name 
);

  EXECUTE func_sql;
  EXECUTE trigger_sql;
END;
$$ LANGUAGE plpgsql;
