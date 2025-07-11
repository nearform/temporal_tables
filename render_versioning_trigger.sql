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

  -- Build the main trigger logic conditionally at generation time
  DECLARE
    unchanged_check_logic text := '';
    conflict_mitigation_logic text := '';
    transaction_check_logic text := '';
    migration_check_logic text := '';
    current_version_update_logic text := '';
    variable_declarations text := E'  time_stamp_to_use timestamptz;\n';
    update_delete_logic text := '';
    insert_update_logic text := '';
  BEGIN
    -- Generate unchanged values check logic
    IF p_ignore_unchanged_values THEN
      unchanged_check_logic := format(E'  IF TG_OP = ''UPDATE'' THEN\n    IF (%s) IS NOT DISTINCT FROM (%s) THEN\n      RETURN OLD;\n    END IF;\n  END IF;\n\n', new_row_compare, old_row_compare);
    END IF;

    -- Generate conflict mitigation logic
    IF p_mitigate_update_conflicts THEN
      conflict_mitigation_logic := E'      IF range_lower >= time_stamp_to_use THEN\n        time_stamp_to_use := range_lower + interval ''1 microseconds'';\n      END IF;\n';
    END IF;

    -- Generate transaction check logic (only if include_current_version_in_history is false)
    IF NOT p_include_current_version_in_history THEN
      transaction_check_logic := E'      -- Ignore rows already modified in the current transaction\n      IF OLD.xmin::TEXT = (txid_current() % (2^32)::BIGINT)::TEXT THEN\n        IF TG_OP = ''DELETE'' THEN\n          RETURN OLD;\n        END IF;\n        RETURN NEW;\n      END IF;\n';
    END IF;

    -- Generate migration check logic
    IF p_enable_migration_mode AND p_include_current_version_in_history THEN
      migration_check_logic := format(E'    -- Check if record exists in history table for migration mode\n    IF TG_OP = ''UPDATE'' OR TG_OP = ''DELETE'' THEN\n      SELECT EXISTS (\n        SELECT FROM %s WHERE ROW(%s) IS NOT DISTINCT FROM ROW(%s)\n      ) INTO record_exists;\n\n      IF NOT record_exists THEN\n        -- Insert current record into history table with its original range\n        INSERT INTO %s (%s, %I%s) VALUES (%s, tstzrange(range_lower, time_stamp_to_use, ''[)'')%s);\n      END IF;\n    END IF;\n\n', 
        p_history_table, common_columns, old_row_compare, p_history_table, common_columns, p_sys_period, version_column_insert, old_row_compare, version_old_value);
    END IF;

    -- Generate current version update logic for include_current_version_in_history mode
    IF p_include_current_version_in_history THEN
      current_version_update_logic := format(E'      IF TG_OP = ''UPDATE'' OR TG_OP = ''DELETE'' THEN\n        UPDATE %s SET %I = tstzrange(range_lower, time_stamp_to_use, ''[)'')\n        WHERE (%s) = (%s) AND %I = OLD.%I;\n      END IF;\n      IF TG_OP = ''UPDATE'' OR TG_OP = ''INSERT'' THEN\n        INSERT INTO %s (%s, %I%s) VALUES (%s, tstzrange(time_stamp_to_use, NULL, ''[)'')%s);\n      END IF;\n',
        p_history_table, p_sys_period, common_columns, common_columns, p_sys_period, p_sys_period,
        p_history_table, common_columns, p_sys_period, version_column_insert, new_row_compare, version_new_value);
    END IF;

    -- Add variables only when needed
    IF p_enable_migration_mode AND p_include_current_version_in_history THEN
      variable_declarations := variable_declarations || E'  record_exists bool;\n';
    END IF;
    
    IF p_increment_version THEN
      variable_declarations := variable_declarations || E'  existing_version integer;\n';
    END IF;
    
    -- Only add range variables if we have UPDATE or DELETE operations
    variable_declarations := variable_declarations || E'  range_lower timestamptz;\n  existing_range tstzrange;';

    -- Build UPDATE/DELETE logic with integrated history handling
    IF p_include_current_version_in_history THEN
      update_delete_logic := format(E'  IF TG_OP = ''UPDATE'' OR TG_OP = ''DELETE'' THEN\n    existing_range := OLD.%1$I;\n    IF existing_range IS NULL THEN\n      RAISE ''system period column "%%" must not be null'', %2$L;\n    END IF;\n    IF isempty(existing_range) OR NOT upper_inf(existing_range) THEN\n      RAISE ''system period column "%%" contains invalid value'', %2$L;\n    END IF;\n    range_lower := lower(existing_range);\n    \n%3$s    IF range_lower >= time_stamp_to_use THEN\n      RAISE ''system period value of relation "%%" cannot be set to a valid period because a row that is attempted to modify was also modified by another transaction'', TG_TABLE_NAME USING\n      ERRCODE = ''data_exception'',\n      DETAIL = ''the start time of the system period is the greater than or equal to the time of the current transaction '';\n    END IF;\n  END IF;\n\n  IF TG_OP = ''UPDATE'' OR TG_OP = ''DELETE'' OR TG_OP = ''INSERT'' THEN\n%4$s%5$s%6$s\n  END IF;',
        p_sys_period, p_sys_period, conflict_mitigation_logic,
        transaction_check_logic, migration_check_logic, current_version_update_logic);
    ELSE
      update_delete_logic := format(E'  IF TG_OP = ''UPDATE'' OR TG_OP = ''DELETE'' THEN\n    existing_range := OLD.%1$I;\n    IF existing_range IS NULL THEN\n      RAISE ''system period column "%%" must not be null'', %2$L;\n    END IF;\n    IF isempty(existing_range) OR NOT upper_inf(existing_range) THEN\n      RAISE ''system period column "%%" contains invalid value'', %2$L;\n    END IF;\n    range_lower := lower(existing_range);\n    \n%3$s    IF range_lower >= time_stamp_to_use THEN\n      RAISE ''system period value of relation "%%" cannot be set to a valid period because a row that is attempted to modify was also modified by another transaction'', TG_TABLE_NAME USING\n      ERRCODE = ''data_exception'',\n      DETAIL = ''the start time of the system period is the greater than or equal to the time of the current transaction '';\n    END IF;\n\n%4$s\n    INSERT INTO %5$s (%6$s, %1$I%7$s) VALUES (%8$s, tstzrange(range_lower, time_stamp_to_use, ''[)'')%9$s);\n  END IF;',
        p_sys_period, p_sys_period, conflict_mitigation_logic, transaction_check_logic,
        p_history_table, common_columns, version_column_insert, old_row_compare, version_old_value);
    END IF;

    -- Build INSERT/UPDATE logic
    IF p_include_current_version_in_history THEN
      insert_update_logic := format(E'  IF TG_OP = ''UPDATE'' OR TG_OP = ''INSERT'' THEN\n    NEW.%1$I := tstzrange(time_stamp_to_use, NULL, ''[)'');%2$s\n    RETURN NEW;\n  END IF;\n\n  RETURN OLD;',
        p_sys_period, version_increment_logic);
    ELSE
      insert_update_logic := format(E'  IF TG_OP = ''UPDATE'' OR TG_OP = ''INSERT'' THEN\n    NEW.%1$I := tstzrange(time_stamp_to_use, NULL, ''[)'');%2$s\n    RETURN NEW;\n  END IF;\n\n  RETURN OLD;',
        p_sys_period, version_increment_logic);
    END IF;

  func_sql := format($outer$
CREATE OR REPLACE FUNCTION %1$I()
RETURNS TRIGGER AS $func$
DECLARE
%2$s
BEGIN
  -- set custom system time if exists
  BEGIN
    SELECT current_setting('user_defined.system_time') INTO STRICT time_stamp_to_use;
    time_stamp_to_use := TO_TIMESTAMP(time_stamp_to_use::text, 'YYYY-MM-DD HH24:MI:SS.US');
  EXCEPTION WHEN OTHERS THEN
    time_stamp_to_use := CURRENT_TIMESTAMP;
  END;

%3$s%4$s

%5$s

%6$s
END;
$func$ LANGUAGE plpgsql;
$outer$,
  trigger_func_name,        -- %1$s
  variable_declarations,    -- %2$s  
  unchanged_check_logic,    -- %3$s
  version_init_logic,       -- %4$s
  update_delete_logic,      -- %5$s
  insert_update_logic       -- %6$s
);
  END;

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
