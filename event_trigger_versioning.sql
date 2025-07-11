-- event_trigger_versioning.sql
-- Event trigger to re-render static versioning trigger on ALTER TABLE

CREATE OR REPLACE FUNCTION rerender_versioning_trigger()
RETURNS event_trigger AS $$
DECLARE
  obj record;
  config record;
  sql text;
  source_schema text;
  source_table text;
  history_table text;
  sys_period text;
BEGIN
  FOR obj IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
    CONTINUE WHEN obj.command_tag <> 'ALTER TABLE';
    source_schema := SPLIT_PART(obj.object_identity, '.', 1);
    source_table := SPLIT_PART(obj.object_identity, '.', 2);
    -- when the source is history, invert to the actual source table
    IF source_table ~ '_history$' THEN
      source_table := SUBSTRING(source_table, 1, LENGTH(source_table) - 8);
    END IF;
    -- when a versioned table is altered, we need to re-render the trigger
    SELECT *
    INTO config
    FROM versioning_tables_metadata
    WHERE table_name = source_table
    AND table_schema = source_schema;

    IF FOUND THEN
      CALL render_versioning_trigger(
        p_table_name => FORMAT('%I.%I', source_schema, source_table),
        p_history_table => FORMAT('%I.%I', config.history_table_schema, config.history_table),
        p_sys_period => config.sys_period,
        p_ignore_unchanged_values => config.ignore_unchanged_values,
        p_include_current_version_in_history => config.include_current_version_in_history,
        p_mitigate_update_conflicts => config.mitigate_update_conflicts,
        p_enable_migration_mode => config.enable_migration_mode,
        p_increment_version => config.increment_version,
        p_version_column_name => config.version_column_name
      );
    END IF;
  END LOOP;
END;
$$ LANGUAGE plpgsql;

DROP EVENT TRIGGER IF EXISTS rerender_versioning_on_alter;

CREATE EVENT TRIGGER rerender_versioning_on_alter
  ON ddl_command_end
  WHEN TAG IN ('ALTER TABLE')
  EXECUTE FUNCTION rerender_versioning_trigger();
