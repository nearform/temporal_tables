-- event_trigger_versioning.sql
-- Event trigger to re-render static versioning trigger on ALTER TABLE

-- this metadata table tracks all the tables the system will automatically
-- create versioning triggers for, so that we can re-render the trigger
-- when the table is altered.
CREATE TABLE IF NOT EXISTS versioning_tables_metadata (
  table_name text,
  table_schema text,
  PRIMARY KEY (table_name, table_schema)
);

CREATE OR REPLACE FUNCTION rerender_versioning_trigger()
RETURNS event_trigger AS $$
DECLARE
  obj record;
  sql text;
  source_schema text;
  source_table text;
  history_table text;
  sys_period text;
BEGIN
  FOR obj IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
    source_schema := SPLIT_PART(obj.object_identity, '.', 1);
    source_table := SPLIT_PART(obj.object_identity, '.', 2);
    IF source_table ~ '_history$' THEN
      source_table := SUBSTRING(source_table, 1, LENGTH(source_table) - 8);
    END IF;
    IF obj.command_tag = 'ALTER TABLE'
    AND EXISTS (
      SELECT
      FROM versioning_tables_metadata
      WHERE table_name = source_table
      AND table_schema = source_schema
    ) THEN
      history_table := source_table || '_history'; -- Example convention
      sys_period := 'sys_period'; -- Example convention
      sql := generate_static_versioning_trigger(source_table, history_table, sys_period);
  	  EXECUTE sql;
    END IF;
  END LOOP;
END;
$$ LANGUAGE plpgsql;

DROP EVENT TRIGGER IF EXISTS rerender_versioning_on_alter;
CREATE EVENT TRIGGER rerender_versioning_on_alter
  ON ddl_command_end
  WHEN TAG IN ('ALTER TABLE')
  EXECUTE FUNCTION rerender_versioning_trigger();
