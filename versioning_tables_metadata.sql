-- this metadata table tracks all the tables the system will automatically
-- create versioning triggers for, so that we can re-render the trigger
-- when the table is altered.
CREATE TABLE IF NOT EXISTS versioning_tables_metadata (
  table_name text NOT NULL,
  table_schema text NOT NULL,
  history_table text NOT NULL,
  history_table_schema text NOT NULL,
  sys_period text NOT NULL DEFAULT 'sys_period',
  ignore_unchanged_values boolean NOT NULL DEFAULT false,
  include_current_version_in_history boolean NOT NULL DEFAULT false,
  mitigate_update_conflicts boolean NOT NULL DEFAULT false,
  enable_migration_mode boolean NOT NULL DEFAULT false,
  PRIMARY KEY (table_name, table_schema)
);

-- Example INSERT statements with all parameters:
-- INSERT INTO versioning_tables_metadata (
--   table_name, 
--   table_schema, 
--   history_table, 
--   history_table_schema,
--   sys_period,
--   ignore_unchanged_values,
--   include_current_version_in_history,
--   mitigate_update_conflicts,
--   enable_migration_mode
-- )
-- VALUES
--   ('subscriptions', 'public', 'subscriptions_history', 'history', 'sys_period', false, false, false, false),
--   ('users', 'public', 'users_history', 'public', 'system_time', true, true, false, false);
