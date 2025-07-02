CREATE OR REPLACE PROCEDURE render_versioning_trigger(
  p_table_name text, 
  p_history_table text, 
  p_sys_period text,
  p_ignore_unchanged_values boolean DEFAULT false,
  p_include_current_version_in_history boolean DEFAULT false,
  p_mitigate_update_conflicts boolean DEFAULT false,
  p_enable_migration_mode boolean DEFAULT false  
) 
AS $$
DECLARE
  sql text;
BEGIN
  sql := generate_static_versioning_trigger(
    p_table_name, 
    p_history_table, 
    p_sys_period,
    p_ignore_unchanged_values,
    p_include_current_version_in_history,
    p_mitigate_update_conflicts,
    p_enable_migration_mode    
  );
  EXECUTE sql;
END;
$$ LANGUAGE plpgsql;
