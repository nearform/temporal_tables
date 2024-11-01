#!/bin/bash

createdb temporal_tables_test
psql temporal_tables_test -q -f versioning_function.sql
psql temporal_tables_test -q -f system_time_function.sql

mkdir -p test/result

TESTS="
  no_history_table no_history_system_period no_system_period
  invalid_system_period_values invalid_system_period invalid_types
  versioning upper_case structure combinations
  different_schema unchanged_values unchanged_version_values
  non_equality_types non_equality_types_unchanged_values
  set_system_time invalid_set_system_time versioning_including_current_version_in_history
  versioning_rollback_include_current_version_in_history noop_update
  "

for name in $TESTS; do
  echo ""
  echo $name
  echo ""
  psql temporal_tables_test -X -a -q --set=SHOW_CONTEXT=never < test/sql/$name.sql > test/result/$name.out 2>&1
  diff -b test/expected/$name.out test/result/$name.out
done


psql -q -c "drop database temporal_tables_test;"
