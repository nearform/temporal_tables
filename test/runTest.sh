#!/bin/bash

export PGDATESTYLE="Postgres, MDY";

createdb temporal_tables_test
psql temporal_tables_test -q -f versioning_function.sql
psql temporal_tables_test -q -f system_time_function.sql

mkdir -p test/result

FILES_DIFFERENT=false

REMOTE_TESTS="
  combinations structure versioning
  invalid_system_period invalid_system_period_values invalid_types
  no_history_system_period no_history_table no_system_period
  versioning_custom_system_time
  "

./test/runRemoteTests.sh "$REMOTE_TESTS"
REMOTE_TESTS_RESULT=$?

if [ "$REMOTE_TESTS_RESULT" -eq 1 ]; then
  # Atleast one of the remote tests failed.
  FILES_DIFFERENT=true
fi

TESTS="
  upper_case different_schema unchanged_values unchanged_version_values
  non_equality_types non_equality_types_unchanged_values
  set_system_time invalid_set_system_time versioning_including_current_version_in_history
  versioning_rollback_include_current_version_in_history noop_update
  "

#TESTS="set_system_time invalid_set_system_time"

for name in $TESTS; do
  echo ""
  echo $name
  echo ""
  psql temporal_tables_test -X -a -q --set=SHOW_CONTEXT=never < test/sql/$name.sql > test/result/$name.out 2>&1
  DIFF_OUTPUT=$(diff -b test/expected/$name.out test/result/$name.out)
  echo "$DIFF_OUTPUT"

  if [ -n "$DIFF_OUTPUT" ]; then
    # Expected and actual files are different.
    FILES_DIFFERENT=true
  fi
done

psql -q -c "drop database temporal_tables_test;"

# Exit with 1 if any of the test case failed.
if [ "$FILES_DIFFERENT" = true ]; then
  exit 1
fi
