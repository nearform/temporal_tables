#!/bin/bash

createdb temporal_tables_test
psql temporal_tables_test -q -f versioning_function_nochecks.sql
psql temporal_tables_test -q -f system_time_function.sql

mkdir -p test/result

TESTS="
  versioning upper_case structure combinations different_schema unchanged_values
  non_equality_types non_equality_types_unchanged_values
  unchanged_version_values
  "

for name in $TESTS; do
  echo ""
  echo $name
  echo ""
  psql temporal_tables_test -X -a -q --set=SHOW_CONTEXT=never < test/sql/$name.sql > test/result/$name.out 2>&1
  diff -b test/expected/$name.out test/result/$name.out
done


psql -q -c "drop database temporal_tables_test;"
