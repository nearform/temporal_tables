#!/bin/bash

createdb temporal_tables_test
psql temporal_tables_test -q -f versioning_function.sql

mkdir -p test/result

TESTS="
  no_history_table no_history_system_period no_system_period
  invalid_system_period_values invalid_system_period invalid_types
  versioning structure combinations different_schema"

error=0

for name in $TESTS; do
  echo ""
  echo $name
  echo ""
  psql temporal_tables_test -X -a -q --set=SHOW_CONTEXT=never < test/sql/$name.sql > test/result/$name.out 2>&1
  difference="$(diff -b test/expected/$name.out test/result/$name.out)"
  echo $difference
  if [ ! -z "$difference" ]; then
    error=1
  fi
done


psql -q -c "drop database temporal_tables_test;"

echo $error
if [ "$error" -eq 1 ]; then
  exit 1
fi

exit 0