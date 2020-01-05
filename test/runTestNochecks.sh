#!/bin/bash

createdb temporal_tables_test
psql temporal_tables_test -q -f versioning_function_nochecks.sql

mkdir -p test/result

TESTS="versioning versioning_camel_case structure combinations different_schema"

for name in $TESTS; do
  echo ""
  echo $name
  echo ""
  psql temporal_tables_test -X -a -q --set=SHOW_CONTEXT=never < test/sql/$name.sql > test/result/$name.out 2>&1
  diff -b test/expected/$name.out test/result/$name.out
done


psql -q -c "drop database temporal_tables_test;"