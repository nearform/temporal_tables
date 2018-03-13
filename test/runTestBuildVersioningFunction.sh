#!/bin/bash

createdb temporal_tables_test
psql temporal_tables_test -q -f build_versioning_function.sql

mkdir -p test/result

TESTS="versioning_build structure_build combinations_build"

for name in $TESTS; do
  echo ""
  echo $name
  echo ""
  psql temporal_tables_test -X -a -q --set=SHOW_CONTEXT=never < test/sql/$name.sql > test/result/$name.out 2>&1
  diff -b test/expected/$name.out test/result/$name.out
done


psql -q -c "drop database temporal_tables_test;"