#!/bin/bash

createdb -h 127.0.0.1 -p 5432 -U postgres temporal_tables_test
psql -h 127.0.0.1 -p 5432 -U postgres temporal_tables_test -q -f versioning_function_nochecks.sql

mkdir -p test/result

TESTS="versioning structure combinations different_schema"

error=0

for name in $TESTS; do
  echo ""
  echo $name
  echo ""
  psql -h 127.0.0.1 -p 5432 -U postgres temporal_tables_test -X -a -q --set=SHOW_CONTEXT=never < test/sql/$name.sql > test/result/$name.out 2>&1
  difference="$(diff -b test/expected/$name.out test/result/$name.out)"
  echo $difference
  if [ ! -z "$difference" ]; then
    error=1
  fi
done


psql -h 127.0.0.1 -p 5432 -U postgres -q -c "drop database temporal_tables_test;"

echo $error "errors"
if [ "$error" -eq 1 ]; then
  exit 1
fi

exit 0