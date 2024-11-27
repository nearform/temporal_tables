#!/bin/bash

export PGDATESTYLE="Postgres, MDY";
psql -c "SHOW DateStyle;"

createdb temporal_tables_test
psql temporal_tables_test -q -f versioning_function_nochecks.sql
psql temporal_tables_test -q -f system_time_function.sql

mkdir -p test/result

echo "Downloading remote files"

REMOTE_BASE_PATH="https://raw.githubusercontent.com/arkhipov/temporal_tables/refs/heads/master/"
REMOTE_SQL_FILE_URL="${REMOTE_BASE_PATH}sql/"
LOCAL_SQL_PATH="test/remote_sql/"
REMOTE_OUT_FILE_URL="${REMOTE_BASE_PATH}expected/"
LOCAL_OUT_PATH="test/remote_expected/"
LOCAL_RESULT_PATH="test/remote_result/"

REMOTE_TESTS="combinations structure versioning"

mkdir -p $LOCAL_SQL_PATH
mkdir -p $LOCAL_OUT_PATH
mkdir -p $LOCAL_RESULT_PATH

REMOTE_FILES_TO_TEST=" "
for name in $REMOTE_TESTS; do
  curl -f -o ${LOCAL_SQL_PATH}${name}.sql ${REMOTE_SQL_FILE_URL}${name}.sql
  SQL_STATUS=$?

  curl -f -o ${LOCAL_OUT_PATH}${name}.out ${REMOTE_OUT_FILE_URL}${name}.out
  OUT_STATUS=$?

  if [ "$SQL_STATUS" -eq 0 ] && [ "$OUT_STATUS" -eq 0 ];  then
    echo "Remote files downloaded successfully for ${name}"
    REMOTE_FILES_TO_TEST="${REMOTE_FILES_TO_TEST}${name} "
  else
    echo "Remote files download failed for ${name}"
  fi
done

echo $REMOTE_FILES_TO_TEST

for name in $REMOTE_FILES_TO_TEST; do
  echo ""
  echo $name
  echo ""
  psql temporal_tables_test -X -a -q --set=SHOW_CONTEXT=never < test/remote_sql/$name.sql > test/remote_result/$name.out 2>&1
  diff -b test/remote_expected/$name.out test/remote_result/$name.out
done

TESTS="
  upper_case different_schema unchanged_values
  non_equality_types non_equality_types_unchanged_values
  unchanged_version_values versioning_including_current_version_in_history
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
