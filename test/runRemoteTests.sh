#!/bin/bash

echo "Downloading remote files"

REMOTE_BASE_PATH="https://raw.githubusercontent.com/arkhipov/temporal_tables/refs/heads/master/"
REMOTE_SQL_FILE_URL="${REMOTE_BASE_PATH}sql/"
LOCAL_SQL_PATH="test/remote_sql/"
REMOTE_OUT_FILE_URL="${REMOTE_BASE_PATH}expected/"
LOCAL_OUT_PATH="test/remote_expected/"
LOCAL_RESULT_PATH="test/remote_result/"
FILES_DIFFERENT=false

# Create test folders if not available.
mkdir -p $LOCAL_SQL_PATH
mkdir -p $LOCAL_OUT_PATH
mkdir -p $LOCAL_RESULT_PATH

# Download the remote test files.
REMOTE_TESTS="$1"
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

# Run the tests for the downloaded files.
for name in $REMOTE_FILES_TO_TEST; do
  echo ""
  echo $name
  echo ""
  psql temporal_tables_test -X -a -q --set=SHOW_CONTEXT=never < test/remote_sql/$name.sql > test/remote_result/$name.out 2>&1
  DIFF_OUTPUT=$(diff -b test/remote_expected/$name.out test/remote_result/$name.out)

  if [ -n "$DIFF_OUTPUT" ]; then
    # Expected and actual files are different.
    FILES_DIFFERENT=true
  fi
done

# Exit with 1 if any of the test case failed.
if [ "$FILES_DIFFERENT" = true ]; then
  exit 1
fi
