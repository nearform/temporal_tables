run_test:
	@echo "\nRunning Tests\n"
	@sh test/runTest.sh

run_test_nochecks:
	@echo "\nRunning Tests - No Check\n"
	@sh test/runTestNochecks.sh


performance_test:
	@echo "\nDB Setup\n"
	@createdb temporal_tables_test
	@psql temporal_tables_test -q -f versioning_function.sql
	@psql temporal_tables_test -q -f system_time_function.sql
	@psql temporal_tables_test -q -f test/performance/setup.sql

	@echo "\nRun Test\n"

	@echo "Insert"
	@psql temporal_tables_test -q -f test/performance/insert.sql

	@echo "Update"
	@psql temporal_tables_test -q -f test/performance/update.sql

	@echo "Delete"
	@psql temporal_tables_test -q -f test/performance/delete.sql

	@echo "\nDB teardown\n"
	@psql temporal_tables_test -q -f test/performance/teardown.sql
	@psql -q -c "drop database temporal_tables_test;"

performance_test_nochecks:
	@echo "\nDB Setup\n"
	@createdb temporal_tables_test
	@psql temporal_tables_test -q -f versioning_function_nochecks.sql
	@psql temporal_tables_test -q -f system_time_function.sql
	@psql temporal_tables_test -q -f test/performance/setup.sql

	@echo "\nRun Test for NOCHECKS version\n"

	@echo "Insert"
	@psql temporal_tables_test -q -f test/performance/insert.sql

	@echo "Update"
	@psql temporal_tables_test -q -f test/performance/update.sql

	@echo "Delete"
	@psql temporal_tables_test -q -f test/performance/delete.sql

	@echo "\nDB teardown\n"
	@psql temporal_tables_test -q -f test/performance/teardown.sql
	@psql -q -c "drop database temporal_tables_test;"


performance_test_original:
	@echo "\nDB Setup\n"
	@createdb temporal_tables_test
	@psql temporal_tables_test -q -c "create extension temporal_tables"
	@psql temporal_tables_test -q -f test/performance/setup.sql

	@echo "\nRun Test for ORIGINAL version\n"

	@echo "Insert"
	@psql temporal_tables_test -q -f test/performance/insert.sql

	@echo "Update"
	@psql temporal_tables_test -q -f test/performance/update.sql

	@echo "Delete"
	@psql temporal_tables_test -q -f test/performance/delete.sql

	@echo "\nDB teardown\n"
	@psql -q -c "drop database temporal_tables_test;"
