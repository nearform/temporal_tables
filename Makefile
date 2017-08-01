performance_test:
	@echo "\nDB Setup\n"
	@createdb temporal_tables_test
	@psql temporal_tables_test -q -f versioning_function_simple.sql
	@psql temporal_tables_test -q -f test/setup.sql

	@echo "\nRun Test\n"

	@echo "Insert"
	@psql temporal_tables_test -q -f test/performance-insert.sql

	@echo "Update"
	@psql temporal_tables_test -q -f test/performance-update.sql

	@echo "Delete"
	@psql temporal_tables_test -q -f test/performance-delete.sql

	@echo "\nDB teardown\n"
	@psql temporal_tables_test -q -f test/teardown.sql
	@psql -q -c "drop database temporal_tables_test;"

performance_test_original:
	@echo "\nDB Setup\n"
	@createdb temporal_tables_test
	@psql temporal_tables_test -q -c "create extension temporal_tables"
	@psql temporal_tables_test -q -f test/setup.sql

	@echo "\nRun Test\n"

	@echo "Insert"
	@psql temporal_tables_test -q -f test/performance-insert.sql

	@echo "Update"
	@psql temporal_tables_test -q -f test/performance-update.sql

	@echo "Delete"
	@psql temporal_tables_test -q -f test/performance-delete.sql

	@echo "\nDB teardown\n"
	@psql temporal_tables_test -q -f test/teardown.sql
	@psql -q -c "drop database temporal_tables_test;"
