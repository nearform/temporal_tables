performance_test:
	@echo "\nDB Setup\n"
	@createdb temporal_tables_test
	@psql temporal_tables_test -q -f versioning_function.sql
	@psql temporal_tables_test -q -f test/setup.sql
	@echo "\nRun Test\n"
	@echo "Insert"
	@psql temporal_tables_test -q -f test/performance.sql
	@echo "\nDB teardown\n"
	@psql temporal_tables_test -q -f test/teardown.sql
	@psql -q -c "drop database temporal_tables_test;"
