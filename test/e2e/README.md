# End-to-End Tests for Temporal Tables

This directory contains comprehensive end-to-end tests for the temporal tables PostgreSQL extension, written in modern TypeScript using Node.js built-in test runner and node-postgres.

## Test Files

### `db-helper.ts`
Database utility class that provides:
- Connection management
- SQL execution with transaction support
- Test data setup and cleanup
- SQL file loading and execution
- Type-safe query results

### `test-static-generator.ts`
Tests for the static trigger generator functionality:
- Basic versioning operations (INSERT, UPDATE, DELETE)
- Advanced features (ignore unchanged values, custom system time)
- Error handling and validation
- Schema compatibility testing
- Performance edge cases

### `test-legacy.ts`
Tests for the legacy versioning function:
- Backward compatibility with existing versioning function
- Parameter variations and options
- Custom system time handling
- Schema compatibility
- Comparison with static generator

### `test-event-trigger.ts`
Tests for event trigger functionality:
- Automatic trigger re-rendering on schema changes
- Metadata table management
- Migration mode handling
- Complex schema evolution scenarios

### `test-integration.ts`
Comprehensive integration tests:
- Real-world e-commerce scenarios
- Schema evolution and migration
- Performance and stress testing
- Referential integrity maintenance
- Error recovery and edge cases
- Concurrent modification handling

## Prerequisites

1. **PostgreSQL Database**: Either via Docker or local installation
2. **Node.js**: Version 18+ (for built-in test runner)
3. **TypeScript**: For type checking and compilation

## Database Setup

### Option 1: Docker (Recommended)
```bash
# Start PostgreSQL database
npm run db:start

# Stop database when done
npm run db:stop
```

### Option 2: Local PostgreSQL
If you have PostgreSQL installed locally:
1. Create a database (default: `postgres`)
2. Ensure the user has appropriate permissions
3. Set environment variables:
   ```bash
   export PGHOST=localhost
   export PGPORT=5432
   export PGUSER=your_username
   export PGPASSWORD=your_password
   export PGDATABASE=your_database
   ```

### Option 3: Remote PostgreSQL
You can also run tests against a remote PostgreSQL instance by setting the appropriate environment variables.

## Running Tests

### All E2E Tests
```bash
npm run test:e2e
```

### Individual Test Suites
```bash
# Static generator tests
npm run test:e2e:static

# Legacy function tests  
npm run test:e2e:legacy

# Integration tests
npm run test:e2e:integration
```

### Manual Test Execution
```bash
# Set environment variables
export PGHOST=localhost
export PGPORT=5432
export PGUSER=postgres
export PGPASSWORD=password

# Run specific test file
node --test test/e2e/test-static-generator.ts
```

## Test Architecture

### Database Helper Pattern
The tests use a `DatabaseHelper` class that encapsulates:
- Connection management with proper cleanup
- Transaction handling for test isolation
- Type-safe query execution with proper error handling
- Automatic loading of SQL functions and extensions

### Test Structure
Each test file follows a consistent pattern:
```typescript
describe('Feature Name', () => {
  let db: DatabaseHelper

  before(async () => {
    db = new DatabaseHelper()
    await db.connect()
    await db.setupVersioning()
  })

  after(async () => {
    await db.cleanup()
    await db.disconnect()
  })

  beforeEach(async () => {
    // Clean up test tables
  })

  // Test cases...
})
```

### Test Data Management
- Each test starts with a clean database state
- Tables are created and dropped per test to ensure isolation
- Helper functions create consistent test scenarios
- Proper cleanup prevents test interference

## Test Coverage

### Functional Testing
- ✅ Basic CRUD operations with versioning
- ✅ Advanced versioning features (ignore unchanged, custom time)
- ✅ Schema compatibility (quoted names, complex types)
- ✅ Error handling and validation
- ✅ Migration scenarios

### Integration Testing
- ✅ Real-world application scenarios
- ✅ Schema evolution and trigger re-rendering
- ✅ Performance under load
- ✅ Concurrent access patterns
- ✅ Referential integrity maintenance

### Edge Cases
- ✅ Transaction rollback handling
- ✅ Rapid sequential updates
- ✅ Bulk data operations
- ✅ Complex data types (JSON, arrays, custom types)
- ✅ Migration mode with existing data

## Database Configuration

The tests expect a PostgreSQL database with:
- Host: `localhost` (configurable via `PGHOST`)
- Port: `5432` (configurable via `PGPORT`)
- User: `postgres` (configurable via `PGUSER`)
- Password: `password` (configurable via `PGPASSWORD`)
- Database: `postgres` (configurable via `PGDATABASE`)

## TypeScript Configuration

The tests are written in TypeScript and use:
- Modern ES modules and async/await
- Strict type checking with proper interfaces
- Node.js built-in modules (`node:assert`, `node:test`)
- Type-safe database interactions

## Troubleshooting

### Database Connection Issues

#### Docker Environment
```bash
# Check if database is running
docker ps

# View database logs
docker logs temporal-tables-test

# Restart database
npm run db:stop
npm run db:start
```

#### Local PostgreSQL
```bash
# Check if PostgreSQL is running
pg_isready -h localhost -p 5432

# Connect manually to test
psql -h localhost -p 5432 -U postgres

# Check PostgreSQL service status (Linux/macOS)
sudo systemctl status postgresql

# Check PostgreSQL service status (Windows)
net start | findstr postgres
```

### Test Failures
- Ensure database is running and accessible
- Check that all SQL files are present in the root directory
- Verify environment variables are set correctly
- Look for port conflicts (PostgreSQL on 5432)
- Ensure the user has CREATE/DROP permissions on the database

### Performance Issues
- Tests include intentional delays (`pg_sleep`) for timestamp differentiation
- Large datasets in performance tests may take time
- Concurrent tests may be slower due to database locking

## Contributing

When adding new tests:
1. Follow the existing pattern and structure
2. Use descriptive test names and group related tests
3. Ensure proper cleanup in `beforeEach`/`afterEach`
4. Add type annotations for better IDE support
5. Include both positive and negative test cases
6. Test edge cases and error conditions

## Notes

- Tests use the Node.js built-in test runner (no Jest dependency)
- All assertions use Node.js built-in `assert` module
- Database operations are properly typed with TypeScript
- Each test suite is independent and can run in isolation
- Tests are designed to be deterministic and repeatable
