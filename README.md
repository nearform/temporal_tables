# Temporal Tables

[![ci](https://github.com/nearform/temporal_tables/actions/workflows/ci.yml/badge.svg)](https://github.com/nearform/temporal_tables/actions/workflows/ci.yml)

This rewrite aims to provide a temporal tables solution in PL/pgSQL, targeting AWS RDS, Google Cloud SQL, and Azure Database for PostgreSQL where custom C extensions aren't permitted.

The script in `versioning_function.sql` serves as a direct substitute.

For a speedier but riskier option, `versioning_function_nochecks.sql` is 2x faster due to the absence of validation checks.

Over time, new features have been introduced while maintaining backward compatibility:

- [Ignore updates with no actual changes](#ignore-unchanged-values)
- [Include the current version in history](#include-current-version-in-history)

<a name="usage"></a>

## Usage

Create a database and the versioning function:

```sh
createdb temporal_test
psql temporal_test < versioning_function.sql
```

If you would like to have `set_system_time` function available (more details [below](#system-time)) you should run the following as well:

```sh
psql temporal_test < system_time_function.sql
```

Connect to the db:

```
psql temporal_test
```

Create the table to version, in this example it will be a "subscription" table:

```sql
CREATE TABLE subscriptions
(
  name text NOT NULL,
  state text NOT NULL
);
```

Add the system period column:

```sql
ALTER TABLE subscriptions
  ADD COLUMN sys_period tstzrange NOT NULL DEFAULT tstzrange(current_timestamp, null);
```

Create the history table:

```sql
CREATE TABLE subscriptions_history (LIKE subscriptions);
```

Finally, create the trigger:

```sql
CREATE TRIGGER versioning_trigger
BEFORE INSERT OR UPDATE OR DELETE ON subscriptions
FOR EACH ROW EXECUTE PROCEDURE versioning(
  'sys_period', 'subscriptions_history', true
);
```

A note on the history table name. Previous versions of this extension quoted and escaped it before usage.
Starting version 0.4.0 we are not escaping it anymore and users need to provide the escaped version as a parameter to the trigger.

This is consistent with the c version, simplifies the extension code and fixes an issue with upper case names that weren't properly supported.

Now test with some data:

```sql
INSERT INTO subscriptions (name, state) VALUES ('test1', 'inserted');
UPDATE subscriptions SET state = 'updated' WHERE name = 'test1';
UPDATE subscriptions SET state = 'updated twice' WHERE name = 'test1';
DELETE FROM subscriptions WHERE name = 'test1';
```

Take some time between a query and the following, otherwise the difference in the time periods won't be noticeable.

After all the queries are completed, you should check the tables content.

```sql
SELECT * FROM subscriptions;
```

Should return 0 rows

```sql
SELECT * FROM subscriptions_history;
```

Should return something similar to:

| name  | state         | sys_period                                                        |
| ----- | ------------- | ----------------------------------------------------------------- |
| test1 | inserted      | ["2017-08-01 16:09:45.542983+02","2017-08-01 16:09:54.984179+02") |
| test1 | updated       | ["2017-08-01 16:09:54.984179+02","2017-08-01 16:10:08.880571+02") |
| test1 | updated twice | ["2017-08-01 16:10:08.880571+02","2017-08-01 16:10:17.33659+02")  |

<a name="system-time"></a>

## Setting custom system time

If you want to take advantage of setting a custom system time you can use the `set_system_time` function. It is a port of the original [set_system_time](https://github.com/arkhipov/temporal_tables#advanced-usage).
The function accepts a timestamp as input. It also accepts string representation of a timestamp in the following formats.
- `YYYY-MM-DD HH:MI:SS`
- `YYYY-MM-DD`

Same as the original function, calling it with `null` will reset to default setting (using the CURRENT_TIMESTAMP):

```sql
SELECT set_system_time(null);
```

Below is an example on how to use this function (continues using the example from above):

Create the set_system_time function:

```sh
psql temporal_test < system_time_function.sql
```

Set a custom value for the system time:

```sql
SELECT set_system_time('1999-12-31 23:59:59'::timestamptz);
```

Now test with some data:

```sql
INSERT INTO subscriptions (name, state) VALUES ('test2', 'inserted');
UPDATE subscriptions SET state = 'updated' WHERE name = 'test2';
UPDATE subscriptions SET state = 'updated twice' WHERE name = 'test2';
DELETE FROM subscriptions WHERE name = 'test2';
```

Take some time between a query and the following, otherwise the difference in the time periods won't be noticeable.

After all the queries are completed, you should check the `subscriptions_history` table content:

```sql
SELECT * FROM subscriptions_history;
```

Should return something similar to:

| name  | state         | sys_period                                                        |
| ----- | ------------- | ----------------------------------------------------------------- |
| test1 | inserted      | ["2017-08-01 16:09:45.542983+02","2017-08-01 16:09:54.984179+02") |
| test1 | updated       | ["2017-08-01 16:09:54.984179+02","2017-08-01 16:10:08.880571+02") |
| test1 | updated twice | ["2017-08-01 16:10:08.880571+02","2017-08-01 16:10:17.33659+02")  |
| test2 | inserted      | ["1999-12-31 23:59:59+01","1999-12-31 23:59:59.000001+01")        |
| test2 | updated       | ["1999-12-31 23:59:59.000001+01","1999-12-31 23:59:59.000002+01") |
| test2 | updated twice | ["1999-12-31 23:59:59.000002+01","1999-12-31 23:59:59.000003+01") |

<a name="additional-features"></a>

## Additional features

<a name="ignore-unchanged-values"></a>

### Ignore updates without actual change

**NOTE: This feature does not work for tables with columns with types that does not support equality operator (e.g. PostGIS types, JSON types, etc.).**

By default this extension creates a record in the history table for every update that occurs in the versioned table, regardless of any change actually happening.

We added a fourth paramater to the trigger to change this behaviour and only record updates that result in an actual change.

It is worth mentioning that before making the change, a check is performed on the source table against the history table, in such a way that if the history table has only a subset of the columns of the source table, and you are performing an update in a column that is not present in this subset (this means the column does not exist in the history table), this extension will NOT add a new record to the history. Then you can have columns in the source table that create no new versions if modified by not including those columns in the history table.

The paramater is set by default to `false`, set it to `true` to stop tracking updates without actual changes:

```sql
CREATE TRIGGER versioning_trigger
BEFORE INSERT OR UPDATE OR DELETE ON subscriptions
FOR EACH ROW EXECUTE PROCEDURE versioning(
  'sys_period', 'subscriptions_history', true, true
);
```


<a name="include-current-version-in-history"></a>

### Include the current version in history

By default this extension only creates a record in the history table for historical records. This feature enables users to also store the details of the current record in the history table. Simplifying cases when you want a consolidated view of both the current and historical states.

e.g

```sql
SELECT * FROM t_history WHERE x <@ sys_period;
```
when `include_current_version_in_history` is true

as opposed to

``` sql
SELECT * FROM t WHERE x <@ sys_period
UNION
SELECT * FROM t_history WHERE x <@ sys_period;
```
when `include_current_version_in_history` is false (or unset)

This is a fith parameter in the extension so all previous parameters need to be specified when using this.

The parameter is set by default to false, set it to true to include current version of records in the history table.

``` sql
CREATE TRIGGER versioning_trigger
BEFORE INSERT OR UPDATE OR DELETE ON subscriptions
FOR EACH ROW EXECUTE PROCEDURE versioning(
  'sys_period', 'subscriptions_history', true, false, true
);
```

### Migrating to include_current_version_in_history

If you're already using temporal tables and want to adopt the `include_current_version_in_history` feature, follow these steps to safely migrate your existing tables without losing historical data.

#### Prerequisites

1. **Maintenance Window**: Schedule a maintenance window when no applications are writing to the tables.
2. **Backup**: Create a backup of your database before proceeding.
3. **Latest Version**: Ensure you're running the latest version of the temporal tables functions:
   ```sql
   -- Update the versioning function
   \i versioning_function.sql
   ```

#### Migration Steps

1. **Identify Versioned Tables**
   First, identify all your versioned tables:
   ```sql
   SELECT DISTINCT trigger_schema, event_object_table 
   FROM information_schema.triggers 
   WHERE trigger_name = 'versioning_trigger';
   ```

2. **Copy Current Records**
   For each versioned table, copy current records to the history table:
   ```sql
   -- Replace table_name with your actual table name
   INSERT INTO table_name_history 
   SELECT *, tstzrange(LOWER(sys_period), NULL) 
   FROM table_name 
   WHERE NOT EXISTS (
     SELECT 1 
     FROM table_name_history 
     WHERE table_name_history.primary_key = table_name.primary_key
     AND UPPER(table_name_history.sys_period) IS NULL
   );
   ```

3. **Update Triggers**
   Recreate the versioning trigger with the new parameter:
   ```sql
   -- Drop existing trigger
   DROP TRIGGER IF EXISTS versioning_trigger ON table_name;
   
   -- Create new trigger with include_current_version_in_history
   CREATE TRIGGER versioning_trigger
   BEFORE INSERT OR UPDATE OR DELETE ON table_name
   FOR EACH ROW EXECUTE PROCEDURE versioning(
     'sys_period',
     'table_name_history',
     true,  -- enforce timestamps
     false, -- ignore unchanged values (adjust as needed)
     true   -- include current version in history
   );
   ```

#### Verification

After migration, verify the setup:

```sql
-- Check if current records exist in history
SELECT t.*, h.*
FROM table_name t
LEFT JOIN table_name_history h ON t.primary_key = h.primary_key
WHERE UPPER(h.sys_period) IS NULL;
```

#### Rollback Plan

If issues occur, you can revert to the previous state:

```sql
-- Recreate trigger without include_current_version_in_history
CREATE OR REPLACE TRIGGER versioning_trigger
BEFORE INSERT OR UPDATE OR DELETE ON table_name
FOR EACH ROW EXECUTE PROCEDURE versioning(
  'sys_period',
  'table_name_history',
  true,  -- enforce timestamps
  false  -- ignore unchanged values (adjust as needed)
);

-- Remove current versions from history
DELETE FROM table_name_history 
WHERE UPPER(sys_period) IS NULL;
```

#### Notes
- The migration process ensures no historical data is lost
- Existing queries that use UNION to combine current and historical records will continue to work
- New queries can benefit from simplified syntax by querying just the history table
- Consider updating application queries to use the new consolidated history view

<a name="migrations"></a>

## Migrations

During the life of an application is may be necessary to change the schema of a table. In order for temporal_tables to continue to work properly the same migrations should be applied to the history table as well.

### What happens if a column is added to the original table but not to the history table?

The new column will be ignored, meaning that the updated row is transferred to the history table, but without the value of the new column. This means that you will lose that specific data.

There are valid use cases for this, for example when you are not interested in storing the historic values of that column.

**Beware that temporal_tables won't raise an error**

### What should I do if I need to remove a column from the original table but want to keep the historic values for it?

You remove the column in the original table, but keep it in the history table - provided it accepts null values.

From that point on the old column in the history table will be ignored and will get null values.

If the column doesn't accept null values you'll need to modify it to allow for null values, otherwise temporal_tables won't be able to create new rows and all operations on the original table will fail

<a name="test"></a>

## Test

In order to run tests:

```sh
make run_test
```

The test suite will run the queries in test/sql and store the output in test/result, and will then diff the output from test/result with the prerecorded output in test/expected.

A test suite is also available for the nochecks alternative:

```sh
make run_test_nochecks
```

Obviously, this suite won't run the tests about the error reporting.

<a name="performance_tests"></a>

## Performance tests

For performance tests run:

```sh
make performance_test
```

This will create the temporal_tables_test database, add all necessary tables, run test tests and drop the database.

Is it also possible to test against the nochecks version:

```sh
make performance_test_nochecks
```

or the original c extension run:

```sh
make performance_test_original
```

This required the original extentions to be installed, but will automatically add it to the database.

On the test machine (my laptop) the complete version is 2x slower than the nochecks versions and 16x slower than the original version.

Two comments about those results:

- original c version makes some use of caching (i.e to share an execution plan), whilst this version doesn't. This is propably accounting for a good chunk of the performance difference. At the moment there's not plan of implementing such caching in this version.
- The trigger still executes in under 1ms and in production environments the the network latency should be more relevant than the trigger itself.

<a name="the-team"></a>

## The team

### Paolo Chiodi

[https://github.com/paolochiodi](https://github.com/paolochiodi)

[https://twitter.com/paolochiodi](https://twitter.com/paolochiodi)

<a name="acknowledgements"></a>

## Acknowledgements

This project was kindly sponsored by [nearForm](http://nearform.com).

## License

Licensed under [MIT](./LICENSE).

The test scenarios in test/sql and test/expected have been copied over from the original temporal_tables extension, whose license is [BSD 2-clause](https://github.com/arkhipov/temporal_tables/blob/master/LICENSE)

[![banner](https://raw.githubusercontent.com/nearform/.github/refs/heads/master/assets/os-banner-green.svg)](https://www.nearform.com/contact/?utm_source=open-source&utm_medium=banner&utm_campaign=os-project-pages)
