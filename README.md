
# temporal_tables

This is an attempt to rewrite the postgresql [temporal_tables](https://github.com/arkhipov/temporal_tables) extension without the need for external c extension.

The goal is to be able to use it on AWS RDS and other hosted solution, where using custom extension is not an option.

## current situation

The version provided in `versioning_function.sql` is almost a drop-in replacement.

It works exactly the same way, but lacks support for history tables that contain only a subset of the original table columns.

The version in `versioning_function_simple.sql` is similar to the previous one, but the code is simpler because it expects the system period column to always be called `sys_period`.

An even simpler version could be built by using a static name for the history table.

## Usage

Create a database and the versioning function:

```sh
createdb temporal_test
psql temporal_test < versioning_function.sql
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
FOR EACH ROW EXECUTE PROCEDURE versioning2(
  'sys_period', 'subscriptions_history', true
);
```

Now test with some data:

```sql
INSERT INTO subscriptions (name, state) VALUES ('test1', 'inserted');
UPDATE subscriptions SET state = 'updated' WHERE name = 'test1';
UPDATE subscriptions SET state = 'updated twice' WHERE name = 'test1';
DELETE FROM subscriptions WHERE name = 'test1';
```

Take some time between a query and the following, otherwise the difference in the time periods won't be noticeable.

After all the query are completed, you should check the tables content.

```sql
SELECT * FROM subscriptions;
```

Should return 0 rows

```sql
SELECT * FROM subscriptions_history
```

Should return something similar to:


name  |     state     |                            sys_period
----- | ------------- | -------------------------------------------------------------------
 test1 | inserted      | ["2017-08-01 16:09:45.542983+02","2017-08-01 16:09:54.984179+02")
 test1 | updated       | ["2017-08-01 16:09:54.984179+02","2017-08-01 16:10:08.880571+02")
 test1 | updated twice | ["2017-08-01 16:10:08.880571+02","2017-08-01 16:10:17.33659+02")


## Performance tests

For performance tests run:

```sh
make performance_test
```

This will create the temporal_tables_test database, add all necessary tables, run test tests and drop the database.

To test against the original c extension run:

```sh
make performance_test_original
```

This required the original extentions to be installed, but will automatically add it to the database.

In the current version `versioning_function.sql` is 3.5x slower than `versionin_function_simple.sql` and around 7x slower then the original version, but still execute the updates (the slowest operation) under 1ms on avarage.

