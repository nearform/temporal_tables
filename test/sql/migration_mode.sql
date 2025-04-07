CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Create the table with various columns
CREATE TABLE versioned_table (
    id SERIAL PRIMARY KEY,
    createdAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    name TEXT,
    price NUMERIC(10, 2),
    is_active BOOLEAN DEFAULT true,
    event_date DATE,
    metadata JSONB,
    unique_key UUID DEFAULT gen_random_uuid(),
    sys_period tstzrange NOT NULL DEFAULT tstzrange(current_timestamp, null)
);

-- create history table
CREATE TABLE versioned_table_history (LIKE versioned_table);

-- create trigger for versioning without include current and migration
CREATE TRIGGER versioned_table_versioning BEFORE
INSERT
    OR
UPDATE
    OR DELETE ON versioned_table FOR EACH ROW EXECUTE PROCEDURE versioning(
        'sys_period',
        'versioned_table_history',
        true
    );

--insert 10 records into the table
INSERT INTO
    versioned_table (
        createdAt,
        updatedAt,
        name,
        price,
        is_active,
        event_date,
        metadata
    )
SELECT
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP,
    'Item ' || gs,
    round((random() * 100) :: numeric, 2),
    (random() < 0.5),
    CURRENT_DATE + (gs % 30),
    jsonb_build_object(
        'generated_id',
        gs,
        'random_val',
        round((random() * 100) :: numeric, 2)
    )
FROM
    generate_series(1, 10) AS gs;

-- should return count of 0
SELECT
    count(*)
FROM
    versioned_table_history;

-- update a column for all rows
UPDATE
    versioned_table
SET
    price = round((random() * 100) :: numeric, 2);

-- should return count of 10
SELECT
    count(*)
FROM
    versioned_table_history;

-- drop versioning trigger
DROP TRIGGER IF EXISTS versioned_table_versioning ON versioned_table;

-- Create trigger with auto_migrate and include_current_version_in_history enabled
CREATE TRIGGER versioned_table_versioning BEFORE
INSERT
    OR
UPDATE
    OR DELETE ON versioned_table FOR EACH ROW EXECUTE PROCEDURE versioning(
        'sys_period',
        'versioned_table_history',
        true,
        false,
        true,
        true
    );

-- update a column for all rows
UPDATE
    versioned_table
SET
    price = round((random() * 100) :: numeric, 2);

-- should return count of 30 as the missing history and the current record should now be in the history table
SELECT
    count(*)
FROM
    versioned_table_history;

-- should not return any records
SELECT
    count(*),
    id
FROM
    versioned_table_history
GROUP BY
    id
HAVING
    count(*) != 3;

-- should return count of 10 for the current record of all 10 rows
SELECT
    count(*)
FROM
    versioned_table_history
WHERE
    upper(sys_period) IS NULL;

DROP TABLE IF EXISTS versioned_table;

DROP TABLE IF EXISTS versioned_table_history;

DROP EXTENSION IF EXISTS pgcrypto;
