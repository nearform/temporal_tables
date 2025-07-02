import { deepStrictEqual, ok, rejects } from 'node:assert'
import { describe, test, before, after, beforeEach } from 'node:test'
import { DatabaseHelper } from './db-helper.js'

describe('Static Generator E2E Tests', () => {
  let db: DatabaseHelper

  before(async () => {
    db = new DatabaseHelper()
    await db.connect()
    await db.setupVersioning(DatabaseHelper.modernMinimumPostgresVersion)
  })

  after(async () => {
    await db.cleanup()
    await db.disconnect()
  })

  beforeEach(async () => {
    // Clean up any existing test tables
    await db.query('DROP TABLE IF EXISTS versioning CASCADE')
    await db.query('DROP TABLE IF EXISTS versioning_history CASCADE')
    await db.query('DROP TABLE IF EXISTS structure CASCADE')
    await db.query('DROP TABLE IF EXISTS structure_history CASCADE')
    await db.query('DROP TABLE IF EXISTS test_table CASCADE')
    await db.query('DROP TABLE IF EXISTS test_table_history CASCADE')
  })

  describe('Basic Versioning Functionality', () => {
    test('should create versioned table with static trigger', async () => {
      // Create main table
      await db.query(`
        CREATE TABLE versioning (
          a bigint, 
          "b b" date, 
          sys_period tstzrange
        )
      `)

      // Create history table
      await db.query(`
        CREATE TABLE versioning_history (
          a bigint, 
          c date, 
          sys_period tstzrange
        )
      `)

      // Use static generator to create trigger
      const triggerResult = await db.query(`
        SELECT generate_static_versioning_trigger(
          'versioning',
          'versioning_history', 
          'sys_period',
          false,
          false,
          false,
          false
        ) as trigger_sql
      `)

      ok(triggerResult.rows.length > 0)
      ok(
        triggerResult.rows[0].trigger_sql.includes('CREATE OR REPLACE FUNCTION')
      )

      // Execute the generated trigger
      await db.query(triggerResult.rows[0].trigger_sql)

      // Verify table exists
      const tableExists = await db.tableExists('versioning')
      ok(tableExists)
    })

    test('should handle INSERT operations correctly', async () => {
      await setupBasicVersioningTable(db)

      const beforeTimestamp = await db.getCurrentTimestamp()
      await db.sleep(0.01) // Small delay to ensure timestamp difference

      // Insert data
      await db.executeTransaction(['INSERT INTO versioning (a) VALUES (3)'])

      const afterTimestamp = await db.getCurrentTimestamp()

      // Check main table
      const mainResult = await db.query(
        `
        SELECT a, "b b", lower(sys_period) >= $1 AND lower(sys_period) <= $2 as timestamp_ok
        FROM versioning 
        WHERE a = 3
        ORDER BY a, sys_period
      `,
        [beforeTimestamp, afterTimestamp]
      )

      deepStrictEqual(mainResult.rows.length, 1)
      deepStrictEqual(mainResult.rows[0].a, '3')
      ok(mainResult.rows[0].timestamp_ok)

      // History table should be empty for INSERT
      const historyResult = await db.query(
        'SELECT * FROM versioning_history ORDER BY a, sys_period'
      )
      deepStrictEqual(historyResult.rows.length, 0)
    })

    test('should handle UPDATE operations correctly', async () => {
      await setupBasicVersioningTable(db)

      // Insert initial data
      await db.executeTransaction(['INSERT INTO versioning (a) VALUES (3)'])

      await db.sleep(0.1) // Ensure timestamp difference

      const beforeUpdateTimestamp = await db.getCurrentTimestamp()

      // Update data
      await db.executeTransaction(['UPDATE versioning SET a = 4 WHERE a = 3'])

      const afterUpdateTimestamp = await db.getCurrentTimestamp()

      // Check main table has updated value
      const mainResult = await db.query(
        `
        SELECT a, "b b", lower(sys_period) >= $1 AND lower(sys_period) <= $2 as timestamp_ok
        FROM versioning 
        ORDER BY a, sys_period
      `,
        [beforeUpdateTimestamp, afterUpdateTimestamp]
      )

      const currentRow = mainResult.rows.find(row => row.a === '4')
      ok(currentRow, 'Updated row should exist in main table')
      ok(currentRow.timestamp_ok, 'Timestamp should be recent')

      // Check history table has old value
      const historyResult = await db.query(
        `
        SELECT a, c, upper(sys_period) >= $1 AND upper(sys_period) <= $2 as timestamp_ok
        FROM versioning_history 
        ORDER BY a, sys_period
      `,
        [beforeUpdateTimestamp, afterUpdateTimestamp]
      )

      deepStrictEqual(historyResult.rows.length, 1)
      deepStrictEqual(historyResult.rows[0].a, '3')
      ok(
        historyResult.rows[0].timestamp_ok,
        'History timestamp should be recent'
      )
    })

    test('should handle DELETE operations correctly', async () => {
      await setupBasicVersioningTable(db)

      // Insert and update to create some history
      await db.executeTransaction(['INSERT INTO versioning (a) VALUES (3)'])

      await db.sleep(0.1)

      await db.executeTransaction(['UPDATE versioning SET a = 4 WHERE a = 3'])

      await db.sleep(0.1)

      const beforeDeleteTimestamp = await db.getReliableTimestamp()

      // Delete data
      await db.executeTransaction(['DELETE FROM versioning WHERE a = 4'])

      const afterDeleteTimestamp = await db.getReliableTimestamp()

      // Main table should be empty (or not contain deleted row)
      const mainResult = await db.query('SELECT * FROM versioning WHERE a = 4')
      deepStrictEqual(mainResult.rows.length, 0)

      // History table should contain the deleted row with more robust timestamp checking
      const historyResult = await db.query(
        `
        SELECT a, c, upper(sys_period) as delete_timestamp
        FROM versioning_history 
        WHERE a = 4
        ORDER BY a, sys_period
      `
      )

      ok(historyResult.rows.length > 0, 'Deleted row should be in history')

      // Use more robust timestamp checking with tolerance
      const deletedRow = historyResult.rows[historyResult.rows.length - 1] // Get the latest row
      const deleteTimestamp = new Date(deletedRow.delete_timestamp)
      const isInRange = await db.isTimestampInRange(
        deleteTimestamp,
        beforeDeleteTimestamp,
        afterDeleteTimestamp,
        2000 // 2 second tolerance for timing issues
      )

      ok(
        isInRange,
        `Delete timestamp ${deleteTimestamp} should be between ${beforeDeleteTimestamp} and ${afterDeleteTimestamp}`
      )
    })
  })

  describe('Advanced Versioning Features', () => {
    test('should ignore unchanged values when configured', async () => {
      // Create table with ignore_unchanged_values = true
      await db.query(`
        CREATE TABLE versioning (
          a bigint, 
          b bigint, 
          sys_period tstzrange
        )
      `)

      await db.query(`
        CREATE TABLE versioning_history (
          a bigint, 
          b bigint, 
          sys_period tstzrange
        )
      `)

      // Generate trigger with ignore_unchanged_values = true
      const triggerResult = await db.query(`
        SELECT generate_static_versioning_trigger(
          'versioning',
          'versioning_history', 
          'sys_period',
          true,  -- ignore_unchanged_values
          false,
          false,
          false
        ) as trigger_sql
      `)

      await db.query(triggerResult.rows[0].trigger_sql)

      // Insert initial data
      await db.executeTransaction([
        "INSERT INTO versioning (a, b, sys_period) VALUES (1, 1, tstzrange('-infinity', NULL))",
        "INSERT INTO versioning (a, b, sys_period) VALUES (2, 2, tstzrange('2000-01-01', NULL))"
      ])

      // Update with no actual changes
      await db.executeTransaction(['UPDATE versioning SET b = 2 WHERE a = 2'])

      // History should be empty since no real changes occurred
      const historyResult = await db.query(
        'SELECT * FROM versioning_history ORDER BY a, sys_period'
      )
      deepStrictEqual(
        historyResult.rows.length,
        0,
        'No history should be created for unchanged values'
      )

      await db.sleep(0.1)

      // Update with actual changes
      await db.executeTransaction(['UPDATE versioning SET b = 3 WHERE a = 2'])

      // History should now contain the change
      const historyAfterChange = await db.query(
        'SELECT * FROM versioning_history ORDER BY a, sys_period'
      )
      ok(
        historyAfterChange.rows.length > 0,
        'History should be created for actual changes'
      )
    })

    test('should include current version in history when configured', async () => {
      await db.query(`
        CREATE TABLE versioning (
          a bigint, 
          sys_period tstzrange
        )
      `)

      await db.query(`
        CREATE TABLE versioning_history (
          a bigint, 
          sys_period tstzrange
        )
      `)

      // Generate trigger with include_current_version_in_history = true
      const triggerResult = await db.query(`
        SELECT generate_static_versioning_trigger(
          'versioning',
          'versioning_history', 
          'sys_period',
          false,
          true,  -- include_current_version_in_history
          false,
          false
        ) as trigger_sql
      `)

      await db.query(triggerResult.rows[0].trigger_sql)

      // Insert data
      await db.executeTransaction(['INSERT INTO versioning (a) VALUES (1)'])

      // Check that current version is also in history
      const historyResult = await db.query(
        'SELECT * FROM versioning_history ORDER BY a, sys_period'
      )
      ok(
        historyResult.rows.length > 0,
        'Current version should be in history table'
      )

      const mainResult = await db.query(
        'SELECT * FROM versioning ORDER BY a, sys_period'
      )
      deepStrictEqual(
        mainResult.rows.length,
        1,
        'Main table should have current version'
      )
    })

    test('should handle custom system time', async () => {
      await setupBasicVersioningTable(db)

      // Set custom system time
      const customTime = '2023-01-01 12:00:00.000000'
      await db.query(`SET user_defined.system_time = '${customTime}'`)

      // Insert data
      await db.executeTransaction(['INSERT INTO versioning (a) VALUES (100)'])

      // Check that the custom timestamp was used
      const result = await db.query(`
        SELECT a, lower(sys_period) as start_time
        FROM versioning 
        WHERE a = 100
      `)

      deepStrictEqual(result.rows.length, 1)
      const startTime = new Date(result.rows[0].start_time)
      const expectedTime = new Date(customTime)

      // Allow for small differences due to parsing
      const timeDiff = Math.abs(startTime.getTime() - expectedTime.getTime())
      ok(timeDiff < 1000, 'Custom system time should be used')

      // Reset system time
      await db.query(`RESET user_defined.system_time`)
    })
  })

  describe('Error Handling', () => {
    test('should reject invalid system period types', async () => {
      await db.query(`
        DROP TABLE IF EXISTS invalid_table CASCADE;

        CREATE TABLE invalid_table (
          a bigint, 
          sys_period text  -- Wrong type!
        )
      `)

      await db.query(`
        DROP TABLE IF EXISTS invalid_table_history CASCADE;

        CREATE TABLE invalid_table_history (
          a bigint, 
          sys_period tstzrange
        )
      `)

      // Should throw error when generating trigger
      await rejects(async () => {
        await db.query(`
          SELECT generate_static_versioning_trigger(
            'invalid_table',
            'invalid_table_history', 
            'sys_period',
            false,
            false,
            false,
            false
          )
        `)
      })
    })

    test('should reject operations on missing history table', async () => {
      await db.query(`
        CREATE TABLE versioning (
          a bigint, 
          sys_period tstzrange
        )
      `)

      // No history table created

      // Should throw error when generating trigger
      await rejects(async () => {
        await db.query(`
          SELECT generate_static_versioning_trigger(
            'versioning',
            'nonexistent_history', 
            'sys_period',
            false,
            false,
            false,
            false
          )
        `)
      })
    })

    test('should reject invalid system period values', async () => {
      await setupBasicVersioningTable(db)

      // Try to insert invalid system period
      await rejects(async () => {
        await db.executeTransaction([
          "INSERT INTO versioning (a, sys_period) VALUES (1, tstzrange('2023-01-01', '2022-01-01'))" // Invalid range
        ])
      })
    })
  })

  describe('Schema Compatibility', () => {
    test('should work with different column names and types', async () => {
      await db.query(`
        CREATE TABLE structure (
          a bigint, 
          "b b" date, 
          d text, 
          sys_period tstzrange
        )
      `)

      await db.query(`
        CREATE TABLE structure_history (
          a bigint, 
          "b b" date, 
          d text, 
          sys_period tstzrange
        )
      `)

      const triggerResult = await db.query(`
        SELECT generate_static_versioning_trigger(
          'structure',
          'structure_history', 
          'sys_period',
          false,
          false,
          false,
          false
        ) as trigger_sql
      `)

      await db.query(triggerResult.rows[0].trigger_sql)

      // Test with various data types
      await db.executeTransaction([
        "INSERT INTO structure (a, \"b b\", d) VALUES (1, '2000-01-01', 'test')"
      ])

      await db.sleep(0.1)

      await db.executeTransaction([
        "UPDATE structure SET d = 'updated' WHERE a = 1"
      ])

      const historyResult = await db.query(
        'SELECT * FROM structure_history ORDER BY a, sys_period'
      )
      deepStrictEqual(historyResult.rows.length, 1)
      deepStrictEqual(historyResult.rows[0].d, 'test')

      const mainResult = await db.query(
        'SELECT * FROM structure ORDER BY a, sys_period'
      )
      deepStrictEqual(mainResult.rows.length, 1)
      deepStrictEqual(mainResult.rows[0].d, 'updated')
    })

    test('should handle tables with different schemas', async () => {
      await db.query('DROP SCHEMA IF EXISTS test_schema CASCADE')

      await db.query('CREATE SCHEMA test_schema')

      await db.query(`
        CREATE TABLE test_schema.versioning (
          a bigint, 
          sys_period tstzrange
        )
      `)

      await db.query(`
        CREATE TABLE test_schema.versioning_history (
          a bigint, 
          sys_period tstzrange
        )
      `)

      const triggerResult = await db.query(`
        SELECT generate_static_versioning_trigger(
          'test_schema.versioning',
          'test_schema.versioning_history', 
          'sys_period',
          false,
          false,
          false,
          false
        ) as trigger_sql
      `)

      await db.query(triggerResult.rows[0].trigger_sql)

      // Test operations
      await db.executeTransaction([
        'INSERT INTO test_schema.versioning (a) VALUES (1)'
      ])

      const result = await db.query('SELECT * FROM test_schema.versioning')
      deepStrictEqual(result.rows.length, 1)

      // Cleanup
      await db.query('DROP SCHEMA test_schema CASCADE')
    })
  })

  describe('Performance and Edge Cases', () => {
    test('should handle multiple rapid updates', async () => {
      await setupBasicVersioningTable(db, false)

      // Insert initial data
      await db.executeTransaction(['INSERT INTO versioning (a) VALUES (1)'])

      // Perform multiple rapid updates
      for (let i = 2; i <= 10; i++) {
        await db.executeTransaction([
          `UPDATE versioning SET a = ${i} WHERE a = ${i - 1}`
        ])
        await db.sleep(0.01) // Small delay to ensure timestamp progression
      }

      // Check final state
      const mainResult = await db.query('SELECT * FROM versioning ORDER BY a')
      deepStrictEqual(mainResult.rows.length, 1)
      deepStrictEqual(mainResult.rows[0].a, '10')

      // Check history contains all intermediate values
      const historyResult = await db.query(
        'SELECT * FROM versioning_history ORDER BY a, sys_period'
      )
      deepStrictEqual(historyResult.rows.length, 9) // 9 updates = 9 history records
    })

    test('should handle concurrent transaction simulation', async () => {
      await setupBasicVersioningTable(db, false)

      // Insert initial data
      await db.executeTransaction([
        'INSERT INTO versioning (a) VALUES (1)',
        'INSERT INTO versioning (a) VALUES (2)'
      ])

      // Simulate concurrent updates (sequential for testing)
      await db.sleep(0.1)

      await db.executeTransaction(['UPDATE versioning SET a = 10 WHERE a = 1'])

      await db.executeTransaction(['UPDATE versioning SET a = 20 WHERE a = 2'])

      // Verify both updates worked
      const mainResult = await db.query('SELECT * FROM versioning ORDER BY a')
      deepStrictEqual(mainResult.rows.length, 2)

      const historyResult = await db.query(
        'SELECT * FROM versioning_history ORDER BY a, sys_period'
      )
      deepStrictEqual(historyResult.rows.length, 2)
    })
  })
})

// Helper function to set up basic versioning table
async function setupBasicVersioningTable(
  db: DatabaseHelper,
  withInitialData: boolean = true
): Promise<void> {
  await db.query(`
    CREATE TABLE versioning (
      a bigint, 
      "b b" date, 
      sys_period tstzrange
    )
  `)

  await db.query(`
    CREATE TABLE versioning_history (
      a bigint, 
      c date, 
      sys_period tstzrange
    )
  `)

  // Insert some initial data
  if (withInitialData) {
    await db.query(`
      INSERT INTO versioning (a, "b b", sys_period) VALUES (1, '2000-01-01', tstzrange('-infinity', NULL))
    `)
    await db.query(`
      INSERT INTO versioning (a, "b b", sys_period) VALUES (2, '2000-01-02', tstzrange('2000-01-01', NULL))
    `)
  }

  // Generate and execute static trigger
  await db.query(`
    CALL render_versioning_trigger(
      p_table_name => 'versioning',
      p_history_table => 'versioning_history', 
      p_sys_period => 'sys_period',
      p_ignore_unchanged_values => false,
      p_include_current_version_in_history => false,
      p_mitigate_update_conflicts => false,
      p_enable_migration_mode => false
    )
  `)
}
