import { deepStrictEqual, ok, rejects } from 'node:assert'
import { describe, test, before, after, beforeEach } from 'node:test'
import * as url from 'url'
import { DatabaseHelper } from './db-helper.js'

const __dirname = url.fileURLToPath(new URL('.', import.meta.url))

describe('Legacy Versioning Function E2E Tests', () => {
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
    // Clean up any existing test tables
    await db.query('DROP TABLE IF EXISTS versioning CASCADE')
    await db.query('DROP TABLE IF EXISTS versioning_history CASCADE')
    await db.query('DROP TABLE IF EXISTS structure CASCADE')
    await db.query('DROP TABLE IF EXISTS structure_history CASCADE')
    await db.query('DROP TABLE IF EXISTS test_table CASCADE')
    await db.query('DROP TABLE IF EXISTS test_table_history CASCADE')
    await db.query('DROP TABLE IF EXISTS legacy_test CASCADE')
    await db.query('DROP TABLE IF EXISTS legacy_test_history CASCADE')
    await db.query('DROP TABLE IF EXISTS static_test CASCADE')
    await db.query('DROP TABLE IF EXISTS static_test_history CASCADE')
  })

  describe('Basic Legacy Versioning', () => {
    test('should work with legacy versioning function syntax', async () => {
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

      // Create trigger using legacy versioning function
      await db.query(`
        CREATE TRIGGER versioning_trigger
        BEFORE INSERT OR UPDATE OR DELETE ON versioning
        FOR EACH ROW EXECUTE PROCEDURE versioning('sys_period', 'versioning_history', false)
      `)

      // Insert some data before versioning is fully active
      await db.query(`
        INSERT INTO versioning (a, sys_period) VALUES (1, tstzrange('-infinity', NULL))
      `)

      await db.query(`
        INSERT INTO versioning (a, sys_period) VALUES (2, tstzrange('2000-01-01', NULL))
      `)

      // Test INSERT
      await db.executeTransaction(['INSERT INTO versioning (a) VALUES (3)'])

      const insertResult = await db.query(`
        SELECT a, "b b", lower(sys_period) = CURRENT_TIMESTAMP as is_current
        FROM versioning 
        WHERE a = 3
        ORDER BY a, sys_period
      `)

      deepStrictEqual(insertResult.rows.length, 1)
      deepStrictEqual(insertResult.rows[0].a, '3')

      // History should be empty for inserts
      const historyAfterInsert = await db.query(
        'SELECT * FROM versioning_history ORDER BY a, sys_period'
      )
      deepStrictEqual(historyAfterInsert.rows.length, 0)

      await db.sleep(0.1)

      // Test UPDATE
      await db.executeTransaction(['UPDATE versioning SET a = 4 WHERE a = 3'])

      const updateResult = await db.query(`
        SELECT a, "b b", lower(sys_period) = CURRENT_TIMESTAMP as is_current
        FROM versioning 
        ORDER BY a, sys_period
      `)

      const updatedRow = updateResult.rows.find(row => row.a === '4')
      ok(updatedRow, 'Updated row should exist')

      // History should contain old value
      const historyAfterUpdate = await db.query(`
        SELECT a, c, upper(sys_period) = CURRENT_TIMESTAMP as is_recent
        FROM versioning_history 
        ORDER BY a, sys_period
      `)

      deepStrictEqual(historyAfterUpdate.rows.length, 1)
      deepStrictEqual(historyAfterUpdate.rows[0].a, '3')

      await db.sleep(0.1)

      // Test DELETE
      await db.executeTransaction(['DELETE FROM versioning WHERE a = 4'])

      const mainAfterDelete = await db.query(
        'SELECT * FROM versioning WHERE a = 4'
      )
      deepStrictEqual(mainAfterDelete.rows.length, 0)

      const historyAfterDelete = await db.query(`
        SELECT a, c, upper(sys_period) = CURRENT_TIMESTAMP as is_recent
        FROM versioning_history 
        WHERE a = 4
        ORDER BY a, sys_period
      `)

      ok(historyAfterDelete.rows.length > 0, 'Deleted row should be in history')
    })

    test('should handle unchanged values option', async () => {
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

      // Create trigger with unchanged values detection enabled
      await db.query(`
        CREATE TRIGGER versioning_trigger
        BEFORE INSERT OR UPDATE OR DELETE ON versioning
        FOR EACH ROW EXECUTE PROCEDURE versioning('sys_period', 'versioning_history', false, true)
      `)

      // Insert initial data
      await db.query(`
        INSERT INTO versioning (a, b, sys_period) VALUES (1, 1, tstzrange('-infinity', NULL))
      `)

      await db.query(`
        INSERT INTO versioning (a, b, sys_period) VALUES (2, 2, tstzrange('2000-01-01', NULL))
      `)

      // Update with no actual changes - should be ignored
      await db.executeTransaction(['UPDATE versioning SET b = 2 WHERE a = 2'])

      const historyAfterNoChange = await db.query(
        'SELECT * FROM versioning_history ORDER BY a, sys_period'
      )
      deepStrictEqual(
        historyAfterNoChange.rows.length,
        0,
        'No history should be created for unchanged values'
      )

      await db.sleep(0.1)

      // Update with actual changes
      await db.executeTransaction(['UPDATE versioning SET b = 3 WHERE a = 2'])

      const historyAfterChange = await db.query(
        'SELECT * FROM versioning_history ORDER BY a, sys_period'
      )
      ok(
        historyAfterChange.rows.length > 0,
        'History should be created for actual changes'
      )
    })
  })

  describe('Custom System Time', () => {
    test('should respect custom system time setting', async () => {
      await setupLegacyVersioningTable(db)

      // Set custom system time
      const customTime = '2023-01-15 14:30:00.123456'
      await db.query(`SET user_defined.system_time = '${customTime}'`)

      await db.executeTransaction(['INSERT INTO versioning (a) VALUES (100)'])

      const result = await db.query(`
        SELECT a, lower(sys_period) as start_time
        FROM versioning 
        WHERE a = 100
      `)

      deepStrictEqual(result.rows.length, 1)

      // Verify custom timestamp was used (allowing for small parsing differences)
      const startTime = new Date(result.rows[0].start_time)
      const expectedTime = new Date(customTime)
      const timeDiff = Math.abs(startTime.getTime() - expectedTime.getTime())
      ok(timeDiff < 10000, 'Custom system time should be used')

      // Reset system time
      await db.query('RESET user_defined.system_time')
    })
  })

  describe('Schema Compatibility', () => {
    test('should work with quoted column names', async () => {
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

      await db.query(`
        CREATE TRIGGER versioning_trigger
        BEFORE INSERT OR UPDATE OR DELETE ON structure
        FOR EACH ROW EXECUTE PROCEDURE versioning('sys_period', 'structure_history', false)
      `)

      // Test with quoted column names
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
  })
})

// Helper function to set up basic legacy versioning table
async function setupLegacyVersioningTable(db: DatabaseHelper): Promise<void> {
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
  await db.query(`
    INSERT INTO versioning (a, sys_period) VALUES (1, tstzrange('-infinity', NULL))
  `)

  await db.query(`
    INSERT INTO versioning (a, sys_period) VALUES (2, tstzrange('2000-01-01', NULL))
  `)

  // Create legacy trigger
  await db.query(`
    CREATE TRIGGER versioning_trigger
    BEFORE INSERT OR UPDATE OR DELETE ON versioning
    FOR EACH ROW EXECUTE PROCEDURE versioning('sys_period', 'versioning_history', false)
  `)
}
