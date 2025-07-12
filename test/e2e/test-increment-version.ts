import { deepStrictEqual, ok } from 'node:assert'
import { describe, test, before, after, beforeEach } from 'node:test'
import { DatabaseHelper } from './db-helper.js'

describe('Increment Version E2E Tests', () => {
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
    await db.query('DROP TABLE IF EXISTS increment_version_test CASCADE')
    await db.query(
      'DROP TABLE IF EXISTS increment_version_test_history CASCADE'
    )
    await db.query(
      'DROP TABLE IF EXISTS increment_version_with_history_test CASCADE'
    )
    await db.query(
      'DROP TABLE IF EXISTS increment_version_with_history_test_history CASCADE'
    )
  })

  describe('Basic Increment Version Functionality', () => {
    test('should increment version on INSERT, UPDATE, and DELETE', async () => {
      // Create tables with version column
      await db.query(`
        CREATE TABLE increment_version_test (
          id serial primary key,
          data text,
          version integer DEFAULT 1,
          sys_period tstzrange
        )
      `)

      await db.query(`
        CREATE TABLE increment_version_test_history (
          id integer,
          data text,
          version integer,
          sys_period tstzrange
        )
      `)

      // Generate trigger with increment_version = true
      await db.query(`
        CALL render_versioning_trigger(
          table_name => 'increment_version_test',
          history_table => 'increment_version_test_history',
          sys_period => 'sys_period',
          increment_version => true
        )
      `)

      // Test INSERT
      await db.executeTransaction([
        "INSERT INTO increment_version_test (data) VALUES ('initial version')"
      ])

      let result = await db.query(
        'SELECT data, version FROM increment_version_test'
      )
      deepStrictEqual(result.rows.length, 1)
      deepStrictEqual(result.rows[0].data, 'initial version')
      deepStrictEqual(result.rows[0].version, 1)

      // History should be empty for INSERT
      result = await db.query(
        'SELECT data, version FROM increment_version_test_history'
      )
      deepStrictEqual(result.rows.length, 0)

      // Test UPDATE
      await db.executeTransaction([
        "UPDATE increment_version_test SET data = 'second version' WHERE id = 1"
      ])

      result = await db.query(
        'SELECT data, version FROM increment_version_test'
      )
      deepStrictEqual(result.rows.length, 1)
      deepStrictEqual(result.rows[0].data, 'second version')
      deepStrictEqual(result.rows[0].version, 2)

      // History should contain the old version
      result = await db.query(
        'SELECT data, version, upper(sys_period) IS NOT NULL as history_ended FROM increment_version_test_history'
      )
      deepStrictEqual(result.rows.length, 1)
      deepStrictEqual(result.rows[0].data, 'initial version')
      deepStrictEqual(result.rows[0].version, 1)
      ok(result.rows[0].history_ended)

      // Test another UPDATE
      await db.executeTransaction([
        "UPDATE increment_version_test SET data = 'third version' WHERE id = 1"
      ])

      result = await db.query(
        'SELECT data, version FROM increment_version_test'
      )
      deepStrictEqual(result.rows.length, 1)
      deepStrictEqual(result.rows[0].data, 'third version')
      deepStrictEqual(result.rows[0].version, 3)

      // History should contain both old versions
      result = await db.query(
        'SELECT data, version, upper(sys_period) IS NOT NULL as history_ended FROM increment_version_test_history ORDER BY version'
      )
      deepStrictEqual(result.rows.length, 2)
      deepStrictEqual(result.rows[0].data, 'initial version')
      deepStrictEqual(result.rows[0].version, 1)
      deepStrictEqual(result.rows[1].data, 'second version')
      deepStrictEqual(result.rows[1].version, 2)

      // Test DELETE
      await db.executeTransaction([
        'DELETE FROM increment_version_test WHERE id = 1'
      ])

      result = await db.query('SELECT * FROM increment_version_test')
      deepStrictEqual(result.rows.length, 0)

      // History should contain all versions
      result = await db.query(
        'SELECT data, version, upper(sys_period) IS NOT NULL as history_ended FROM increment_version_test_history ORDER BY version'
      )
      deepStrictEqual(result.rows.length, 3)
      deepStrictEqual(result.rows[0].data, 'initial version')
      deepStrictEqual(result.rows[0].version, 1)
      deepStrictEqual(result.rows[1].data, 'second version')
      deepStrictEqual(result.rows[1].version, 2)
      deepStrictEqual(result.rows[2].data, 'third version')
      deepStrictEqual(result.rows[2].version, 3)
    })

    test('should work with include_current_version_in_history', async () => {
      // Create tables with version column
      await db.query(`
        CREATE TABLE increment_version_with_history_test (
          id serial primary key,
          data text,
          version integer DEFAULT 1,
          sys_period tstzrange
        )
      `)

      await db.query(`
        CREATE TABLE increment_version_with_history_test_history (
          id integer,
          data text,
          version integer,
          sys_period tstzrange
        )
      `)

      // Generate trigger with increment_version and include_current_version_in_history
      await db.query(`
        CALL render_versioning_trigger(
          table_name => 'increment_version_with_history_test',
          history_table => 'increment_version_with_history_test_history',
          sys_period => 'sys_period',
          include_current_version_in_history => true,
          increment_version => true
        )
      `)

      // Test INSERT
      await db.executeTransaction([
        "INSERT INTO increment_version_with_history_test (data) VALUES ('initial version')"
      ])

      let result = await db.query(
        'SELECT data, version FROM increment_version_with_history_test'
      )
      deepStrictEqual(result.rows.length, 1)
      deepStrictEqual(result.rows[0].data, 'initial version')
      deepStrictEqual(result.rows[0].version, 1)

      // History should contain current version for INSERT
      result = await db.query(
        'SELECT data, version FROM increment_version_with_history_test_history'
      )
      deepStrictEqual(result.rows.length, 1)
      deepStrictEqual(result.rows[0].data, 'initial version')
      deepStrictEqual(result.rows[0].version, 1)

      // Test UPDATE
      await db.executeTransaction([
        "UPDATE increment_version_with_history_test SET data = 'second version' WHERE id = 1"
      ])

      result = await db.query(
        'SELECT data, version FROM increment_version_with_history_test'
      )
      deepStrictEqual(result.rows.length, 1)
      deepStrictEqual(result.rows[0].data, 'second version')
      deepStrictEqual(result.rows[0].version, 2)

      // History should contain both old and current versions
      result = await db.query(
        'SELECT data, version, upper(sys_period) IS NOT NULL as history_ended FROM increment_version_with_history_test_history ORDER BY version'
      )
      deepStrictEqual(result.rows.length, 2)
      deepStrictEqual(result.rows[0].data, 'initial version')
      deepStrictEqual(result.rows[0].version, 1)
      ok(result.rows[0].history_ended)
      deepStrictEqual(result.rows[1].data, 'second version')
      deepStrictEqual(result.rows[1].version, 2)
      ok(!result.rows[1].history_ended) // Current version has open period
    })

    test('should handle custom version column name', async () => {
      // Create tables with custom version column name
      await db.query(`
        CREATE TABLE increment_version_test (
          id serial primary key,
          data text,
          rev_number integer DEFAULT 1,
          sys_period tstzrange
        )
      `)

      await db.query(`
        CREATE TABLE increment_version_test_history (
          id integer,
          data text,
          rev_number integer,
          sys_period tstzrange
        )
      `)

      // Generate trigger with custom version column name
      await db.query(`
        CALL render_versioning_trigger(
          table_name => 'increment_version_test',
          history_table => 'increment_version_test_history',
          sys_period => 'sys_period',
          increment_version => true,
          version_column_name => 'rev_number'
        )
      `)

      // Test functionality
      await db.executeTransaction([
        "INSERT INTO increment_version_test (data) VALUES ('test data')"
      ])

      await db.executeTransaction([
        "UPDATE increment_version_test SET data = 'updated data' WHERE id = 1"
      ])

      const result = await db.query(
        'SELECT data, rev_number FROM increment_version_test'
      )
      deepStrictEqual(result.rows.length, 1)
      deepStrictEqual(result.rows[0].data, 'updated data')
      deepStrictEqual(result.rows[0].rev_number, 2)
    })

    test('should validate version column exists and is integer', async () => {
      // Create table without version column
      await db.query(`
        CREATE TABLE increment_version_test (
          id serial primary key,
          data text,
          sys_period tstzrange
        )
      `)

      await db.query(`
        CREATE TABLE increment_version_test_history (
          id integer,
          data text,
          sys_period tstzrange
        )
      `)

      // Should fail when trying to generate trigger without version column
      try {
        await db.query(`
          CALL render_versioning_trigger(
            table_name => 'increment_version_test',
            history_table => 'increment_version_test_history',
            sys_period => 'sys_period',
            increment_version => true
          )
        `)
        throw new Error('Should have failed')
      } catch (error: any) {
        ok(
          error.message.includes('does not contain version column'),
          'Should fail with missing version column error'
        )
      }
    })

    test('should work with render_versioning_trigger procedure', async () => {
      // Create tables with version column
      await db.query(`
        CREATE TABLE increment_version_test (
          id serial primary key,
          data text,
          version integer DEFAULT 1,
          sys_period tstzrange
        )
      `)

      await db.query(`
        CREATE TABLE increment_version_test_history (
          id integer,
          data text,
          version integer,
          sys_period tstzrange
        )
      `)

      // Use render procedure with increment_version
      await db.query(`
        CALL render_versioning_trigger(
          table_name => 'increment_version_test',
          history_table => 'increment_version_test_history',
          sys_period => 'sys_period',
          increment_version => true
        )
      `)

      // Test functionality
      await db.executeTransaction([
        "INSERT INTO increment_version_test (data) VALUES ('test data')"
      ])

      await db.executeTransaction([
        "UPDATE increment_version_test SET data = 'updated data' WHERE id = 1"
      ])

      const result = await db.query(
        'SELECT data, version FROM increment_version_test'
      )
      deepStrictEqual(result.rows.length, 1)
      deepStrictEqual(result.rows[0].data, 'updated data')
      deepStrictEqual(result.rows[0].version, 2)
    })
  })

  describe('Integration with Metadata and Event Triggers', () => {
    test('should work with versioning metadata table', async () => {
      // Create tables with version column
      await db.query(`
        CREATE TABLE increment_version_test (
          id serial primary key,
          data text,
          version integer DEFAULT 1,
          sys_period tstzrange
        )
      `)

      await db.query(`
        CREATE TABLE increment_version_test_history (
          id integer,
          data text,
          version integer,
          sys_period tstzrange
        )
      `)

      // Register in metadata table with increment_version
      await db.query(`
        INSERT INTO versioning_tables_metadata (
          table_name, 
          table_schema, 
          history_table, 
          history_table_schema,
          sys_period,
          ignore_unchanged_values,
          include_current_version_in_history,
          mitigate_update_conflicts,
          enable_migration_mode,
          increment_version,
          version_column_name
        ) VALUES (
          'increment_version_test', 
          'public', 
          'increment_version_test_history',
          'public',
          'sys_period',
          false,
          false,
          false,
          false,
          true,
          'version'
        )
      `)

      // Generate initial trigger
      await db.query(`
        CALL render_versioning_trigger(
          table_name => 'increment_version_test',
          history_table => 'increment_version_test_history',
          sys_period => 'sys_period',
          increment_version => true
        )
      `)

      // Test functionality
      await db.executeTransaction([
        "INSERT INTO increment_version_test (data) VALUES ('test data')"
      ])

      // Add a column to trigger re-rendering (event trigger should handle this)
      await db.query(
        'ALTER TABLE increment_version_test ADD COLUMN description text'
      )
      await db.query(
        'ALTER TABLE increment_version_test_history ADD COLUMN description text'
      )

      // Test that versioning still works after schema change
      await db.executeTransaction([
        "UPDATE increment_version_test SET data = 'updated data', description = 'test desc' WHERE id = 1"
      ])

      const result = await db.query(
        'SELECT data, description, version FROM increment_version_test'
      )
      deepStrictEqual(result.rows.length, 1)
      deepStrictEqual(result.rows[0].data, 'updated data')
      deepStrictEqual(result.rows[0].description, 'test desc')
      deepStrictEqual(result.rows[0].version, 2)

      // Check history contains old version
      const historyResult = await db.query(
        'SELECT data, version FROM increment_version_test_history'
      )
      deepStrictEqual(historyResult.rows.length, 1)
      deepStrictEqual(historyResult.rows[0].data, 'test data')
      deepStrictEqual(historyResult.rows[0].version, 1)
    })
  })
})
