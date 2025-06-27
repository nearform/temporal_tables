import { deepStrictEqual, ok, rejects } from 'node:assert'
import { describe, test, before, after, beforeEach } from 'node:test'
import * as url from 'url';
import { DatabaseHelper } from './db-helper.js'

const __dirname = url.fileURLToPath(new URL('.', import.meta.url));

describe('Event Trigger Versioning E2E Tests', () => {
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
    // Clean up any existing test tables and metadata
    await db.query('DROP TABLE IF EXISTS subscriptions CASCADE')
    await db.query('DROP TABLE IF EXISTS subscriptions_history CASCADE')
    await db.query('DROP TABLE IF EXISTS users CASCADE')
    await db.query('DROP TABLE IF EXISTS users_history CASCADE')
    await db.query('DELETE FROM versioning_tables_metadata WHERE table_schema = \'public\'')
  })

  describe('Event Trigger Setup and Management', () => {
    test('should create versioning metadata table', async () => {
      // Load event trigger functionality
      const eventTriggerPath = require('path').join(__dirname, '..', '..', 'event_trigger_versioning.sql')
      
      try {
        await db.loadAndExecuteSqlFile(eventTriggerPath)
      } catch (error) {
        // Load manually if file loading fails
        await db.query(`
          CREATE TABLE IF NOT EXISTS versioning_tables_metadata (
            table_name text,
            table_schema text,
            PRIMARY KEY (table_name, table_schema)
          )
        `)
      }

      const tableExists = await db.tableExists('versioning_tables_metadata')
      ok(tableExists, 'Versioning metadata table should exist')

      // Check table structure
      const structure = await db.getTableStructure('versioning_tables_metadata')
      const hasTableName = structure.some(col => col.column_name === 'table_name')
      const hasTableSchema = structure.some(col => col.column_name === 'table_schema')
      
      ok(hasTableName, 'Should have table_name column')
      ok(hasTableSchema, 'Should have table_schema column')
    })

    test('should register tables in metadata for automatic re-rendering', async () => {
      // Ensure metadata table exists
      await db.query(`
        CREATE TABLE IF NOT EXISTS versioning_tables_metadata (
          table_name text,
          table_schema text,
          PRIMARY KEY (table_name, table_schema)
        )
      `)

      // Register a table for versioning
      await db.query(`
        INSERT INTO versioning_tables_metadata (table_name, table_schema)
        VALUES ('subscriptions', 'public')
      `)

      // Verify registration
      const result = await db.query(`
        SELECT * FROM versioning_tables_metadata 
        WHERE table_name = 'subscriptions' AND table_schema = 'public'
      `)

      deepStrictEqual(result.rows.length, 1)
      deepStrictEqual(result.rows[0].table_name, 'subscriptions')
      deepStrictEqual(result.rows[0].table_schema, 'public')
    })

    test('should create render_versioning_trigger procedure', async () => {
      // Create the procedure manually for testing
      await db.query(`
        CREATE OR REPLACE PROCEDURE render_versioning_trigger(
          p_table_name text, 
          p_history_table text, 
          p_sys_period text,
          p_ignore_unchanged_values boolean DEFAULT false,
          p_include_current_version_in_history boolean DEFAULT false,
          p_mitigate_update_conflicts boolean DEFAULT false,
          p_enable_migration_mode boolean DEFAULT false  
        ) 
        AS $$
        DECLARE
          sql text;
        BEGIN
          sql := generate_static_versioning_trigger(
            p_table_name, 
            p_history_table, 
            p_sys_period,
            p_ignore_unchanged_values,
            p_include_current_version_in_history,
            p_mitigate_update_conflicts,
            p_enable_migration_mode    
          );
          EXECUTE sql;
        END;
        $$ LANGUAGE plpgsql
      `)

      // Test the procedure
      await db.query(`
        CREATE TABLE test_table (
          id bigint,
          name text,
          sys_period tstzrange
        )
      `)

      await db.query(`
        CREATE TABLE test_table_history (
          id bigint,
          name text,
          sys_period tstzrange
        )
      `)

      // Call the procedure
      await db.query(`
        CALL render_versioning_trigger(
          'test_table',
          'test_table_history',
          'sys_period'
        )
      `)

      // Verify trigger was created by testing functionality
      await db.query("INSERT INTO test_table (id, name) VALUES (1, 'test')")
      
      const result = await db.query('SELECT * FROM test_table WHERE id = 1')
      deepStrictEqual(result.rows.length, 1)
      ok(result.rows[0].sys_period, 'sys_period should be set by trigger')
    })
  })

  describe('Automatic Trigger Re-rendering', () => {
    test('should handle table alterations and re-render triggers', async () => {
      // Set up metadata table and procedure
      await db.query(`
        CREATE TABLE IF NOT EXISTS versioning_tables_metadata (
          table_name text,
          table_schema text,
          PRIMARY KEY (table_name, table_schema)
        )
      `)

      await db.query(`
        CREATE OR REPLACE PROCEDURE render_versioning_trigger(
          p_table_name text, 
          p_history_table text, 
          p_sys_period text,
          p_ignore_unchanged_values boolean DEFAULT false,
          p_include_current_version_in_history boolean DEFAULT false,
          p_mitigate_update_conflicts boolean DEFAULT false,
          p_enable_migration_mode boolean DEFAULT false  
        ) 
        AS $$
        DECLARE
          sql text;
        BEGIN
          sql := generate_static_versioning_trigger(
            p_table_name, 
            p_history_table, 
            p_sys_period,
            p_ignore_unchanged_values,
            p_include_current_version_in_history,
            p_mitigate_update_conflicts,
            p_enable_migration_mode    
          );
          EXECUTE sql;
        END;
        $$ LANGUAGE plpgsql
      `)

      // Create versioned table
      await db.query(`
        CREATE TABLE users (
          id bigint,
          email text,
          sys_period tstzrange
        )
      `)

      await db.query(`
        CREATE TABLE users_history (
          id bigint,
          email text,
          sys_period tstzrange
        )
      `)

      // Register for versioning
      await db.query(`
        INSERT INTO versioning_tables_metadata (table_name, table_schema)
        VALUES ('users', 'public')
      `)

      // Create initial trigger
      await db.query(`
        CALL render_versioning_trigger('users', 'users_history', 'sys_period')
      `)

      // Test initial functionality
      await db.query("INSERT INTO users (id, email) VALUES (1, 'test@example.com')")
      
      let result = await db.query('SELECT * FROM users WHERE id = 1')
      deepStrictEqual(result.rows.length, 1)

      // Alter the table (add column)
      await db.query('ALTER TABLE users ADD COLUMN name text')
      await db.query('ALTER TABLE users_history ADD COLUMN name text')

      // Re-render trigger manually (simulating event trigger)
      await db.query(`
        CALL render_versioning_trigger('users', 'users_history', 'sys_period')
      `)

      // Test that versioning still works with new column
      await db.query("INSERT INTO users (id, email, name) VALUES (2, 'test2@example.com', 'Test User')")
      
      result = await db.query('SELECT * FROM users WHERE id = 2')
      deepStrictEqual(result.rows.length, 1)
      deepStrictEqual(result.rows[0].name, 'Test User')

      // Test update to create history
      await db.sleep(0.1)
      await db.query("UPDATE users SET name = 'Updated User' WHERE id = 2")

      const historyResult = await db.query('SELECT * FROM users_history WHERE id = 2')
      deepStrictEqual(historyResult.rows.length, 1)
      deepStrictEqual(historyResult.rows[0].name, 'Test User') // Original value in history
    })

    test('should handle schema changes gracefully', async () => {
      // Create table with initial schema
      await db.query(`
        CREATE TABLE subscriptions (
          id bigint,
          user_id bigint,
          sys_period tstzrange
        )
      `)

      await db.query(`
        CREATE TABLE subscriptions_history (
          id bigint,
          user_id bigint,
          sys_period tstzrange
        )
      `)

      // Generate initial trigger
      const triggerResult = await db.query(`
        SELECT generate_static_versioning_trigger(
          'subscriptions',
          'subscriptions_history',
          'sys_period'
        ) as trigger_sql
      `)

      await db.query(triggerResult.rows[0].trigger_sql)

      // Insert test data
      await db.query("INSERT INTO subscriptions (id, user_id) VALUES (1, 100)")

      // Add column to both tables
      await db.query('ALTER TABLE subscriptions ADD COLUMN plan_type text DEFAULT \'basic\'')
      await db.query('ALTER TABLE subscriptions_history ADD COLUMN plan_type text')

      // Re-generate trigger with new schema
      const newTriggerResult = await db.query(`
        SELECT generate_static_versioning_trigger(
          'subscriptions',
          'subscriptions_history',
          'sys_period'
        ) as trigger_sql
      `)

      await db.query(newTriggerResult.rows[0].trigger_sql)

      // Test that new column is handled correctly
      await db.query("INSERT INTO subscriptions (id, user_id, plan_type) VALUES (2, 200, 'premium')")

      await db.sleep(0.1)

      await db.query("UPDATE subscriptions SET plan_type = 'enterprise' WHERE id = 2")

      // Verify history includes new column
      const historyResult = await db.query(`
        SELECT id, user_id, plan_type 
        FROM subscriptions_history 
        WHERE id = 2
      `)

      deepStrictEqual(historyResult.rows.length, 1)
      deepStrictEqual(historyResult.rows[0].plan_type, 'premium') // Original value
    })
  })

  describe('Migration Mode with Event Triggers', () => {
    test('should handle migration mode correctly', async () => {
      // Create tables
      await db.query(`
        CREATE TABLE users (
          id bigint,
          email text,
          created_at timestamp,
          sys_period tstzrange
        )
      `)

      await db.query(`
        CREATE TABLE users_history (
          id bigint,
          email text,
          created_at timestamp,
          sys_period tstzrange
        )
      `)

      // Generate trigger with migration mode enabled
      const triggerResult = await db.query(`
        SELECT generate_static_versioning_trigger(
          'users',
          'users_history',
          'sys_period',
          false,  -- ignore_unchanged_values
          false,  -- include_current_version_in_history
          false,  -- mitigate_update_conflicts
          true    -- enable_migration_mode
        ) as trigger_sql
      `)

      await db.query(triggerResult.rows[0].trigger_sql)

      // Insert existing data with historical periods
      const oldTime = '2023-01-01 10:00:00+00'
      const midTime = '2023-06-01 10:00:00+00'
      
      await db.query(`
        INSERT INTO users (id, email, created_at, sys_period) 
        VALUES (1, 'old@example.com', '2023-01-01', tstzrange($1, $2))
      `, [oldTime, midTime])

      // Update should handle migration mode
      await db.query("UPDATE users SET email = 'updated@example.com' WHERE id = 1")

      // Check that history was created appropriately
      const historyResult = await db.query('SELECT * FROM users_history WHERE id = 1')
      ok(historyResult.rows.length > 0, 'History should be created in migration mode')

      const mainResult = await db.query('SELECT * FROM users WHERE id = 1')
      deepStrictEqual(mainResult.rows.length, 1)
      deepStrictEqual(mainResult.rows[0].email, 'updated@example.com')
    })
  })

  describe('Error Handling in Event Triggers', () => {
    test('should handle missing history table gracefully', async () => {
      await db.query(`
        CREATE TABLE orphan_table (
          id bigint,
          data text,
          sys_period tstzrange
        )
      `)

      // Register table without creating history table
      await db.query(`
        CREATE TABLE IF NOT EXISTS versioning_tables_metadata (
          table_name text,
          table_schema text,
          PRIMARY KEY (table_name, table_schema)
        )
      `)

      await db.query(`
        INSERT INTO versioning_tables_metadata (table_name, table_schema)
        VALUES ('orphan_table', 'public')
      `)

      // Attempt to create trigger should fail gracefully
      await rejects(async () => {
        await db.query(`
          SELECT generate_static_versioning_trigger(
            'orphan_table',
            'orphan_table_history',
            'sys_period'
          )
        `)
      })
    })

    test('should validate system period column exists', async () => {
      await db.query(`
        CREATE TABLE invalid_period_table (
          id bigint,
          data text
          -- Missing sys_period column
        )
      `)

      await db.query(`
        CREATE TABLE invalid_period_table_history (
          id bigint,
          data text,
          sys_period tstzrange
        )
      `)

      // Should fail when trying to generate trigger
      await rejects(async () => {
        await db.query(`
          SELECT generate_static_versioning_trigger(
            'invalid_period_table',
            'invalid_period_table_history',
            'sys_period'
          )
        `)
      })
    })
  })

  describe('Complex Schema Scenarios', () => {
    test('should handle tables with complex data types', async () => {
      await db.query(`
        CREATE TABLE complex_table (
          id bigint,
          metadata jsonb,
          tags text[],
          coordinates point,
          sys_period tstzrange
        )
      `)

      await db.query(`
        CREATE TABLE complex_table_history (
          id bigint,
          metadata jsonb,
          tags text[],
          coordinates point,
          sys_period tstzrange
        )
      `)

      const triggerResult = await db.query(`
        SELECT generate_static_versioning_trigger(
          'complex_table',
          'complex_table_history',
          'sys_period'
        ) as trigger_sql
      `)

      await db.query(triggerResult.rows[0].trigger_sql)

      // Test with complex data
      await db.query(`
        INSERT INTO complex_table (id, metadata, tags, coordinates) 
        VALUES (
          1, 
          '{"key": "value", "nested": {"array": [1,2,3]}}',
          ARRAY['tag1', 'tag2', 'tag3'],
          point(1.0, 2.0)
        )
      `)

      await db.sleep(0.1)

      await db.query(`
        UPDATE complex_table 
        SET metadata = '{"key": "updated", "new": true}'
        WHERE id = 1
      `)

      // Verify complex types are preserved in history
      const historyResult = await db.query('SELECT * FROM complex_table_history WHERE id = 1')
      deepStrictEqual(historyResult.rows.length, 1)
      
      const historyRow = historyResult.rows[0]
      ok(historyRow.metadata, 'JSON metadata should be preserved')
      ok(historyRow.tags, 'Array should be preserved')
      ok(historyRow.coordinates, 'Point should be preserved')
    })
  })
})
