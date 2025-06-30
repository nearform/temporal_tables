import { deepStrictEqual, ok, rejects } from 'node:assert'
import { describe, test, before, after, beforeEach } from 'node:test'
import * as url from 'url'
import { DatabaseHelper } from './db-helper.js'

const __dirname = url.fileURLToPath(new URL('.', import.meta.url))

describe('Integration Tests - All Features', () => {
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
    // Clean up all test tables
    const tables = [
      'users',
      'users_history',
      'orders',
      'orders_history',
      'products',
      'products_history',
      'complex_scenario',
      'complex_scenario_history',
      'migration_test',
      'migration_test_history',
      'performance_test',
      'performance_test_history'
    ]

    for (const table of tables)
      await db.query(`DROP TABLE IF EXISTS ${table} CASCADE`)
  })

  describe('Real-world Scenario Testing', () => {
    test('should handle e-commerce order system with versioning', async () => {
      // Create users table
      await db.query(`
        CREATE TABLE users (
          id bigint PRIMARY KEY,
          email text UNIQUE NOT NULL,
          name text,
          created_at timestamp DEFAULT CURRENT_TIMESTAMP,
          sys_period tstzrange
        )
      `)

      await db.query(`
        CREATE TABLE users_history (
          id bigint,
          email text,
          name text,
          created_at timestamp,
          sys_period tstzrange
        )
      `)

      // Create orders table
      await db.query(`
        CREATE TABLE orders (
          id bigint PRIMARY KEY,
          user_id bigint REFERENCES users(id),
          total_amount decimal(10,2),
          status text DEFAULT 'pending',
          created_at timestamp DEFAULT CURRENT_TIMESTAMP,
          sys_period tstzrange
        )
      `)

      await db.query(`
        CREATE TABLE orders_history (
          id bigint,
          user_id bigint,
          total_amount decimal(10,2),
          status text,
          created_at timestamp,
          sys_period tstzrange
        )
      `)

      // Set up versioning for both tables
      const userTriggerResult = await db.query(`
        SELECT generate_static_versioning_trigger(
          'users',
          'users_history',
          'sys_period',
          true,  -- ignore unchanged values
          false,
          false,
          false
        ) as trigger_sql
      `)

      await db.query(userTriggerResult.rows[0].trigger_sql)

      const orderTriggerResult = await db.query(`
        SELECT generate_static_versioning_trigger(
          'orders',
          'orders_history',
          'sys_period',
          true,  -- ignore unchanged values
          false,
          false,
          false
        ) as trigger_sql
      `)

      await db.query(orderTriggerResult.rows[0].trigger_sql)

      // Simulate user registration
      await db.executeTransaction([
        "INSERT INTO users (id, email, name) VALUES (1, 'john@example.com', 'John Doe')"
      ])

      // Simulate order creation
      await db.executeTransaction([
        "INSERT INTO orders (id, user_id, total_amount, status) VALUES (100, 1, 299.99, 'pending')"
      ])

      await db.sleep(0.1)

      // Update user profile
      await db.executeTransaction([
        "UPDATE users SET name = 'John Smith' WHERE id = 1"
      ])

      // Process order through various states
      await db.sleep(0.05)
      await db.executeTransaction([
        "UPDATE orders SET status = 'processing' WHERE id = 100"
      ])

      await db.sleep(0.05)
      await db.executeTransaction([
        "UPDATE orders SET status = 'shipped' WHERE id = 100"
      ])

      await db.sleep(0.05)
      await db.executeTransaction([
        "UPDATE orders SET status = 'delivered' WHERE id = 100"
      ])

      // Verify current state
      const currentUser = await db.query('SELECT * FROM users WHERE id = 1')
      deepStrictEqual(currentUser.rows[0].name, 'John Smith')

      const currentOrder = await db.query('SELECT * FROM orders WHERE id = 100')
      deepStrictEqual(currentOrder.rows[0].status, 'delivered')

      // Verify history tracking
      const userHistory = await db.query(
        'SELECT * FROM users_history WHERE id = 1 ORDER BY sys_period'
      )
      deepStrictEqual(userHistory.rows.length, 1)
      deepStrictEqual(userHistory.rows[0].name, 'John Doe') // Original name

      const orderHistory = await db.query(
        'SELECT * FROM orders_history WHERE id = 100 ORDER BY sys_period'
      )
      ok(orderHistory.rows.length >= 3, 'Should have history of status changes')

      // Verify we can track order status progression
      const statusProgression = orderHistory.rows.map(row => row.status)
      ok(statusProgression.includes('pending'), 'Should include pending status')
      ok(
        statusProgression.includes('processing'),
        'Should include processing status'
      )
      ok(statusProgression.includes('shipped'), 'Should include shipped status')
    })

    test('should handle complex schema evolution scenario', async () => {
      // Start with basic product table
      await db.query(`
        CREATE TABLE products (
          id bigint PRIMARY KEY,
          name text,
          price decimal(10,2),
          sys_period tstzrange
        )
      `)

      await db.query(`
        CREATE TABLE products_history (
          id bigint,
          name text,
          price decimal(10,2),
          sys_period tstzrange
        )
      `)

      // Initial versioning setup
      let triggerResult = await db.query(`
        SELECT generate_static_versioning_trigger(
          'products',
          'products_history',
          'sys_period'
        ) as trigger_sql
      `)

      await db.query(triggerResult.rows[0].trigger_sql)

      // Insert initial product data
      await db.executeTransaction([
        "INSERT INTO products (id, name, price) VALUES (1, 'Widget A', 19.99)",
        "INSERT INTO products (id, name, price) VALUES (2, 'Gadget B', 29.99)"
      ])

      await db.sleep(0.1)

      // First schema evolution: add category
      await db.query(
        "ALTER TABLE products ADD COLUMN category text DEFAULT 'uncategorized'"
      )
      await db.query('ALTER TABLE products_history ADD COLUMN category text')

      // Regenerate trigger for new schema
      triggerResult = await db.query(`
        SELECT generate_static_versioning_trigger(
          'products',
          'products_history',
          'sys_period'
        ) as trigger_sql
      `)

      await db.query(triggerResult.rows[0].trigger_sql)

      // Update with new column
      await db.executeTransaction([
        "UPDATE products SET category = 'electronics', price = 24.99 WHERE id = 1"
      ])

      // Second schema evolution: add description and remove default
      await db.query('ALTER TABLE products ADD COLUMN description text')
      await db.query('ALTER TABLE products_history ADD COLUMN description text')
      await db.query('ALTER TABLE products ALTER COLUMN category DROP DEFAULT')

      // Regenerate trigger again
      triggerResult = await db.query(`
        SELECT generate_static_versioning_trigger(
          'products',
          'products_history',
          'sys_period'
        ) as trigger_sql
      `)

      await db.query(triggerResult.rows[0].trigger_sql)

      // Test with full schema
      await db.executeTransaction([
        "INSERT INTO products (id, name, price, category, description) VALUES (3, 'Super Widget', 39.99, 'premium', 'Advanced widget with extra features')"
      ])

      await db.sleep(0.1)

      await db.executeTransaction([
        "UPDATE products SET description = 'Ultimate widget with premium features', price = 44.99 WHERE id = 3"
      ])

      // Verify current state includes all columns
      const currentProducts = await db.query(
        'SELECT * FROM products ORDER BY id'
      )
      deepStrictEqual(currentProducts.rows.length, 3)
      ok(
        currentProducts.rows[2].description.includes('Ultimate'),
        'Should have updated description'
      )

      // Verify history preservation across schema changes
      const productHistory = await db.query(
        'SELECT * FROM products_history ORDER BY id, sys_period'
      )
      ok(productHistory.rows.length >= 2, 'Should have history records')

      // Verify we can query historical data even after schema changes
      const originalProduct1 = productHistory.rows.find(
        row => row.id === '1' && parseFloat(row.price) === 19.99
      )
      ok(originalProduct1, 'Should preserve original price in history')
    })
  })

  describe('Performance and Stress Testing', () => {
    test('should handle bulk operations efficiently', async () => {
      await db.query(`
        CREATE TABLE performance_test (
          id bigint PRIMARY KEY,
          data text,
          counter int DEFAULT 0,
          sys_period tstzrange
        )
      `)

      await db.query(`
        CREATE TABLE performance_test_history (
          id bigint,
          data text,
          counter int,
          sys_period tstzrange
        )
      `)

      const triggerResult = await db.query(`
        SELECT generate_static_versioning_trigger(
          'performance_test',
          'performance_test_history',
          'sys_period'
        ) as trigger_sql
      `)

      await db.query(triggerResult.rows[0].trigger_sql)

      const startTime = Date.now()

      // Bulk insert
      const insertPromises = []
      for (let i = 1; i <= 100; i++) {
        insertPromises.push(
          db.executeTransaction([
            `INSERT INTO performance_test (id, data) VALUES (${i}, 'data-${i}')`
          ])
        )
      }

      await Promise.all(insertPromises)

      const insertTime = Date.now() - startTime
      console.log(`Bulk insert time: ${insertTime}ms`)

      // Verify all records inserted
      const insertResult = await db.query(
        'SELECT COUNT(*) as count FROM performance_test'
      )
      deepStrictEqual(parseInt(insertResult.rows[0].count), 100)

      await db.sleep(0.1)

      // Bulk update
      const updateStartTime = Date.now()

      const updatePromises = []
      for (let i = 1; i <= 100; i++) {
        updatePromises.push(
          db.executeTransaction([
            `UPDATE performance_test SET counter = ${i}, data = 'updated-data-${i}' WHERE id = ${i}`
          ])
        )
      }

      await Promise.all(updatePromises)

      const updateTime = Date.now() - updateStartTime
      console.log(`Bulk update time: ${updateTime}ms`)

      // Verify history was created
      const historyResult = await db.query(
        'SELECT COUNT(*) as count FROM performance_test_history'
      )
      deepStrictEqual(parseInt(historyResult.rows[0].count), 100)

      // Performance assertion (should complete within reasonable time)
      ok(insertTime < 10000, 'Bulk insert should complete within 10 seconds')
      ok(updateTime < 15000, 'Bulk update should complete within 15 seconds')
    })

    test('should handle rapid sequential updates correctly', async () => {
      await db.query(`
        CREATE TABLE rapid_test (
          id bigint PRIMARY KEY,
          value int,
          sys_period tstzrange
        )
      `)

      await db.query(`
        CREATE TABLE rapid_test_history (
          id bigint,
          value int,
          sys_period tstzrange
        )
      `)

      const triggerResult = await db.query(`
        SELECT generate_static_versioning_trigger(
          'rapid_test',
          'rapid_test_history',
          'sys_period'
        ) as trigger_sql
      `)

      await db.query(triggerResult.rows[0].trigger_sql)

      // Insert initial record
      await db.executeTransaction([
        'INSERT INTO rapid_test (id, value) VALUES (1, 0)'
      ])

      // Perform rapid sequential updates
      for (let i = 1; i <= 50; i++) {
        await db.executeTransaction([
          `UPDATE rapid_test SET value = ${i} WHERE id = 1`
        ])
        await db.sleep(0.001) // Very small delay to ensure timestamp progression
      }

      // Verify final state
      const finalResult = await db.query(
        'SELECT * FROM rapid_test WHERE id = 1'
      )
      deepStrictEqual(parseInt(finalResult.rows[0].value), 50)

      // Verify all intermediate states were captured
      const historyResult = await db.query(
        'SELECT COUNT(*) as count FROM rapid_test_history WHERE id = 1'
      )
      deepStrictEqual(parseInt(historyResult.rows[0].count), 50) // All 50 updates should create history

      // Verify history contains sequential values
      const historyValues = await db.query(`
        SELECT value 
        FROM rapid_test_history 
        WHERE id = 1 
        ORDER BY sys_period
      `)

      for (let i = 0; i < 50; i++) {
        deepStrictEqual(parseInt(historyValues.rows[i].value), i)
      }
    })
  })

  describe('Migration and Data Integrity', () => {
    test('should handle migration mode with existing data', async () => {
      // Create table with existing historical data
      await db.query(`
        CREATE TABLE migration_test (
          id bigint PRIMARY KEY,
          status text,
          updated_at timestamp,
          sys_period tstzrange
        )
      `)

      await db.query(`
        CREATE TABLE migration_test_history (
          id bigint,
          status text,
          updated_at timestamp,
          sys_period tstzrange
        )
      `)

      // Insert historical data with specific time ranges
      const baseTime = '2023-01-01 10:00:00+00'
      const midTime = '2023-06-01 10:00:00+00'
      const currentTime = '2023-12-01 10:00:00+00'

      await db.query(
        `
        INSERT INTO migration_test (id, status, updated_at, sys_period)
        VALUES (1, 'active', '2023-01-01', tstzrange($1, NULL))
      `,
        [currentTime]
      )

      // Insert existing history
      await db.query(
        `
        INSERT INTO migration_test_history (id, status, updated_at, sys_period)
        VALUES 
          (1, 'pending', '2023-01-01', tstzrange($1, $2)),
          (1, 'processing', '2023-03-01', tstzrange($2, $3))
      `,
        [baseTime, midTime, currentTime]
      )

      // Set up versioning with migration mode
      const triggerResult = await db.query(`
        SELECT generate_static_versioning_trigger(
          'migration_test',
          'migration_test_history',
          'sys_period',
          false,  -- ignore_unchanged_values
          false,  -- include_current_version_in_history
          false,  -- mitigate_update_conflicts
          true    -- enable_migration_mode
        ) as trigger_sql
      `)

      await db.query(triggerResult.rows[0].trigger_sql)

      // Update should work correctly with existing history
      await db.executeTransaction([
        "UPDATE migration_test SET status = 'completed' WHERE id = 1"
      ])

      // Verify current state
      const currentResult = await db.query(
        'SELECT * FROM migration_test WHERE id = 1'
      )
      deepStrictEqual(currentResult.rows[0].status, 'completed')

      // Verify history preservation
      const historyResult = await db.query(`
        SELECT status, sys_period
        FROM migration_test_history 
        WHERE id = 1 
        ORDER BY sys_period
      `)

      ok(
        historyResult.rows.length >= 3,
        'Should preserve existing history and add new'
      )

      const statuses = historyResult.rows.map(row => row.status)
      ok(statuses.includes('pending'), 'Should preserve original history')
      ok(statuses.includes('processing'), 'Should preserve original history')
      ok(
        statuses.includes('active'),
        'Should add previous current state to history'
      )
    })

    test('should maintain referential integrity during versioning', async () => {
      // Create related tables with foreign keys
      await db.query(`
        CREATE TABLE users (
          id bigint PRIMARY KEY,
          name text,
          sys_period tstzrange
        )
      `)

      await db.query(`
        CREATE TABLE users_history (
          id bigint,
          name text,
          sys_period tstzrange
        )
      `)

      await db.query(`
        CREATE TABLE orders (
          id bigint PRIMARY KEY,
          user_id bigint REFERENCES users(id),
          amount decimal(10,2),
          sys_period tstzrange
        )
      `)

      await db.query(`
        CREATE TABLE orders_history (
          id bigint,
          user_id bigint,
          amount decimal(10,2),
          sys_period tstzrange
        )
      `)

      // Set up versioning for both tables
      const userTriggerResult = await db.query(`
        SELECT generate_static_versioning_trigger(
          'users',
          'users_history',
          'sys_period'
        ) as trigger_sql
      `)

      await db.query(userTriggerResult.rows[0].trigger_sql)

      const orderTriggerResult = await db.query(`
        SELECT generate_static_versioning_trigger(
          'orders',
          'orders_history',
          'sys_period'
        ) as trigger_sql
      `)

      await db.query(orderTriggerResult.rows[0].trigger_sql)

      // Create user and order
      await db.executeTransaction([
        "INSERT INTO users (id, name) VALUES (1, 'Test User')",
        'INSERT INTO orders (id, user_id, amount) VALUES (100, 1, 50.00)'
      ])

      await db.sleep(0.1)

      // Update both related records
      await db.executeTransaction([
        "UPDATE users SET name = 'Updated User' WHERE id = 1",
        'UPDATE orders SET amount = 75.00 WHERE id = 100'
      ])

      // Verify referential integrity is maintained
      const currentOrder = await db.query(`
        SELECT o.*, u.name as user_name
        FROM orders o
        JOIN users u ON o.user_id = u.id
        WHERE o.id = 100
      `)

      deepStrictEqual(currentOrder.rows.length, 1)
      deepStrictEqual(currentOrder.rows[0].user_name, 'Updated User')
      deepStrictEqual(parseFloat(currentOrder.rows[0].amount), 75.0)

      // Verify history maintains referential relationships
      const historyJoin = await db.query(`
        SELECT oh.amount as old_amount, uh.name as old_user_name
        FROM orders_history oh
        JOIN users_history uh ON oh.user_id = uh.id
        WHERE oh.id = 100
      `)

      ok(historyJoin.rows.length > 0, 'Should be able to join historical data')
    })
  })

  describe('Error Recovery and Edge Cases', () => {
    test('should handle transaction rollbacks correctly', async () => {
      await db.query(`
        CREATE TABLE rollback_test (
          id bigint PRIMARY KEY,
          value text,
          sys_period tstzrange
        )
      `)

      await db.query(`
        CREATE TABLE rollback_test_history (
          id bigint,
          value text,
          sys_period tstzrange
        )
      `)

      const triggerResult = await db.query(`
        SELECT generate_static_versioning_trigger(
          'rollback_test',
          'rollback_test_history',
          'sys_period'
        ) as trigger_sql
      `)

      await db.query(triggerResult.rows[0].trigger_sql)

      // Insert initial data
      await db.executeTransaction([
        "INSERT INTO rollback_test (id, value) VALUES (1, 'original')"
      ])

      // Attempt transaction that will fail
      await db.query('BEGIN')

      try {
        await db.query(
          "UPDATE rollback_test SET value = 'updated' WHERE id = 1"
        )
        // Force an error
        await db.query(
          "INSERT INTO rollback_test (id, value) VALUES (1, 'duplicate')"
        ) // Will fail due to PK constraint
        await db.query('COMMIT')
      } catch (error) {
        await db.query('ROLLBACK')
      }

      // Verify original state is preserved
      const result = await db.query('SELECT * FROM rollback_test WHERE id = 1')
      deepStrictEqual(result.rows[0].value, 'original')

      // Verify no spurious history was created
      const historyResult = await db.query(
        'SELECT * FROM rollback_test_history'
      )
      deepStrictEqual(historyResult.rows.length, 0)
    })

    test('should handle concurrent modifications gracefully', async () => {
      await db.query(`
        CREATE TABLE concurrent_test (
          id bigint PRIMARY KEY,
          counter int DEFAULT 0,
          sys_period tstzrange
        )
      `)

      await db.query(`
        CREATE TABLE concurrent_test_history (
          id bigint,
          counter int,
          sys_period tstzrange
        )
      `)

      const triggerResult = await db.query(`
        SELECT generate_static_versioning_trigger(
          'concurrent_test',
          'concurrent_test_history',
          'sys_period'
        ) as trigger_sql
      `)

      await db.query(triggerResult.rows[0].trigger_sql)

      // Insert initial record
      await db.executeTransaction([
        'INSERT INTO concurrent_test (id, counter) VALUES (1, 0)'
      ])

      // Simulate concurrent updates (sequential for testing)
      const updates = []
      for (let i = 1; i <= 10; i++) {
        updates.push(
          db.executeTransaction([
            `UPDATE concurrent_test SET counter = counter + 1 WHERE id = 1`
          ])
        )
      }

      await Promise.all(updates)

      // Verify final state (some updates might have been lost due to concurrency, but that's expected)
      const finalResult = await db.query(
        'SELECT * FROM concurrent_test WHERE id = 1'
      )
      const finalCounter = parseInt(finalResult.rows[0].counter)
      ok(
        finalCounter > 0 && finalCounter <= 10,
        'Counter should be between 1 and 10'
      )

      // Verify history was created for successful updates
      const historyResult = await db.query(
        'SELECT COUNT(*) as count FROM concurrent_test_history WHERE id = 1'
      )
      const historyCount = parseInt(historyResult.rows[0].count)
      ok(historyCount > 0, 'Should have some history records')
    })
  })
})
