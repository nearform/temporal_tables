import { deepStrictEqual, ok } from 'node:assert'
import { describe, test, before, after, beforeEach } from 'node:test'
import { DatabaseHelper } from './db-helper.js'

interface PerformanceMetrics {
  setupTime: number
  insertTime: number
  updateTime: number
  deleteTime: number
  totalTime: number
  operationCount: number
}

interface TestResults {
  mainTableRows: any[]
  historyTableRows: any[]
  totalMainCount: number
  totalHistoryCount: number
}

describe('Legacy vs Modern Implementation Performance Comparison', () => {
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
    await db.query('DROP TABLE IF EXISTS legacy_perf_test CASCADE')
    await db.query('DROP TABLE IF EXISTS legacy_perf_test_history CASCADE')
    await db.query('DROP TABLE IF EXISTS modern_perf_test CASCADE')
    await db.query('DROP TABLE IF EXISTS modern_perf_test_history CASCADE')
  })

  async function setupLegacyTable(): Promise<void> {
    // Create main table
    await db.query(`
      CREATE TABLE legacy_perf_test (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100),
        value INTEGER,
        description TEXT,
        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        sys_period tstzrange NOT NULL DEFAULT tstzrange(CURRENT_TIMESTAMP, NULL)
      )
    `)

    // Create history table
    await db.query(`
      CREATE TABLE legacy_perf_test_history (
        id INTEGER,
        name VARCHAR(100),
        value INTEGER,
        description TEXT,
        created_at TIMESTAMPTZ,
        sys_period tstzrange NOT NULL
      )
    `)

    // Apply legacy versioning function
    await db.query(`
      CREATE TRIGGER versioning_trigger
        BEFORE INSERT OR UPDATE OR DELETE ON legacy_perf_test
        FOR EACH ROW EXECUTE FUNCTION versioning(
          'sys_period', 'legacy_perf_test_history', true
        )
    `)
  }

  async function setupModernTable(): Promise<void> {
    // Create main table
    await db.query(`
      CREATE TABLE modern_perf_test (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100),
        value INTEGER,
        description TEXT,
        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        sys_period tstzrange NOT NULL DEFAULT tstzrange(CURRENT_TIMESTAMP, NULL)
      )
    `)

    // Create history table
    await db.query(`
      CREATE TABLE modern_perf_test_history (
        id INTEGER,
        name VARCHAR(100),
        value INTEGER,
        description TEXT,
        created_at TIMESTAMPTZ,
        sys_period tstzrange NOT NULL
      )
    `)

    // Use render_versioning_trigger procedure with named arguments
    await db.query(`
      CALL render_versioning_trigger(
        p_table_name => 'modern_perf_test',
        p_history_table => 'modern_perf_test_history',
        p_sys_period => 'sys_period',
        p_ignore_unchanged_values => true,
        p_include_current_version_in_history => false,
        p_mitigate_update_conflicts => false,
        p_enable_migration_mode => false
      )
    `)
  }

  async function performOperations(
    tableName: string,
    historyTableName: string,
    testDataSize: number
  ): Promise<PerformanceMetrics> {
    const metrics: PerformanceMetrics = {
      setupTime: 0,
      insertTime: 0,
      updateTime: 0,
      deleteTime: 0,
      totalTime: 0,
      operationCount: testDataSize
    }

    const startTotal = Date.now()

    // INSERT operations
    const insertStart = Date.now()
    for (let i = 1; i <= testDataSize; i++) {
      await db.query(
        `
        INSERT INTO ${tableName} (name, value, description) 
        VALUES ($1, $2, $3)
      `,
        [`Test Item ${i}`, i * 10, `Description for item ${i}`]
      )
    }
    metrics.insertTime = Date.now() - insertStart

    // UPDATE operations (update half of the records)
    const updateStart = Date.now()
    for (let i = 1; i <= Math.floor(testDataSize / 2); i++) {
      await db.query(
        `
        UPDATE ${tableName} 
        SET value = $1, description = $2 
        WHERE id = $3
      `,
        [i * 20, `Updated description for item ${i}`, i]
      )
    }
    metrics.updateTime = Date.now() - updateStart

    // DELETE operations (delete quarter of the records)
    const deleteStart = Date.now()
    for (let i = 1; i <= Math.floor(testDataSize / 4); i++) {
      await db.query(`DELETE FROM ${tableName} WHERE id = $1`, [i])
    }
    metrics.deleteTime = Date.now() - deleteStart

    metrics.totalTime = Date.now() - startTotal

    return metrics
  }

  async function getTableResults(
    tableName: string,
    historyTableName: string
  ): Promise<TestResults> {
    const mainResult = await db.query(`
      SELECT id, name, value, description, sys_period
      FROM ${tableName}
      ORDER BY id
    `)

    const historyResult = await db.query(`
      SELECT id, name, value, description, sys_period
      FROM ${historyTableName}
      ORDER BY id, sys_period
    `)

    const mainCountResult = await db.query(
      `SELECT COUNT(*) as count FROM ${tableName}`
    )
    const historyCountResult = await db.query(
      `SELECT COUNT(*) as count FROM ${historyTableName}`
    )

    return {
      mainTableRows: mainResult.rows,
      historyTableRows: historyResult.rows,
      totalMainCount: parseInt(mainCountResult.rows[0].count),
      totalHistoryCount: parseInt(historyCountResult.rows[0].count)
    }
  }

  function formatPerformanceReport(
    legacyMetrics: PerformanceMetrics,
    modernMetrics: PerformanceMetrics
  ): string {
    // Collect all data for dynamic width calculation
    const operations = ['INSERT', 'UPDATE', 'DELETE', 'TOTAL']
    const times = [
      [legacyMetrics.insertTime, modernMetrics.insertTime],
      [legacyMetrics.updateTime, modernMetrics.updateTime],
      [legacyMetrics.deleteTime, modernMetrics.deleteTime],
      [legacyMetrics.totalTime, modernMetrics.totalTime]
    ]

    // Calculate dynamic column widths (ensure column headers fit)
    const operationWidth = Math.max('Operation'.length, ...operations.map(op => op.length))
    const legacyWidth = Math.max('Legacy'.length, ...times.map(([legacy]) => `${legacy}ms`.length))
    const modernWidth = Math.max('Modern'.length, ...times.map(([, modern]) => `${modern}ms`.length))
    const diffWidth = Math.max('Diff'.length, ...times.map(([legacy, modern]) => `${Math.abs(legacy - modern)}ms`.length))
    const improvWidth = Math.max('Improv'.length, 7) // Fixed width for percentage
    const statusWidth = Math.max('✓'.length, 1) // Fixed width for checkmark/X

    // Calculate total width precisely: sum of all column widths + padding (2 per column) + separators (1 per separator)
    const totalWidth = (operationWidth + 2) + (legacyWidth + 2) + (modernWidth + 2) + (diffWidth + 2) + (improvWidth + 2) + (statusWidth + 2) + 5 // 5 separators (|)

    const createRow = (
      operation: string,
      legacyTime: number,
      modernTime: number,
      isTotal: boolean = false
    ) => {
      const diff = legacyTime - modernTime
      const percentage = legacyTime > 0 ? ((diff / legacyTime) * 100).toFixed(1) : '0.0'
      const symbol = diff > 0 ? '✓' : diff < 0 ? '✗' : '≈'
      const diffDisplay = diff > 0 ? `+${diff}` : diff.toString()

      return `│ ${operation.padEnd(operationWidth)} │ ${`${legacyTime}ms`.padStart(legacyWidth)} │ ${`${modernTime}ms`.padStart(modernWidth)} │ ${`${diffDisplay}ms`.padStart(diffWidth)} │ ${`${percentage}%`.padStart(improvWidth)} │ ${symbol.padStart(statusWidth)} │`
    }

    const titleText = 'PERFORMANCE COMPARISON REPORT'
    const titlePadding = Math.max(0, Math.floor((totalWidth - titleText.length) / 2))
    const title = ' '.repeat(titlePadding) + titleText + ' '.repeat(totalWidth - titleText.length - titlePadding)

    return `
┌${'─'.repeat(totalWidth)}┐
│${title}│
├${'─'.repeat(operationWidth + 2)}┬${'─'.repeat(legacyWidth + 2)}┬${'─'.repeat(modernWidth + 2)}┬${'─'.repeat(diffWidth + 2)}┬${'─'.repeat(improvWidth + 2)}┬${'─'.repeat(statusWidth + 2)}┤
│ ${'Operation'.padEnd(operationWidth)} │ ${'Legacy'.padStart(legacyWidth)} │ ${'Modern'.padStart(modernWidth)} │ ${'Diff'.padStart(diffWidth)} │ ${'Improv'.padStart(improvWidth)} │ ${'✓'.padStart(statusWidth)} │
├${'─'.repeat(operationWidth + 2)}┼${'─'.repeat(legacyWidth + 2)}┼${'─'.repeat(modernWidth + 2)}┼${'─'.repeat(diffWidth + 2)}┼${'─'.repeat(improvWidth + 2)}┼${'─'.repeat(statusWidth + 2)}┤
${createRow('INSERT', legacyMetrics.insertTime, modernMetrics.insertTime)}
${createRow('UPDATE', legacyMetrics.updateTime, modernMetrics.updateTime)}
${createRow('DELETE', legacyMetrics.deleteTime, modernMetrics.deleteTime)}
├${'─'.repeat(operationWidth + 2)}┼${'─'.repeat(legacyWidth + 2)}┼${'─'.repeat(modernWidth + 2)}┼${'─'.repeat(diffWidth + 2)}┼${'─'.repeat(improvWidth + 2)}┼${'─'.repeat(statusWidth + 2)}┤
${createRow('TOTAL', legacyMetrics.totalTime, modernMetrics.totalTime, true)}
└${'─'.repeat(operationWidth + 2)}┴${'─'.repeat(legacyWidth + 2)}┴${'─'.repeat(modernWidth + 2)}┴${'─'.repeat(diffWidth + 2)}┴${'─'.repeat(improvWidth + 2)}┴${'─'.repeat(statusWidth + 2)}┘

📊 Test Data:
   • Operations performed: ${legacyMetrics.operationCount.toLocaleString()}
   • Insert operations: ${legacyMetrics.operationCount}
   • Update operations: ${Math.floor(legacyMetrics.operationCount / 2)}
   • Delete operations: ${Math.floor(legacyMetrics.operationCount / 4)}

🎯 Performance Summary:
   • Modern implementation is ${(((legacyMetrics.totalTime - modernMetrics.totalTime) / legacyMetrics.totalTime) * 100).toFixed(1)}% ${modernMetrics.totalTime < legacyMetrics.totalTime ? 'faster' : 'slower'} overall
   • Legacy total time: ${legacyMetrics.totalTime.toLocaleString()}ms
   • Modern total time: ${modernMetrics.totalTime.toLocaleString()}ms
   • Time difference: ${Math.abs(legacyMetrics.totalTime - modernMetrics.totalTime).toLocaleString()}ms
`
  }

  function validateResultsMatch(
    legacyResults: TestResults,
    modernResults: TestResults
  ): void {
    // Both should have the same number of records in main tables
    deepStrictEqual(
      legacyResults.totalMainCount,
      modernResults.totalMainCount,
      `Main table counts differ: Legacy=${legacyResults.totalMainCount}, Modern=${modernResults.totalMainCount}`
    )

    // Both should have the same number of records in history tables
    deepStrictEqual(
      legacyResults.totalHistoryCount,
      modernResults.totalHistoryCount,
      `History table counts differ: Legacy=${legacyResults.totalHistoryCount}, Modern=${modernResults.totalHistoryCount}`
    )

    // Compare the data in main tables (excluding sys_period which may have timing differences)
    for (let i = 0; i < legacyResults.mainTableRows.length; i++) {
      const legacyRow = legacyResults.mainTableRows[i]
      const modernRow = modernResults.mainTableRows[i]

      deepStrictEqual(
        legacyRow.id,
        modernRow.id,
        `Main table row ${i} ID mismatch`
      )
      deepStrictEqual(
        legacyRow.name,
        modernRow.name,
        `Main table row ${i} name mismatch`
      )
      deepStrictEqual(
        legacyRow.value,
        modernRow.value,
        `Main table row ${i} value mismatch`
      )
      deepStrictEqual(
        legacyRow.description,
        modernRow.description,
        `Main table row ${i} description mismatch`
      )
    }

    console.log(
      '✅ Data consistency validation passed: Both implementations produce identical results!'
    )
  }

  // Create individual tests for each data size
  const testDataSizes = [100, 500, 1000, 5000]
  
  test('should compare performance with multiple data sizes and validate result consistency', async () => {
    console.log('\n🚀 Starting Performance Comparison Tests...\n')

    for (const testDataSize of testDataSizes) {
      console.log(
        `\n🚀 Testing with ${testDataSize.toLocaleString()} operations...\n`
      )

      // Setup and test legacy implementation
      console.log('📊 Testing Legacy Implementation...')
      await setupLegacyTable()
      const legacyMetrics = await performOperations(
        'legacy_perf_test',
        'legacy_perf_test_history',
        testDataSize
      )
      const legacyResults = await getTableResults(
        'legacy_perf_test',
        'legacy_perf_test_history'
      )

      // Clean up before setting up modern table
      await db.query('DROP TABLE IF EXISTS legacy_perf_test CASCADE')
      await db.query('DROP TABLE IF EXISTS legacy_perf_test_history CASCADE')

      // Setup and test modern implementation
      console.log('📊 Testing Modern Implementation...')
      await setupModernTable()
      const modernMetrics = await performOperations(
        'modern_perf_test',
        'modern_perf_test_history',
        testDataSize
      )
      const modernResults = await getTableResults(
        'modern_perf_test',
        'modern_perf_test_history'
      )

      // Validate that both implementations produce the same results
      validateResultsMatch(legacyResults, modernResults)

      // Display performance comparison report
      console.log(formatPerformanceReport(legacyMetrics, modernMetrics))

      // Assert that modern implementation performs reasonably (not more than 50% slower)
      const performanceRatio = modernMetrics.totalTime / legacyMetrics.totalTime
      ok(
        performanceRatio <= 1.5,
        `Modern implementation is significantly slower (${(performanceRatio * 100).toFixed(1)}% of legacy time). ` +
          `Expected ratio <= 150%, got ${(performanceRatio * 100).toFixed(1)}%`
      )

      // Verify that we have the expected number of operations
      ok(
        legacyResults.totalMainCount > 0,
        'Legacy implementation should have main table records'
      )
      ok(
        legacyResults.totalHistoryCount > 0,
        'Legacy implementation should have history table records'
      )
      ok(
        modernResults.totalMainCount > 0,
        'Modern implementation should have main table records'
      )
      ok(
        modernResults.totalHistoryCount > 0,
        'Modern implementation should have history table records'
      )

      console.log('\n✅ Performance comparison test completed successfully!')

      // Clean up after test
      await db.query('DROP TABLE IF EXISTS modern_perf_test CASCADE')
      await db.query('DROP TABLE IF EXISTS modern_perf_test_history CASCADE')
    }
  })

  test('should provide performance scaling summary across all data sizes', async () => {
    console.log('\n📈 Running Performance Scaling Analysis...\n')

    interface ScalingResult {
      dataSize: number
      legacyTime: number
      modernTime: number
      ratio: number
    }

    const scalingResults: ScalingResult[] = []
    const testDataSizes = [100, 500, 1000, 5000]

    for (const testDataSize of testDataSizes) {
      console.log(
        `\n⚡ Testing with ${testDataSize.toLocaleString()} operations...`
      )

      // Test legacy implementation
      await setupLegacyTable()
      const legacyMetrics = await performOperations(
        'legacy_perf_test',
        'legacy_perf_test_history',
        testDataSize
      )
      await db.query('DROP TABLE IF EXISTS legacy_perf_test CASCADE')
      await db.query('DROP TABLE IF EXISTS legacy_perf_test_history CASCADE')

      // Test modern implementation
      await setupModernTable()
      const modernMetrics = await performOperations(
        'modern_perf_test',
        'modern_perf_test_history',
        testDataSize
      )
      await db.query('DROP TABLE IF EXISTS modern_perf_test CASCADE')
      await db.query('DROP TABLE IF EXISTS modern_perf_test_history CASCADE')

      const ratio = modernMetrics.totalTime / legacyMetrics.totalTime
      scalingResults.push({
        dataSize: testDataSize,
        legacyTime: legacyMetrics.totalTime,
        modernTime: modernMetrics.totalTime,
        ratio
      })

      console.log(
        `   Legacy: ${legacyMetrics.totalTime}ms | Modern: ${modernMetrics.totalTime}ms | Ratio: ${ratio.toFixed(2)}x`
      )
    }

    // Display scaling summary
    console.log('\n' + formatScalingReport(scalingResults))

    // Assert that performance doesn't degrade significantly at higher scales
    const maxRatio = Math.max(...scalingResults.map(r => r.ratio))
    ok(
      maxRatio <= 2.0,
      `Performance ratio should not exceed 2.0x at any scale, but got ${maxRatio.toFixed(2)}x`
    )

    console.log('\n✅ Performance scaling analysis completed!')
  })

  function formatScalingReport(
    results: {
      dataSize: number
      legacyTime: number
      modernTime: number
      ratio: number
    }[]
  ): string {
    // Calculate dynamic column widths (ensure column headers fit)
    const dataSizeWidth = Math.max('Data Size'.length, ...results.map(r => r.dataSize.toLocaleString().length))
    const legacyWidth = Math.max('Legacy'.length, ...results.map(r => `${r.legacyTime}ms`.length))
    const modernWidth = Math.max('Modern'.length, ...results.map(r => `${r.modernTime}ms`.length))
    const ratioWidth = Math.max('Ratio'.length, 8)
    const throughputWidth = Math.max('Throughput'.length, 12)

    // Calculate total width precisely: sum of all column widths + padding (2 per column) + separators (1 per separator)
    const totalWidth = (dataSizeWidth + 2) + (legacyWidth + 2) + (modernWidth + 2) + (ratioWidth + 2) + (throughputWidth + 2) + 4 // 4 separators (|)

    const titleText = 'PERFORMANCE SCALING REPORT'
    const titlePadding = Math.max(0, Math.floor((totalWidth - titleText.length) / 2))
    const title = ' '.repeat(titlePadding) + titleText + ' '.repeat(totalWidth - titleText.length - titlePadding)

    let report = `
┌${'─'.repeat(totalWidth)}┐
│${title}│
├${'─'.repeat(dataSizeWidth + 2)}┬${'─'.repeat(legacyWidth + 2)}┬${'─'.repeat(modernWidth + 2)}┬${'─'.repeat(ratioWidth + 2)}┬${'─'.repeat(throughputWidth + 2)}┤
│ ${'Data Size'.padEnd(dataSizeWidth)} │ ${'Legacy'.padStart(legacyWidth)} │ ${'Modern'.padStart(modernWidth)} │ ${'Ratio'.padStart(ratioWidth)} │ ${'Throughput'.padStart(throughputWidth)} │
├${'─'.repeat(dataSizeWidth + 2)}┼${'─'.repeat(legacyWidth + 2)}┼${'─'.repeat(modernWidth + 2)}┼${'─'.repeat(ratioWidth + 2)}┼${'─'.repeat(throughputWidth + 2)}┤`

    for (const result of results) {
      const throughput = Math.round(
        result.dataSize / (result.modernTime / 1000)
      ) // ops per second
      const ratioDisplay = `${result.ratio.toFixed(2)}x`
      const throughputDisplay = `${throughput.toLocaleString()}/s`

      report += `
│ ${result.dataSize.toLocaleString().padEnd(dataSizeWidth)} │ ${`${result.legacyTime}ms`.padStart(legacyWidth)} │ ${`${result.modernTime}ms`.padStart(modernWidth)} │ ${ratioDisplay.padStart(ratioWidth)} │ ${throughputDisplay.padStart(throughputWidth)} │`
    }

    report += `
└${'─'.repeat(dataSizeWidth + 2)}┴${'─'.repeat(legacyWidth + 2)}┴${'─'.repeat(modernWidth + 2)}┴${'─'.repeat(ratioWidth + 2)}┴${'─'.repeat(throughputWidth + 2)}┘

📊 Scaling Analysis:
   • Best performance ratio: ${Math.min(...results.map(r => r.ratio)).toFixed(2)}x (at ${results.find(r => r.ratio === Math.min(...results.map(r => r.ratio)))?.dataSize.toLocaleString()} operations)
   • Worst performance ratio: ${Math.max(...results.map(r => r.ratio)).toFixed(2)}x (at ${results.find(r => r.ratio === Math.max(...results.map(r => r.ratio)))?.dataSize.toLocaleString()} operations)
   • Average performance ratio: ${(results.reduce((sum, r) => sum + r.ratio, 0) / results.length).toFixed(2)}x
   • Modern implementation efficiency improves with scale: ${results[results.length - 1].ratio < results[0].ratio ? '✓ Yes' : '✗ No'}`

    return report
  }

  test('should demonstrate advanced modern features work correctly', async () => {
    console.log('\n🔬 Testing Advanced Modern Features...')

    // Test with ignore_unchanged_values enabled
    await db.query(`
      CREATE TABLE modern_advanced_test (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100),
        value INTEGER,
        sys_period tstzrange NOT NULL DEFAULT tstzrange(CURRENT_TIMESTAMP, NULL)
      )
    `)

    await db.query(`
      CREATE TABLE modern_advanced_test_history (
        id INTEGER,
        name VARCHAR(100),
        value INTEGER,
        sys_period tstzrange NOT NULL
      )
    `)

    // Use render_versioning_trigger with ignore_unchanged_values enabled
    await db.query(`
      CALL render_versioning_trigger(
        p_table_name => 'modern_advanced_test',
        p_history_table => 'modern_advanced_test_history',
        p_sys_period => 'sys_period',
        p_ignore_unchanged_values => true,
        p_include_current_version_in_history => false,
        p_mitigate_update_conflicts => false,
        p_enable_migration_mode => false
      )
    `)

    // Insert a record
    await db.query(`
      INSERT INTO modern_advanced_test (name, value) 
      VALUES ('test', 100)
    `)

    // Update with the same values (should be ignored)
    await db.query(`
      UPDATE modern_advanced_test 
      SET name = 'test', value = 100 
      WHERE id = 1
    `)

    // Update with different values (should create history)
    await db.query(`
      UPDATE modern_advanced_test 
      SET name = 'updated', value = 200 
      WHERE id = 1
    `)

    // Check history count - should only have one entry (from the real update)
    const historyCount = await db.query(`
      SELECT COUNT(*) as count FROM modern_advanced_test_history
    `)

    deepStrictEqual(
      parseInt(historyCount.rows[0].count),
      1,
      'Should have only one history entry due to ignore_unchanged_values'
    )

    console.log('✅ Advanced features test passed!')
  })
})
