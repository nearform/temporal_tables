import { Client, ClientConfig, QueryResult } from 'pg'
import { readFileSync } from 'fs'
import { join } from 'path'
import * as url from 'url'

const __dirname = url.fileURLToPath(new URL('.', import.meta.url))

export interface DatabaseRow {
  [key: string]: any
}

export interface TestResult {
  rows: DatabaseRow[]
  command: string
  rowCount: number
}

export class DatabaseHelper {
  static modernMinimumPostgresVersion = '13.21' as const

  private client: Client
  private isConnected = false

  constructor(config?: ClientConfig) {
    const defaultConfig: ClientConfig = {
      host: process.env.PGHOST || 'localhost',
      port: parseInt(process.env.PGPORT || '5432'),
      user: process.env.PGUSER || 'postgres',
      password: process.env.PGPASSWORD || 'password',
      database: process.env.PGDATABASE || 'postgres'
    }

    this.client = new Client({ ...defaultConfig, ...config })
  }

  async cleanup(): Promise<void> {
    // Drop all test tables and functions
    const tables = await this.query(`
      SELECT tablename 
      FROM pg_tables 
      WHERE schemaname = 'public' 
      AND (tablename LIKE '%versioning%' OR tablename LIKE '%test%')
    `)

    for (const table of tables.rows)
      await this.query(`DROP TABLE IF EXISTS ${table.tablename} CASCADE`)

    // Clean up any test functions
    const functions = await this.query(`
      SELECT proname 
      FROM pg_proc 
      WHERE proname LIKE '%test%' OR proname LIKE '%versioning%'
    `)

    for (const func of functions.rows)
      await this.query(`DROP FUNCTION IF EXISTS ${func.proname}() CASCADE`)
  }

  async connect(): Promise<void> {
    if (!this.isConnected) {
      await this.client.connect()
      this.isConnected = true
    }
  }

  async disconnect(): Promise<void> {
    if (this.isConnected) {
      await this.client.end()
      this.isConnected = false
    }
  }

  async ensureTimestampGap(): Promise<void> {
    // Ensure a small gap between timestamps to avoid timing races
    await this.sleep(0.001) // 1 millisecond gap
  }

  async executeTransaction(sqlStatements: string[]): Promise<TestResult[]> {
    const results: TestResult[] = []

    await this.query('BEGIN')

    try {
      for (const sql of sqlStatements)
        if (sql.trim()) {
          const result = await this.query(sql)
          results.push(result)
        }
      await this.query('COMMIT')
    } catch (error) {
      await this.query('ROLLBACK')
      throw error
    }

    return results
  }

  async getCurrentTimestamp(): Promise<Date> {
    const result = await this.query('SELECT CURRENT_TIMESTAMP as now')
    return result.rows[0].now
  }

  async getReliableTimestamp(): Promise<Date> {
    // Get timestamp and ensure it's unique by adding a small delay
    const timestamp = await this.getCurrentTimestamp()
    await this.ensureTimestampGap()
    return timestamp
  }

  async getTableStructure(tableName: string): Promise<DatabaseRow[]> {
    const result = await this.query(
      `
      SELECT column_name, data_type, is_nullable, column_default
      FROM information_schema.columns
      WHERE table_schema = 'public' AND table_name = $1
      ORDER BY ordinal_position
    `,
      [tableName]
    )

    return result.rows
  }

  async isTimestampInRange(
    timestamp: Date,
    beforeTime: Date,
    afterTime: Date,
    toleranceMs: number = 1000 // 1 second tolerance by default
  ): Promise<boolean> {
    const timestampMs = timestamp.getTime()
    const beforeMs = beforeTime.getTime() - toleranceMs
    const afterMs = afterTime.getTime() + toleranceMs

    return timestampMs >= beforeMs && timestampMs <= afterMs
  }

  async loadAndExecuteSqlFile(filePath: string): Promise<void> {
    await this.query(readFileSync(filePath, 'utf-8'))
  }

  async query(sql: string, params?: any[]): Promise<TestResult> {
    const result: QueryResult = await this.client.query(sql, params)
    return {
      rows: result.rows,
      command: result.command,
      rowCount: result.rowCount ?? 0
    }
  }

  async setupVersioning(minimumServerVersion: string = '0.0'): Promise<void> {
    const rootPath = join(__dirname, '..', '..')

    // Always load legacy functionality (works on any Postgres version)
    const legacySqlFiles = [
      'versioning_function.sql',
      'system_time_function.sql'
    ]

    // Modern functionality requires Postgres 13+
    const modernSqlFiles = [
      'generate_static_versioning_trigger.sql',
      'versioning_tables_metadata.sql',
      'render_versioning_trigger.sql',
      'event_trigger_versioning.sql'
    ]

    // Verify PostgreSQL version first
    if (!(await this.verifyPostgresVersion(minimumServerVersion)))
      process.exit(0)

    // Always load legacy files
    for (const filename of legacySqlFiles)
      await this.loadAndExecuteSqlFile(join(rootPath, filename))

    // Only load modern files if we meet the minimum version requirement
    // (minimum version 13.0 or higher for new functionality)
    const [majorVersion] = minimumServerVersion.split('.').map(Number)
    if (majorVersion >= 13)
      for (const filename of modernSqlFiles)
        await this.loadAndExecuteSqlFile(join(rootPath, filename))
  }

  async sleep(seconds: number): Promise<void> {
    await this.query(`SELECT pg_sleep($1)`, [seconds])
  }

  async tableExists(tableName: string): Promise<boolean> {
    const result = await this.query(
      `
      SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = $1
      )
    `,
      [tableName]
    )

    return result.rows[0].exists
  }

  async verifyPostgresVersion(minVersion: string): Promise<boolean> {
    const result = await this.query('SHOW server_version')
    const version = result.rows[0].server_version
    const [major, minor] = version.split('.').map(Number) // Split version into major and minor parts
    const [minMajor, minMinor] = minVersion.split('.').map(Number) // Split minVersion into major and minor parts
    return !(major < minMajor || (major === minMajor && minor < minMinor))
  }
}
