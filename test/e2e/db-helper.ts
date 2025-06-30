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

  async query(sql: string, params?: any[]): Promise<TestResult> {
    const result: QueryResult = await this.client.query(sql, params)
    return {
      rows: result.rows,
      command: result.command,
      rowCount: result.rowCount || 0
    }
  }

  async executeTransaction(sqlStatements: string[]): Promise<TestResult[]> {
    const results: TestResult[] = []

    await this.query('BEGIN')

    try {
      for (const sql of sqlStatements) {
        if (sql.trim()) {
          const result = await this.query(sql)
          results.push(result)
        }
      }
      await this.query('COMMIT')
    } catch (error) {
      await this.query('ROLLBACK')
      throw error
    }

    return results
  }

  async loadAndExecuteSqlFile(filePath: string): Promise<void> {
    await this.query(readFileSync(filePath, 'utf-8'))
  }

  async setupVersioning(): Promise<void> {
    const rootPath = join(__dirname, '..', '..')

    const sqlFiles = [
      'versioning_function.sql',
      'system_time_function.sql',
      'generate_static_versioning_trigger.sql',
      'versioning_tables_metadata.sql',
      'render_versioning_trigger.sql',
      'event_trigger_versioning.sql'
    ]

    for (const filename of sqlFiles)
      await this.loadAndExecuteSqlFile(join(rootPath, filename))
  }

  async cleanup(): Promise<void> {
    // Drop all test tables and functions
    const tables = await this.query(`
      SELECT tablename 
      FROM pg_tables 
      WHERE schemaname = 'public' 
      AND (tablename LIKE '%versioning%' OR tablename LIKE '%test%')
    `)

    for (const table of tables.rows) {
      await this.query(`DROP TABLE IF EXISTS ${table.tablename} CASCADE`)
    }

    // Clean up any test functions
    const functions = await this.query(`
      SELECT proname 
      FROM pg_proc 
      WHERE proname LIKE '%test%' OR proname LIKE '%versioning%'
    `)

    for (const func of functions.rows) {
      await this.query(`DROP FUNCTION IF EXISTS ${func.proname}() CASCADE`)
    }
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

  async getCurrentTimestamp(): Promise<Date> {
    const result = await this.query('SELECT CURRENT_TIMESTAMP as now')
    return result.rows[0].now
  }

  async sleep(seconds: number): Promise<void> {
    await this.query(`SELECT pg_sleep($1)`, [seconds])
  }
}
