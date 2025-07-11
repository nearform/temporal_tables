#!/usr/bin/env node

import { spawn, ChildProcess } from 'child_process'
import { fileURLToPath } from 'url'
import { dirname, join } from 'path'
import { existsSync } from 'fs'

const __filename: string = fileURLToPath(import.meta.url)
const __dirname: string = dirname(__filename)

interface TestEnvironment {
  PGHOST: string
  PGPORT: string
  PGUSER: string
  PGPASSWORD: string
  PGDATABASE: string
  [key: string]: string | undefined
}

interface TestResult {
  testFile: string
  success: boolean
  stdout: string
  stderr: string
  code?: number
}

interface TestError {
  testFile: string
  error: Error
}

const testFiles: string[] = [
  'test-static-generator.ts',
  'test-legacy.ts',
  'test-event-trigger.ts',
  'test-integration.ts',
  'test-increment-version.ts'
]

const env: TestEnvironment = {
  ...process.env,
  PGHOST: process.env.PGHOST || 'localhost',
  PGPORT: process.env.PGPORT || '5432',
  PGUSER: process.env.PGUSER || 'postgres',
  PGPASSWORD: process.env.PGPASSWORD || 'password',
  PGDATABASE: process.env.PGDATABASE || 'postgres'
} as TestEnvironment

console.log('🚀 Running Temporal Tables E2E Tests')
console.log('=====================================')
console.log(
  `Database: ${env.PGUSER}@${env.PGHOST}:${env.PGPORT}/${env.PGDATABASE}`
)
console.log('')

async function runTest(testFile: string): Promise<TestResult> {
  return new Promise<TestResult>((resolve, reject) => {
    console.log(`📋 Running ${testFile}...`)

    const testPath: string = join(__dirname, testFile)

    // Use the same execution pattern as the working npm scripts
    const nodeArgs: string[] = [
      '--env-file-if-exists',
      './.env',
      '--loader',
      'ts-node/esm/transpile-only',
      '--test',
      testPath
    ]

    const child: ChildProcess = spawn('node', nodeArgs, {
      env,
      stdio: 'pipe',
      shell: process.platform === 'win32',
      cwd: join(__dirname, '../..') // Run from project root
    })

    let stdout: string = ''
    let stderr: string = ''

    child.stdout?.on('data', (data: Buffer) => {
      stdout += data.toString()
    })

    child.stderr?.on('data', (data: Buffer) => {
      stderr += data.toString()
    })

    child.on('close', (code: number | null) => {
      if (code === 0) {
        console.log(`✅ ${testFile} passed`)
        console.log(stdout)
        resolve({ testFile, success: true, stdout, stderr })
      } else {
        console.error(`❌ ${testFile} failed (exit code: ${code})`)
        console.error(stderr)
        console.error(stdout)
        resolve({
          testFile,
          success: false,
          stdout,
          stderr,
          code: code || undefined
        })
      }
    })

    child.on('error', (error: Error) => {
      console.error(`❌ ${testFile} error:`, error)
      reject({ testFile, error })
    })
  })
}

async function runAllTests(): Promise<void> {
  const results: (TestResult | TestError)[] = []

  for (const testFile of testFiles) {
    try {
      const result: TestResult = await runTest(testFile)
      results.push(result)
      console.log('') // Add spacing between tests
    } catch (error) {
      results.push(error as TestError)
      console.error(`Failed to run ${testFile}:`, error)
      console.log('')
    }
  }

  // Summary
  console.log('📊 Test Summary')
  console.log('===============')

  const passed: number = results.filter(
    (r): r is TestResult => 'success' in r && r.success
  ).length
  const failed: number = results.filter(
    (r): r is TestResult => 'success' in r && !r.success
  ).length

  console.log(`✅ Passed: ${passed}`)
  console.log(`❌ Failed: ${failed}`)
  console.log(`📁 Total:  ${results.length}`)

  if (failed > 0) {
    console.log('')
    console.log('Failed tests:')
    results
      .filter((r): r is TestResult => 'success' in r && !r.success)
      .forEach((r: TestResult) => {
        console.log(`  - ${r.testFile}`)
      })
    process.exit(1)
  } else {
    console.log('')
    console.log('🎉 All tests passed!')
    process.exit(0)
  }
}

// Check if specific test file was requested
const requestedTest: string | undefined = process.argv[2]
if (requestedTest && testFiles.includes(requestedTest)) {
  runTest(requestedTest)
    .then((result: TestResult) => {
      process.exit(result.success ? 0 : 1)
    })
    .catch((error: TestError) => {
      console.error('Error running test:', error)
      process.exit(1)
    })
} else {
  runAllTests().catch((error: Error) => {
    console.error('Error running tests:', error)
    process.exit(1)
  })
}
