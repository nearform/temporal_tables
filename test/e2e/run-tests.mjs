#!/usr/bin/env node

import { spawn } from 'child_process'
import { fileURLToPath } from 'url'
import { dirname, join } from 'path'

const __filename = fileURLToPath(import.meta.url)
const __dirname = dirname(__filename)

const testFiles = [
  'test-static-generator.ts',
  'test-legacy.ts', 
  'test-event-trigger.ts',
  'test-integration.ts'
]

const env = {
  ...process.env,
  PGHOST: process.env.PGHOST || 'localhost',
  PGPORT: process.env.PGPORT || '5432',
  PGUSER: process.env.PGUSER || 'postgres',
  PGPASSWORD: process.env.PGPASSWORD || 'password',
  PGDATABASE: process.env.PGDATABASE || 'postgres'
}

console.log('🚀 Running Temporal Tables E2E Tests')
console.log('=====================================')
console.log(`Database: ${env.PGUSER}@${env.PGHOST}:${env.PGPORT}/${env.PGDATABASE}`)
console.log('')

async function runTest(testFile) {
  return new Promise((resolve, reject) => {
    console.log(`📋 Running ${testFile}...`)
    
    const testPath = join(__dirname, testFile)
    const child = spawn('node', ['--test', testPath], {
      env,
      stdio: 'pipe'
    })

    let stdout = ''
    let stderr = ''

    child.stdout.on('data', (data) => {
      stdout += data.toString()
    })

    child.stderr.on('data', (data) => {
      stderr += data.toString()
    })

    child.on('close', (code) => {
      if (code === 0) {
        console.log(`✅ ${testFile} passed`)
        console.log(stdout)
        resolve({ testFile, success: true, stdout, stderr })
      } else {
        console.error(`❌ ${testFile} failed (exit code: ${code})`)
        console.error(stderr)
        console.error(stdout)
        resolve({ testFile, success: false, stdout, stderr, code })
      }
    })

    child.on('error', (error) => {
      console.error(`❌ ${testFile} error:`, error)
      reject({ testFile, error })
    })
  })
}

async function runAllTests() {
  const results = []
  
  for (const testFile of testFiles) {
    try {
      const result = await runTest(testFile)
      results.push(result)
      console.log('') // Add spacing between tests
    } catch (error) {
      results.push(error)
      console.error(`Failed to run ${testFile}:`, error)
      console.log('')
    }
  }

  // Summary
  console.log('📊 Test Summary')
  console.log('===============')
  
  const passed = results.filter(r => r.success).length
  const failed = results.filter(r => !r.success).length
  
  console.log(`✅ Passed: ${passed}`)
  console.log(`❌ Failed: ${failed}`)
  console.log(`📁 Total:  ${results.length}`)
  
  if (failed > 0) {
    console.log('')
    console.log('Failed tests:')
    results.filter(r => !r.success).forEach(r => {
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
const requestedTest = process.argv[2]
if (requestedTest && testFiles.includes(requestedTest)) {
  runTest(requestedTest).then(result => {
    process.exit(result.success ? 0 : 1)
  }).catch(error => {
    console.error('Error running test:', error)
    process.exit(1)
  })
} else {
  runAllTests().catch(error => {
    console.error('Error running tests:', error)
    process.exit(1)
  })
}
