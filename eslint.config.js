import globals from 'globals'
import js from '@eslint/js'
import prettierRecommended from 'eslint-plugin-prettier/recommended'

export default [
  js.configs.recommended,
  prettierRecommended,
  {
    languageOptions: {
      globals: {
        ...globals.node
      },
      ecmaVersion: 'latest',
      sourceType: 'module'
    }
  }
]
