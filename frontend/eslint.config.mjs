// Configuración mínima de ESLint (Graphite Premium 6.0).
// Objetivo inicial: hacer cumplir las Reglas de los Hooks en todo el
// frontend para prevenir el error React #310 («Rendered more hooks than
// during the previous render»), detectado en Análisis de Márgenes.
// No se activan reglas de estilo para no generar ruido sobre el código
// existente; se puede ampliar progresivamente.
import nextPlugin from "@next/eslint-plugin-next"
import reactHooks from "eslint-plugin-react-hooks"
import tsParser from "typescript-eslint"

export default [
  {
    ignores: [
      ".next/**",
      "node_modules/**",
      "public/**",
      "scripts/**",
      "**/*.test.{ts,tsx}",
    ],
  },
  {
    files: ["**/*.{ts,tsx}"],
    languageOptions: {
      parser: tsParser.parser,
      parserOptions: {
        ecmaFeatures: { jsx: true },
        sourceType: "module",
      },
    },
    plugins: {
      "react-hooks": reactHooks,
      "@next/next": nextPlugin,
    },
    linterOptions: {
      // Existen comentarios eslint-disable heredados para reglas aún no
      // activadas (p. ej. exhaustive-deps); no deben ensuciar el resultado.
      reportUnusedDisableDirectives: "off",
    },
    rules: {
      "react-hooks/rules-of-hooks": "error",
      "@next/next/no-img-element": "off",
    },
  },
]
