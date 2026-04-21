/**
 * Orquesta `next build` + post-build con logging explícito (exit code, señales, stacks).
 * Uso: npm run build | npm run build:debug
 *
 * Memoria OOM en Docker: ver Dockerfile (NODE_OPTIONS) o
 *   NODE_OPTIONS="--max-old-space-size=2048" npm run build
 *
 * Omitir post-build (copia standalone): BUILD_SKIP_STANDALONE_COPY=1
 * Solo Next (sin post-build): npm run build:next-only
 */
const { spawnSync } = require("child_process")
const path = require("path")

const root = path.resolve(__dirname, "..")

function logErr(prefix, err) {
  console.error(prefix, err && err.message ? err.message : err)
  if (err && err.stack) {
    console.error(err.stack)
  }
}

process.on("uncaughtException", (err) => {
  logErr("[build] uncaughtException:", err)
  process.exit(1)
})

process.on("unhandledRejection", (reason) => {
  console.error("[build] unhandledRejection:", reason)
  if (reason && typeof reason === "object" && "stack" in reason) {
    console.error(reason.stack)
  }
  process.exit(1)
})

if (process.argv.includes("--trace-deprecation")) {
  const cur = process.env.NODE_OPTIONS || ""
  if (!/\b--trace-deprecation\b/.test(cur)) {
    process.env.NODE_OPTIONS = `${cur} --trace-deprecation`.trim()
  }
}

console.log("[build] cwd=%s", root)
console.log("[build] NODE_OPTIONS=%s", process.env.NODE_OPTIONS || "(none)")
console.log("[build] starting: next build --webpack")

const nextCli = path.join(root, "node_modules", "next", "dist", "bin", "next")
const nextResult = spawnSync(process.execPath, [nextCli, "build", "--webpack"], {
  cwd: root,
  stdio: "inherit",
  env: process.env,
})

if (nextResult.error) {
  logErr("[build] failed to spawn next:", nextResult.error)
  process.exit(1)
}

const code = nextResult.status
if (code !== 0) {
  console.error(
    "[build] next build FAILED — exitCode=%s signal=%s",
    code,
    nextResult.signal || "(none)",
  )
  console.error(
    "[build] Revisar salida de compilación arriba (webpack/TS/React). Causas frecuentes:",
    "variables no definidas, hooks fuera de componente, window/document en SSR, imports dinámicos.",
  )
  process.exit(typeof code === "number" ? code : 255)
}

console.log("[build] next build OK — running post-build: copy-standalone-assets.cjs")

if (process.env.BUILD_SKIP_STANDALONE_COPY === "1") {
  console.log(
    "[build] BUILD_SKIP_STANDALONE_COPY=1 — se omite copy-standalone-assets (solo diagnóstico).",
  )
  process.exit(0)
}

const copyScript = path.join(__dirname, "copy-standalone-assets.cjs")
const copyResult = spawnSync(process.execPath, [copyScript], {
  cwd: root,
  stdio: "inherit",
  env: process.env,
})

if (copyResult.error) {
  logErr("[build] failed to spawn copy-standalone-assets:", copyResult.error)
  process.exit(1)
}

if (copyResult.status !== 0) {
  console.error(
    "[build] copy-standalone-assets FAILED — exitCode=%s",
    copyResult.status,
  )
  console.error(
    "[build] Para aislar: npm run build:next-only (sin copia a standalone).",
  )
  process.exit(copyResult.status || 1)
}

console.log("[build] Done.")
