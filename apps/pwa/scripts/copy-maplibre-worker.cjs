/**
 * MapLibre GL JS v6: el worker no se resuelve bien desde webpack/Next.
 * Copia worker + shared a public/ para setWorkerUrl('/maplibre-gl-worker.mjs').
 */
const fs = require("fs")
const path = require("path")

const root = process.cwd()
const dist = path.join(root, "node_modules", "maplibre-gl", "dist")
const pub = path.join(root, "public")
const files = ["maplibre-gl-worker.mjs", "maplibre-gl-shared.mjs"]

if (!fs.existsSync(dist)) {
  console.error("[copy-maplibre-worker] Falta node_modules/maplibre-gl/dist")
  process.exit(1)
}
fs.mkdirSync(pub, { recursive: true })
for (const name of files) {
  const src = path.join(dist, name)
  const dest = path.join(pub, name)
  if (!fs.existsSync(src)) {
    console.warn(`[copy-maplibre-worker] omitido (no existe): ${name}`)
    continue
  }
  fs.copyFileSync(src, dest)
  const size = fs.statSync(dest).size
  console.log(`[copy-maplibre-worker] ${name} → public/ (${size} bytes)`)
}
