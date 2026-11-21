/**
 * Tras `next build` con output: "standalone", Next no copia public/ ni .next/static
 * dentro de .next/standalone. Sin esto, arrancar server.js rompe estáticos y CSS.
 * Ref: https://nextjs.org/docs/app/building-your-application/deploying#manual-setup
 */
const fs = require("fs")
const path = require("path")

function logErr(prefix, err) {
  console.error(prefix, err && err.message ? err.message : err)
  if (err && err.stack) {
    console.error(err.stack)
  }
}

function main() {
  const root = process.cwd()
  const standaloneDir = path.join(root, ".next", "standalone")
  const serverPath = path.join(standaloneDir, "server.js")

  if (!fs.existsSync(serverPath)) {
    console.error(
      "[copy-standalone-assets] Falta .next/standalone/server.js (¿next build sin output standalone?). Abortando.",
    )
    process.exit(1)
  }

  const pubSrc = path.join(root, "public")
  const pubDest = path.join(standaloneDir, "public")
  if (fs.existsSync(pubSrc)) {
    fs.rmSync(pubDest, { recursive: true, force: true })
    fs.cpSync(pubSrc, pubDest, { recursive: true })
  }

  // Bust SW caches + alinear offline-build.json (navBuildId) en cada deploy.
  try {
    const buildIdPath = path.join(root, ".next", "BUILD_ID")
    const swPath = path.join(pubDest, "sw.js")
    if (fs.existsSync(buildIdPath) && fs.existsSync(swPath)) {
      const buildId = fs.readFileSync(buildIdPath, "utf8").trim().slice(0, 16)
      let sw = fs.readFileSync(swPath, "utf8")
      const baseVer = "lq-pwa-v9-safari-no-redirect"
      sw = sw.replace(
        /const VERSION = "lq-pwa-v9-safari-no-redirect(?:-[^"]*)?"/,
        `const VERSION = "${baseVer}-${buildId}"`,
      )
      fs.writeFileSync(swPath, sw)

      const buildJson = JSON.stringify(
        {
          navBuildId: buildId,
          swVersionPrefix: baseVer,
        },
        null,
        2,
      )
      fs.writeFileSync(path.join(pubDest, "offline-build.json"), `${buildJson}\n`)
      // También en public/ fuente para next start local tras build.
      fs.writeFileSync(path.join(pubSrc, "offline-build.json"), `${buildJson}\n`)
      console.log(
        `[copy-standalone-assets] SW+offline-build stamped BUILD_ID=${buildId}`,
      )
    }
  } catch (err) {
    logErr("[copy-standalone-assets] SW stamp skipped:", err)
  }

  const staticSrc = path.join(root, ".next", "static")
  const staticDest = path.join(standaloneDir, ".next", "static")
  if (fs.existsSync(staticSrc)) {
    fs.mkdirSync(path.dirname(staticDest), { recursive: true })
    fs.rmSync(staticDest, { recursive: true, force: true })
    fs.cpSync(staticSrc, staticDest, { recursive: true })
  }

  console.log("[copy-standalone-assets] OK: public + .next/static → .next/standalone")
}

try {
  main()
} catch (err) {
  logErr("[copy-standalone-assets] Error:", err)
  process.exit(1)
}
