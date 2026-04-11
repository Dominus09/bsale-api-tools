/**
 * Producción (p. ej. work.quillotana.cl):
 * - Raíz: app/page.tsx + force-dynamic + home-client (evita 404 si el host espera HTML dinámico).
 * - CSP: solo middleware.ts (NODE_ENV=production). Si el proxy añade otra CSP, quitarla allí.
 * - Producción con output "standalone": usar `pnpm start` → node .next/standalone/server.js
 *   (Next 16+ no soporta `next start` con standalone). Ver scripts/copy-standalone-assets.cjs.
 * - _headers en public/: útil en Cloudflare Pages; no lo usa el servidor standalone.
 */
/** @type {import('next').NextConfig} */
const nextConfig = {
  output: "standalone",
  typescript: {
    ignoreBuildErrors: true,
  },
  images: {
    unoptimized: true,
  },
  experimental: {
    optimizePackageImports: ["lucide-react", "recharts"],
  },
  // Evita 404 en /favicon.ico (no hay .ico en public/; el navegador lo pide siempre)
  // Proxy same-origin: el front hace fetch a /api-upstream/... y Next reenvía al API real
  // (evita CORS cuando el panel está en test.quillotana.cl y el API en api.quillotana.cl).
  async rewrites() {
    const apiBase = (
      process.env.API_PROXY_TARGET ||
      process.env.NEXT_PUBLIC_API_URL ||
      "https://api.quillotana.cl"
    )
      .trim()
      .replace(/\/$/, "")
    return [
      { source: "/favicon.ico", destination: "/icon.svg" },
      { source: "/api-upstream/:path*", destination: `${apiBase}/:path*` },
    ]
  },
}

export default nextConfig
