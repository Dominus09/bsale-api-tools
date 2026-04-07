/**
 * Producción (p. ej. work.quillotana.cl):
 * - Raíz: app/page.tsx + force-dynamic + home-client (evita 404 si el host espera HTML dinámico).
 * - CSP: middleware.ts + lib/csp.ts (solo NODE_ENV=production). Si el proxy añade otra CSP
 *   (p. ej. default-src 'none'), el navegador aplica ambas: quitar/ajustar en Cloudflare/Coolify.
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
  async rewrites() {
    return [{ source: "/favicon.ico", destination: "/icon.svg" }]
  },
}

export default nextConfig
