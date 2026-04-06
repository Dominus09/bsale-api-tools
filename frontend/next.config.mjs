/**
 * Producción (p. ej. work.quillotana.cl):
 * - Raíz: app/page.tsx + force-dynamic + home-client (evita 404 si el host espera HTML dinámico).
 * - CSP: middleware.ts + lib/csp.ts (solo NODE_ENV=production). Si el proxy añade otra CSP
 *   (p. ej. default-src 'none'), el navegador aplica ambas: quitar/ajustar en Cloudflare/Coolify.
 * - _headers en public/: útil en Cloudflare Pages estático; no lo usa `next start`.
 */
/** @type {import('next').NextConfig} */
const nextConfig = {
  typescript: {
    ignoreBuildErrors: true,
  },
  eslint: {
    // Evita que eslint bloquee o alargue el build en CI/producción
    ignoreDuringBuilds: true,
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
