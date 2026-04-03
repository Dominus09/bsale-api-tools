/**
 * Producción (p. ej. work.quillotana.cl):
 * - Raíz: app/page.tsx + force-dynamic + home-client (evita 404 si el host espera HTML dinámico).
 * - CSP: middleware.ts (solo NODE_ENV=production). Si Cloudflare añade otra CSP en el panel,
 *   el navegador aplica ambas: relajar allí o quitar una. Beacon CF: static.cloudflareinsights.com.
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
}

export default nextConfig
