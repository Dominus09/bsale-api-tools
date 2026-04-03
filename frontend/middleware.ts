import type { NextRequest } from "next/server"
import { NextResponse } from "next/server"

/**
 * CSP aplicada solo a respuestas HTML para no interferir con chunks / RSC.
 * Si Cloudflare (u otro proxy) añade otra cabecera CSP, el navegador exige cumplir
 * ambas: en ese caso relajar o quitar la CSP del panel de Cloudflare.
 */
const CONTENT_SECURITY_POLICY = [
  "default-src 'self'",
  "base-uri 'self'",
  "form-action 'self'",
  "frame-ancestors 'self'",
  "script-src 'self' 'unsafe-inline' 'unsafe-eval' https://static.cloudflareinsights.com https://va.vercel-scripts.com",
  "connect-src 'self' https://static.cloudflareinsights.com https://api.quillotana.cl https://vitals.vercel-insights.com https://va.vercel-scripts.com",
  "img-src 'self' data: blob: https://hebbkx1anhila5yf.public.blob.vercel-storage.com",
  "style-src 'self' 'unsafe-inline'",
  "font-src 'self' data:",
].join("; ")

export function middleware(request: NextRequest) {
  if (process.env.NODE_ENV !== "production") {
    return NextResponse.next()
  }

  const { pathname } = request.nextUrl
  if (
    pathname.startsWith("/_next/") ||
    pathname === "/favicon.ico" ||
    /\.(?:ico|png|jpg|jpeg|svg|gif|webp|woff2?)$/i.test(pathname)
  ) {
    return NextResponse.next()
  }

  const accept = request.headers.get("accept") ?? ""
  if (!accept.includes("text/html")) {
    return NextResponse.next()
  }

  const res = NextResponse.next()
  res.headers.set("Content-Security-Policy", CONTENT_SECURITY_POLICY)
  return res
}

export const config = {
  matcher: ["/:path*"],
}
