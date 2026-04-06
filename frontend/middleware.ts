import type { NextRequest } from "next/server"
import { NextResponse } from "next/server"

import { buildContentSecurityPolicy } from "@/lib/csp"

/**
 * CSP en documentos y navegación same-origin.
 * No aplica a /_next/* ni a estáticos con extensión (evita interferir con chunks).
 *
 * Si un proxy (Cloudflare, Traefik/Coolify) envía OTRA cabecera CSP (p. ej. default-src 'none'),
 * el navegador exige cumplir TODAS las políticas: hay que quitar o relajar la del proxy.
 */
function isStaticAssetPath(pathname: string): boolean {
  if (pathname === "/favicon.ico") return true
  return /\.(?:ico|png|jpg|jpeg|svg|gif|webp|woff2?)$/i.test(pathname)
}

export function middleware(request: NextRequest) {
  if (process.env.NODE_ENV !== "production") {
    return NextResponse.next()
  }

  const { pathname } = request.nextUrl
  if (pathname.startsWith("/_next/")) {
    return NextResponse.next()
  }
  if (isStaticAssetPath(pathname)) {
    return NextResponse.next()
  }

  // Sin rutas /api locales: GET fuera de estáticos son páginas o RSC; CSP aquí es segura.
  // Antes se exigía Accept: text/html; algunos proxies lo cambian y la app no enviaba CSP.
  if (request.method !== "GET") {
    return NextResponse.next()
  }

  const res = NextResponse.next()
  res.headers.set("Content-Security-Policy", buildContentSecurityPolicy())
  return res
}

export const config = {
  matcher: ["/:path*"],
}
