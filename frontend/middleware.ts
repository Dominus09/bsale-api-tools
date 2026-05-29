import type { NextRequest } from "next/server"
import { NextResponse } from "next/server"

import { getApiOrigin } from "@/lib/api-base"

/**
 * ÚNICA fuente de Content-Security-Policy en esta app: este archivo hace un solo setHeader.
 * No hay CSP en next.config, ni en lib/csp.ts (eliminado), ni cabecera CSP en public/_headers.
 *
 * Política base (pedida):
 *   default-src 'self'; script-src ... static.cloudflareinsights.com; connect-src 'self' https://static.cloudflareinsights.com;
 *   img-src 'self' data: blob:; style-src ...; font-src ...
 *
 * connect-src se amplía con https://cloudflareinsights.com (RUM/beacon) y el origin del API
 * (NEXT_PUBLIC_API_URL / default), si no los fetch del front fallan.
 *
 * img-src incluye el host del logo en Vercel Blob usado en la home.
 * img-src incluye CARTO basemaps (Leaflet Mapa Rutero / distribuidora) y OSM tiles
 * (operaciones recorrido / mapa operacional: {s}.tile.openstreetmap.org).
 *
 * default-src 'none' NO sale de este repo. Si el navegador lo muestra, lo añade el proxy
 * (p. ej. Cloudflare Transform Rules): hay que quitar esa regla; varias CSP se aplican todas.
 */
function contentSecurityPolicy(): string {
  const apiOrigin = getApiOrigin()
  return [
    "default-src 'self'",
    "script-src 'self' 'unsafe-inline' 'unsafe-eval' https://static.cloudflareinsights.com",
    `connect-src 'self' https://static.cloudflareinsights.com https://cloudflareinsights.com ${apiOrigin}`,
    "img-src 'self' data: blob: https://hebbkx1anhila5yf.public.blob.vercel-storage.com https://*.tile.openstreetmap.org https://tile.openstreetmap.org https://*.basemaps.cartocdn.com https://cartodb-basemaps-a.global.ssl.fastly.net https://cartodb-basemaps-b.global.ssl.fastly.net https://cartodb-basemaps-c.global.ssl.fastly.net https://cartodb-basemaps-d.global.ssl.fastly.net",
    "style-src 'self' 'unsafe-inline'",
    "font-src 'self' data:",
  ].join("; ")
}

function isStaticAssetPath(pathname: string): boolean {
  if (pathname === "/favicon.ico") return true
  return /\.(?:ico|png|jpg|jpeg|svg|gif|webp|woff2?)$/i.test(pathname)
}

export function middleware(request: NextRequest) {
  if (process.env.NODE_ENV !== "production") {
    return NextResponse.next()
  }

  const { pathname } = request.nextUrl
  if (pathname === "/health") {
    return NextResponse.next()
  }
  if (pathname.startsWith("/_next/")) {
    return NextResponse.next()
  }
  if (isStaticAssetPath(pathname)) {
    return NextResponse.next()
  }

  if (request.method !== "GET") {
    return NextResponse.next()
  }

  const res = NextResponse.next()
  res.headers.set("Content-Security-Policy", contentSecurityPolicy())
  return res
}

export const config = {
  matcher: ["/:path*"],
}
