/**
 * Restricción de servidor para rutas de laboratorio Offline V2.
 *
 * /pruebas/offline-v2-lab solo si:
 *   OFFLINE_V2_LAB_ENABLED=1 (o NEXT_PUBLIC_OFFLINE_V2_LAB_ENABLED=1) Y
 *   host NO es *.quillotana.cl
 * En cualquier otro caso → 404.
 *
 * NEXT_PUBLIC_* se usa además de la env de servidor porque Middleware Edge
 * en Next puede no ver todas las env privadas en runtime.
 */

import { NextResponse } from "next/server"
import type { NextRequest } from "next/server"

function isQuillotanaHost(hostHeader: string | null): boolean {
  const h = String(hostHeader || "")
    .split(":")[0]
    .toLowerCase()
  if (!h) return false
  return (
    h === "api.quillotana.cl" ||
    h.endsWith(".quillotana.cl") ||
    h.includes("quillotana.cl")
  )
}

function labEnabled(): boolean {
  return (
    process.env.OFFLINE_V2_LAB_ENABLED === "1" ||
    process.env.NEXT_PUBLIC_OFFLINE_V2_LAB_ENABLED === "1"
  )
}

export function middleware(request: NextRequest) {
  const path = request.nextUrl.pathname
  if (!path.startsWith("/pruebas/offline-v2-lab")) {
    return NextResponse.next()
  }

  const host =
    request.headers.get("x-forwarded-host") ||
    request.headers.get("host") ||
    ""

  if (!labEnabled() || isQuillotanaHost(host)) {
    return new NextResponse("Not Found", {
      status: 404,
      headers: {
        "Cache-Control": "no-store",
        "X-Offline-V2-Lab": "blocked",
      },
    })
  }

  return NextResponse.next()
}

export const config = {
  matcher: ["/pruebas/offline-v2-lab", "/pruebas/offline-v2-lab/:path*"],
}
