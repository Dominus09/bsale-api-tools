/**
 * Restricción de servidor para /pruebas/offline-v2-lab.
 *
 * ÚNICA habilitación: OFFLINE_V2_LAB_ENABLED=1 (env de servidor / runtime).
 * NUNCA NEXT_PUBLIC_*, localStorage ni headers del cliente.
 *
 * En cualquier host *.quillotana.cl → siempre 404 (producción protegida).
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

export function middleware(request: NextRequest) {
  const path = request.nextUrl.pathname
  if (!path.startsWith("/pruebas/offline-v2-lab")) {
    return NextResponse.next()
  }

  const host =
    request.headers.get("x-forwarded-host") ||
    request.headers.get("host") ||
    ""

  // Producción / cualquier Quillotana: 404 incondicional.
  if (isQuillotanaHost(host)) {
    return new NextResponse("Not Found", {
      status: 404,
      headers: {
        "Cache-Control": "no-store",
        "X-Offline-V2-Lab": "blocked-prod-host",
      },
    })
  }

  // Solo env de servidor (no pública al bundle).
  if (process.env.OFFLINE_V2_LAB_ENABLED !== "1") {
    return new NextResponse("Not Found", {
      status: 404,
      headers: {
        "Cache-Control": "no-store",
        "X-Offline-V2-Lab": "blocked-flag",
      },
    })
  }

  // Gate de laboratorio: cookie/header de servidor (no localStorage).
  const gate = (process.env.OFFLINE_V2_LAB_GATE_TOKEN || "").trim()
  if (gate) {
    const hdr = (request.headers.get("x-offline-v2-lab-gate") || "").trim()
    const cookie = request.cookies.get("lq_offline_v2_lab_gate")?.value || ""
    if (hdr !== gate && cookie !== gate) {
      return new NextResponse("Forbidden", {
        status: 403,
        headers: {
          "Cache-Control": "no-store",
          "X-Offline-V2-Lab": "blocked-gate",
        },
      })
    }
  }

  return NextResponse.next()
}

export const config = {
  matcher: ["/pruebas/offline-v2-lab", "/pruebas/offline-v2-lab/:path*"],
}
