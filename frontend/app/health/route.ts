import { NextResponse } from "next/server"

/** Para Coolify / monitoreo: si esto devuelve 200, el proceso Next sigue arriba. */
export const dynamic = "force-dynamic"

export async function GET() {
  return NextResponse.json(
    { ok: true, service: "front-erp", t: new Date().toISOString() },
    {
      status: 200,
      headers: {
        "Cache-Control": "no-store, max-age=0",
      },
    },
  )
}
