import { headers } from "next/headers"
import { notFound } from "next/navigation"

/**
 * Gate de servidor (RSC) — no depende de NEXT_PUBLIC ni localStorage.
 * En *.quillotana.cl siempre 404. Solo OFFLINE_V2_LAB_ENABLED=1 en server env.
 */
export default async function OfflineV2LabLayout({
  children,
}: {
  children: React.ReactNode
}) {
  const h = await headers()
  const host = (
    h.get("x-forwarded-host") ||
    h.get("host") ||
    ""
  )
    .split(":")[0]
    .toLowerCase()

  if (
    host === "api.quillotana.cl" ||
    host.endsWith(".quillotana.cl") ||
    host.includes("quillotana.cl")
  ) {
    notFound()
  }

  if (process.env.OFFLINE_V2_LAB_ENABLED !== "1") {
    notFound()
  }

  const gate = (process.env.OFFLINE_V2_LAB_GATE_TOKEN || "").trim()
  if (gate) {
    const hdr = (h.get("x-offline-v2-lab-gate") || "").trim()
    const cookie = h.get("cookie") || ""
    const cookieOk = cookie.split(";").some((part) => {
      const [k, ...rest] = part.trim().split("=")
      return k === "lq_offline_v2_lab_gate" && rest.join("=") === gate
    })
    if (hdr !== gate && !cookieOk) {
      notFound()
    }
  }

  return children
}
