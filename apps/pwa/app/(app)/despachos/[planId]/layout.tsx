"use client"

import Link from "next/link"
import { useParams, usePathname } from "next/navigation"
import { PlanTabs } from "@/components/plan-tabs"

export default function PlanLayout({ children }: { children: React.ReactNode }) {
  const params = useParams()
  const pathname = usePathname() || ""
  const planId = Number(params.planId)
  const focusEntrega = pathname.includes("/entrega")

  return (
    <div className={`plan-shell${focusEntrega ? " plan-shell-focus" : ""}`}>
      {!focusEntrega ? (
        <div className="plan-top">
          <Link href="/despachos" className="muted" style={{ fontSize: "0.85rem" }}>
            ← Inicio
          </Link>
          {Number.isFinite(planId) && planId > 0 ? <PlanTabs planId={planId} /> : null}
        </div>
      ) : null}
      <div className="plan-body">{children}</div>
    </div>
  )
}
