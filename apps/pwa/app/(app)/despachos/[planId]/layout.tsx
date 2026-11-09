"use client"

import Link from "next/link"
import { useParams } from "next/navigation"
import { PlanTabs } from "@/components/plan-tabs"

export default function PlanLayout({ children }: { children: React.ReactNode }) {
  const params = useParams()
  const planId = Number(params.planId)

  return (
    <div className="plan-shell">
      <div className="plan-top">
        <Link href="/despachos" className="muted">
          ← Mis despachos
        </Link>
        {Number.isFinite(planId) && planId > 0 ? <PlanTabs planId={planId} /> : null}
      </div>
      <div className="plan-body">{children}</div>
    </div>
  )
}
