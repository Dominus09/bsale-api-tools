"use client"

import Link from "next/link"
import { usePathname } from "next/navigation"

export function PlanTabs({ planId }: { planId: number }) {
  const pathname = usePathname() || ""
  const base = `/despachos/${planId}`
  const tabs = [
    { href: base, label: "Resumen", match: (p: string) => p === base },
    {
      href: `${base}/clientes`,
      label: "Clientes",
      match: (p: string) => p.startsWith(`${base}/clientes`),
    },
    {
      href: `${base}/mapa`,
      label: "Mapa",
      match: (p: string) => p.startsWith(`${base}/mapa`),
    },
  ]

  return (
    <nav className="plan-tabs" aria-label="Secciones del despacho">
      {tabs.map((t) => {
        const active = t.match(pathname)
        return (
          <Link key={t.href} href={t.href} className={active ? "plan-tab active" : "plan-tab"}>
            {t.label}
          </Link>
        )
      })}
    </nav>
  )
}
