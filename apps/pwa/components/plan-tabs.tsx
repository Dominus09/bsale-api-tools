"use client"

import { usePathname } from "next/navigation"
import { AppLink } from "@/components/app-link"

function TabIcon({ label }: { label: string }) {
  return <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
    {label === "Resumen" ? <><rect x="4" y="4" width="6" height="6" rx="1"/><rect x="14" y="4" width="6" height="6" rx="1"/><rect x="4" y="14" width="6" height="6" rx="1"/><rect x="14" y="14" width="6" height="6" rx="1"/></> :
      label === "Clientes" ? <><circle cx="9" cy="8" r="3"/><path d="M3 20v-2a6 6 0 0 1 12 0v2m2-14a3 3 0 0 1 0 6m4 8v-2a5 5 0 0 0-3-4"/></> :
      <><path d="m3 6 6-3 6 3 6-3v15l-6 3-6-3-6 3V6Zm6-3v15m6-12v15"/></>}
  </svg>
}

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
          <AppLink key={t.href} href={t.href} className={active ? "plan-tab active" : "plan-tab"} aria-current={active ? "page" : undefined}>
            <TabIcon label={t.label} />
            <span>{t.label}</span>
          </AppLink>
        )
      })}
    </nav>
  )
}
