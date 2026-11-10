"use client"

import Link from "next/link"
import { AuthGate } from "@/components/auth-gate"
import { AppMenu } from "@/components/app-menu"
import { LocalToolsProvider } from "@/components/local-tools-context"

export default function AppLayout({ children }: { children: React.ReactNode }) {
  return (
    <AuthGate>
      <LocalToolsProvider>
        <header className="topbar">
          <div className="topbar-title">
            <h1>
              <Link href="/despachos">Mis despachos</Link>
            </h1>
            <div className="muted topbar-sub">Entregas y evidencias · Chofer</div>
          </div>
          <AppMenu />
        </header>
        <div className="main">{children}</div>
      </LocalToolsProvider>
    </AuthGate>
  )
}
