"use client"

import Link from "next/link"
import { AuthGate } from "@/components/auth-gate"
import { AppMenu } from "@/components/app-menu"
import { LocalToolsProvider } from "@/components/local-tools-context"
import { BRAND } from "@/lib/brand"

function UserAvatar() {
  return (
    <span className="topbar-avatar" aria-hidden>
      <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8">
        <circle cx="12" cy="8" r="3.5" />
        <path d="M5 19c1.8-3.2 4.2-4.8 7-4.8s5.2 1.6 7 4.8" />
      </svg>
    </span>
  )
}

export default function AppLayout({ children }: { children: React.ReactNode }) {
  return (
    <AuthGate>
      <LocalToolsProvider>
        <header className="topbar">
          <div className="topbar-brand">
            <UserAvatar />
            <div className="topbar-title">
              <h1>
                <Link href="/despachos">{BRAND.name}</Link>
              </h1>
              <div className="muted topbar-sub">
                {BRAND.subtitle} · Chofer
              </div>
            </div>
          </div>
          <AppMenu />
        </header>
        <div className="main">{children}</div>
      </LocalToolsProvider>
    </AuthGate>
  )
}
