"use client"

import Link from "next/link"
import { AuthGate } from "@/components/auth-gate"
import { clearSession } from "@/lib/auth"

export default function AppLayout({ children }: { children: React.ReactNode }) {
  function logout() {
    clearSession()
    window.location.href = "/login"
  }

  return (
    <AuthGate>
      <header className="topbar">
        <div>
          <h1>
            <Link href="/despachos">Mis despachos</Link>
          </h1>
          <div className="muted" style={{ fontSize: "0.75rem" }}>
            Solo lectura · Chofer
          </div>
        </div>
        <button type="button" className="btn btn-ghost" style={{ width: "auto" }} onClick={logout}>
          Salir
        </button>
      </header>
      <div className="main">{children}</div>
    </AuthGate>
  )
}
