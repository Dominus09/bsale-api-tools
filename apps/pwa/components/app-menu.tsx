"use client"

import Link from "next/link"
import { useEffect, useState } from "react"
import { clearSession } from "@/lib/auth"
import { useLocalTools } from "@/components/local-tools-context"

function sessionEmail(): string {
  if (typeof window === "undefined") return "—"
  return localStorage.getItem("pwa_email") || "—"
}

export function AppMenu() {
  const [open, setOpen] = useState(false)
  const [email, setEmail] = useState("—")
  const { tools, requestAction } = useLocalTools()

  useEffect(() => {
    setEmail(sessionEmail())
  }, [open])

  useEffect(() => {
    if (!open) return
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") setOpen(false)
    }
    window.addEventListener("keydown", onKey)
    return () => window.removeEventListener("keydown", onKey)
  }, [open])

  function logout() {
    clearSession()
    window.location.href = "/login"
  }

  function runLocal(action: "gps" | "photo" | "navigate" | "history") {
    setOpen(false)
    if (action === "navigate" && tools?.navigationUrl) {
      window.open(tools.navigationUrl, "_blank", "noopener,noreferrer")
      return
    }
    requestAction(action)
  }

  return (
    <>
      <button
        type="button"
        className="icon-btn"
        aria-label="Menú"
        aria-expanded={open}
        onClick={() => setOpen(true)}
      >
        <span className="icon-btn-bars" aria-hidden />
      </button>

      {open ? (
        <div className="drawer-root" role="dialog" aria-modal="true" aria-label="Menú">
          <button
            type="button"
            className="drawer-backdrop"
            aria-label="Cerrar menú"
            onClick={() => setOpen(false)}
          />
          <aside className="drawer-panel">
            <div className="drawer-head">
              <strong>Menú</strong>
              <button type="button" className="btn btn-ghost btn-compact" onClick={() => setOpen(false)}>
                Cerrar
              </button>
            </div>

            {tools?.available && tools.backHref ? (
              <section className="drawer-section">
                <h3>Navegación</h3>
                <div className="drawer-actions">
                  <Link
                    className="drawer-item"
                    href={tools.backHref}
                    onClick={() => setOpen(false)}
                  >
                    ← Volver a despachos / clientes
                  </Link>
                </div>
              </section>
            ) : null}

            <section className="drawer-section">
              <h3>Herramientas del local</h3>
              {tools?.available ? (
                <div className="drawer-actions">
                  <button type="button" className="drawer-item" onClick={() => runLocal("gps")}>
                    Registrar / corregir GPS
                  </button>
                  <button type="button" className="drawer-item" onClick={() => runLocal("photo")}>
                    Tomar / cambiar fotografía
                  </button>
                  <button
                    type="button"
                    className="drawer-item"
                    disabled={!tools.canNavigate}
                    onClick={() => runLocal("navigate")}
                  >
                    Cómo llegar
                  </button>
                  <button type="button" className="drawer-item" onClick={() => runLocal("history")}>
                    Historial de ubicación
                  </button>
                  {tools.locationLabel ? (
                    <p className="muted drawer-note">{tools.locationLabel}</p>
                  ) : null}
                </div>
              ) : (
                <p className="muted drawer-note">
                  Abra la ficha de un cliente para usar GPS, fachada y navegación.
                </p>
              )}
            </section>

            <section className="drawer-section">
              <h3>Pruebas</h3>
              <div className="drawer-actions">
                <Link
                  className="drawer-item"
                  href="/pruebas/reportes-whatsapp"
                  onClick={() => setOpen(false)}
                >
                  🧪 Reportes WhatsApp (ficticios)
                </Link>
              </div>
              <p className="muted drawer-note">
                Solo demostración. No escribe entregas reales ni usa enlaces de grupo.
              </p>
            </section>

            <section className="drawer-section">
              <h3>Mi cuenta</h3>
              <p className="drawer-user">{email}</p>
              <button type="button" className="drawer-item drawer-item-danger" onClick={logout}>
                Cerrar sesión
              </button>
            </section>
          </aside>
        </div>
      ) : null}
    </>
  )
}
