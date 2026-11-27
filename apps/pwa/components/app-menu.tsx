"use client"

import Link from "next/link"
import { AppLink } from "@/components/app-link"
import { useEffect, useState } from "react"
import { createPortal } from "react-dom"
import { usePathname } from "next/navigation"
import { getSessionEmail, logoutAndRedirect } from "@/lib/auth"
import { getActiveModuleId, moduleLabel, setActiveModuleId } from "@/lib/modules"
import { useLocalTools } from "@/components/local-tools-context"
import { OfflineStatusIndicator } from "@/components/offline-status-indicator"

function IconLogout() {
  return (
    <svg
      className="drawer-item-icon"
      width="18"
      height="18"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.9"
      aria-hidden
    >
      <path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4" />
      <path d="M16 17l5-5-5-5" />
      <path d="M21 12H9" />
    </svg>
  )
}

export function AppMenu() {
  const pathname = usePathname()
  const [open, setOpen] = useState(false)
  const [email, setEmail] = useState("—")
  const [moduleName, setModuleName] = useState("Operativa")
  const [mounted, setMounted] = useState(false)
  const { tools, requestAction } = useLocalTools()
  const inChofer = pathname?.startsWith("/despachos")

  useEffect(() => {
    setMounted(true)
  }, [])

  useEffect(() => {
    setEmail(getSessionEmail() || "—")
    const id =
      pathname?.startsWith("/despachos")
        ? "choferes"
        : pathname?.startsWith("/vendedores")
          ? "vendedores"
          : pathname?.startsWith("/bodega")
            ? "bodega"
            : pathname?.startsWith("/supervision")
              ? "supervision"
              : pathname === "/modulos" || pathname?.startsWith("/modulos/")
                ? null
                : getActiveModuleId()
    setModuleName(moduleLabel(id))
  }, [open, pathname])

  useEffect(() => {
    if (!open) return
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") setOpen(false)
    }
    const prevOverflow = document.body.style.overflow
    document.body.style.overflow = "hidden"
    window.addEventListener("keydown", onKey)
    return () => {
      document.body.style.overflow = prevOverflow
      window.removeEventListener("keydown", onKey)
    }
  }, [open])

  function logout() {
    setOpen(false)
    void logoutAndRedirect("/login")
  }

  function runLocal(action: "gps" | "photo" | "navigate" | "history") {
    setOpen(false)
    if (action === "navigate" && tools?.navigationUrl) {
      window.open(tools.navigationUrl, "_blank", "noopener,noreferrer")
      return
    }
    requestAction(action)
  }

  const drawer =
    open && mounted
      ? createPortal(
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
                <button
                  type="button"
                  className="btn btn-ghost btn-compact"
                  onClick={() => setOpen(false)}
                >
                  Cerrar
                </button>
              </div>

              <div className="drawer-scroll">
                <section className="drawer-section">
                  <h3>Cuenta</h3>
                  <p className="drawer-user">{email}</p>
                  <p className="drawer-profile">Módulo activo · {moduleName}</p>
                </section>

                <section className="drawer-section">
                  <h3>Navegación</h3>
                  <div className="drawer-actions">
                    <AppLink
                      className="drawer-item"
                      href="/modulos"
                      onClick={() => {
                        setActiveModuleId(null)
                        setOpen(false)
                      }}
                    >
                      Cambiar módulo
                    </AppLink>
                    {inChofer ? (
                      <Link
                        className="drawer-item"
                        href="/despachos"
                        onClick={() => setOpen(false)}
                      >
                        Mis despachos
                      </Link>
                    ) : null}
                    {inChofer && tools?.available && tools.backHref ? (
                      <Link
                        className="drawer-item"
                        href={tools.backHref}
                        onClick={() => setOpen(false)}
                      >
                        ← Volver a despachos / clientes
                      </Link>
                    ) : null}
                  </div>
                </section>

                {inChofer ? (
                  <section className="drawer-section">
                    <h3>Herramientas del local</h3>
                    {tools?.available ? (
                      <div className="drawer-actions">
                        <button type="button" className="drawer-item" onClick={() => runLocal("gps")}>
                          Registrar / corregir GPS
                        </button>
                        <button
                          type="button"
                          className="drawer-item"
                          onClick={() => runLocal("photo")}
                        >
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
                        <button
                          type="button"
                          className="drawer-item"
                          onClick={() => runLocal("history")}
                        >
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
                ) : null}

                {inChofer ? (
                  <section className="drawer-section">
                    <h3>Pruebas</h3>
                    <div className="drawer-actions">
                      <Link
                        className="drawer-item"
                        href="/pruebas/reportes-whatsapp"
                        onClick={() => setOpen(false)}
                      >
                        Reportes WhatsApp (ficticios)
                      </Link>
                    </div>
                    <p className="muted drawer-note">
                      Solo demostración. No escribe entregas reales ni usa enlaces de grupo.
                    </p>
                  </section>
                ) : null}
              </div>

              <div className="drawer-footer">
                <button
                  type="button"
                  className="drawer-item drawer-item-danger drawer-item-logout"
                  onClick={logout}
                >
                  <IconLogout />
                  Cerrar sesión
                </button>
              </div>
            </aside>
          </div>,
          document.body,
        )
      : null

  return (
    <div className="topbar-actions">
      <OfflineStatusIndicator />
      <button
        type="button"
        className="icon-btn"
        aria-label="Menú"
        aria-expanded={open}
        onClick={() => setOpen(true)}
      >
        <span className="icon-btn-bars" aria-hidden />
      </button>
      {drawer}
    </div>
  )
}
