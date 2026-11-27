"use client"

import { useEffect, useMemo, useState } from "react"
import { fetchMe } from "@/lib/api"
import { getSessionUserId } from "@/lib/auth"
import { getOfflineMeta, isLikelyOfflineError } from "@/lib/offline-store"
import { AppLink } from "@/components/app-link"
import { modulesForProfiles, setActiveModuleId, type PwaModuleId } from "@/lib/modules"

function ModuleIcon({ id }: { id: PwaModuleId }) {
  return (
    <svg width="26" height="26" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.65" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
      {id === "choferes" ? <>
        <path d="M3 5h12v13H3zM15 9h4l3 4v5h-7" />
        <circle cx="7" cy="18" r="2.5" fill="var(--surface-2)" />
        <circle cx="18" cy="18" r="2.5" fill="var(--surface-2)" />
      </> : id === "vendedores" ? <>
        <path d="M3 4h2l2.5 11h11L21 7H6M9 19h.01M18 19h.01" />
        <circle cx="9" cy="19" r="1.5" /><circle cx="18" cy="19" r="1.5" />
      </> : id === "bodega" ? <>
        <path d="m3 9 9-5 9 5v12H3zM7 21v-9h10v9M7 15h10M7 18h10" />
      </> : <>
        <rect x="3" y="4" width="18" height="16" rx="3" />
        <path d="M7 15v-3m5 3V8m5 7v-5" />
      </>}
    </svg>
  )
}

export default function ModulosPage() {
  const [profiles, setProfiles] = useState<string[] | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [offline, setOffline] = useState(false)
  const [revision, setRevision] = useState(0)

  useEffect(() => {
    setActiveModuleId(null)
  }, [])

  useEffect(() => {
    let cancelled = false
    let sequence = 0
    async function load() {
      const request = ++sequence
      const current = () => !cancelled && request === sequence
      setError(null)
      try {
        if (typeof navigator !== "undefined" && navigator.onLine === false) {
          throw new Error("Sin conexión")
        }
        const me = await fetchMe()
        if (!current()) return
        setOffline(false)
        setProfiles(me.has_pwa_access ? me.profiles || [] : [])
      } catch (err) {
        if (!current()) return
        // No usar permisos antiguos ante una denegación real del servidor.
        const status = err && typeof err === "object" && "status" in err
          ? Number(err.status) : null
        if (status !== 401 && status !== 403 && isLikelyOfflineError(err)) {
          const uid = getSessionUserId()
          if (uid) {
            const meta = await getOfflineMeta(uid).catch(() => null)
            if (!current()) return
            if (uid === getSessionUserId() && meta?.profiles?.length) {
              setOffline(true)
              setProfiles(meta.profiles)
              return
            }
          }
        }
        if (current()) {
          setProfiles(null)
          setError("No se pudieron verificar tus módulos. Revisa tu conexión e inténtalo nuevamente.")
        }
      }
    }
    void load()
    const onOnline = () => { setProfiles(null); void load() }
    window.addEventListener("online", onOnline)
    return () => {
      cancelled = true
      window.removeEventListener("online", onOnline)
    }
  }, [revision])

  const modules = useMemo(() => modulesForProfiles(profiles || []), [profiles])

  if (error) {
    return (
      <div className="module-picker">
        <div className="banner banner-error" role="alert">{error}</div>
        <button className="btn" type="button" onClick={() => setRevision(v => v + 1)}>Volver a intentar</button>
      </div>
    )
  }

  if (!profiles) {
    return <div className="empty" role="status">Cargando módulos…</div>
  }

  if (modules.length === 0) {
    return (
      <div className="banner banner-error" role="alert">
        No tienes un módulo operativo asignado. Contacta a gerencia.
      </div>
    )
  }

  return (
    <div className="module-picker">
      <header className="module-picker-hero">
        <p className="module-picker-kicker">Centro de operaciones</p>
        <h2>Selecciona tu módulo</h2>
        <p className="muted">Elige el área en la que vas a trabajar hoy.</p>
        {offline ? <p className="module-picker-offline" role="status">Sin conexión · perfiles guardados en este dispositivo.</p> : null}
      </header>

      <div className="module-picker-grid">
        {modules.map((mod) => {
          const ready = mod.status === "ready"
          const available = ready || !offline
          const contents = <>
            <div className="module-card-top">
              <span className="module-card-icon"><ModuleIcon id={mod.id} /></span>
              <span className={`module-badge module-badge-${mod.status}`}>{mod.statusLabel}</span>
            </div>
            <strong className="module-card-title">{mod.title}</strong>
            <p className="muted">{mod.description}</p>
            <span className="module-card-cta">{ready ? "Entrar al módulo" : available ? "Ver estado" : "Disponible con conexión"}{available ? <span aria-hidden="true"> →</span> : null}</span>
          </>
          return available ? (
            <AppLink key={mod.id} href={mod.href} prefetch={false}
              className={`module-card ${ready ? "module-card-ready" : "module-card-soon"}`}
              onClick={() => setActiveModuleId(ready ? mod.id : null)}>
              {contents}
            </AppLink>
          ) : (
            <div key={mod.id} className="module-card module-card-soon module-card-unavailable" aria-disabled="true">{contents}</div>
          )
        })}
      </div>
      <p className="module-picker-note">Solo aparecen los módulos asignados a tu perfil. Puedes cambiar de área desde el menú sin cerrar sesión.</p>
    </div>
  )
}
