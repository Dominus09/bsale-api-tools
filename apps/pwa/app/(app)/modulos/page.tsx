"use client"

import { useEffect, useMemo, useState } from "react"
import { useRouter } from "next/navigation"
import { fetchMe } from "@/lib/api"
import { getSessionUserId } from "@/lib/auth"
import { getOfflineMeta } from "@/lib/offline-store"
import {
  modulesForProfiles,
  setActiveModuleId,
  type PwaModuleDef,
} from "@/lib/modules"

export default function ModulosPage() {
  const router = useRouter()
  const [profiles, setProfiles] = useState<string[] | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    async function load() {
      try {
        const me = await fetchMe()
        if (cancelled) return
        setProfiles(me.profiles || [])
      } catch {
        const uid = getSessionUserId()
        if (uid) {
          const meta = await getOfflineMeta(uid)
          if (!cancelled && meta?.profiles?.length) {
            setProfiles(meta.profiles)
            return
          }
        }
        if (!cancelled) setError("No se pudieron cargar tus módulos.")
      }
    }
    void load()
    return () => {
      cancelled = true
    }
  }, [])

  const modules = useMemo(() => modulesForProfiles(profiles || []), [profiles])

  useEffect(() => {
    if (!profiles) return
    if (modules.length === 1 && modules[0].status === "ready") {
      setActiveModuleId(modules[0].id)
      router.replace(modules[0].href)
    }
  }, [profiles, modules, router])

  function enter(mod: PwaModuleDef) {
    if (mod.status !== "ready") {
      router.push(mod.href)
      return
    }
    setActiveModuleId(mod.id)
    router.push(mod.href)
  }

  if (error) {
    return <div className="banner banner-error">{error}</div>
  }

  if (!profiles) {
    return <div className="empty">Cargando módulos…</div>
  }

  if (modules.length === 0) {
    return (
      <div className="banner banner-error">
        Tu cuenta tiene acceso PWA pero ningún módulo operativo asignado. Contacta a gerencia.
      </div>
    )
  }

  return (
    <div className="module-picker">
      <header className="module-picker-hero">
        <p className="module-picker-kicker">Quillotana Operativa</p>
        <h2>Selecciona tu módulo de trabajo</h2>
        <p className="muted">
          Solo ves los módulos autorizados en tu perfil. Puedes cambiar después desde el menú.
        </p>
      </header>

      <div className="module-picker-grid">
        {modules.map((mod) => {
          const ready = mod.status === "ready"
          return (
            <button
              key={mod.id}
              type="button"
              className={`module-card ${ready ? "module-card-ready" : "module-card-soon"}`}
              onClick={() => enter(mod)}
            >
              <div className="module-card-top">
                <strong>{mod.title}</strong>
                <span className={`module-badge module-badge-${mod.status}`}>
                  {mod.statusLabel}
                </span>
              </div>
              <p className="muted">{mod.description}</p>
              <span className="module-card-cta">
                {ready ? "Entrar →" : "Ver estado →"}
              </span>
            </button>
          )
        })}
      </div>
    </div>
  )
}
