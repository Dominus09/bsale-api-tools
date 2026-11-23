/**
 * Módulos PWA Operativa — autorizados por perfiles de /auth/me (backend).
 * No usar solo UI: siempre cruzar con ``profiles`` del servidor.
 */

export type PwaModuleId = "choferes" | "vendedores" | "bodega" | "supervision"

export type PwaModuleStatus = "ready" | "preparing" | "soon"

export type PwaModuleDef = {
  id: PwaModuleId
  /** Perfil operativo requerido en auth/me.profiles */
  profile: string
  title: string
  description: string
  /** Ruta al entrar (solo si status === ready) */
  href: string
  status: PwaModuleStatus
  statusLabel: string
}

/** Catálogo UI. El acceso real lo decide el perfil en el JWT/me. */
export const PWA_MODULES: PwaModuleDef[] = [
  {
    id: "choferes",
    profile: "chofer",
    title: "Choferes",
    description: "Despachos, entregas y evidencias en ruta.",
    href: "/despachos",
    status: "ready",
    statusLabel: "Operativo",
  },
  {
    id: "vendedores",
    profile: "vendedor",
    title: "Vendedores",
    description: "Ruta comercial, visitas y jornada del día.",
    href: "/vendedores",
    status: "preparing",
    statusLabel: "En preparación",
  },
  {
    id: "bodega",
    profile: "bodega",
    title: "Bodega",
    description: "Operación de bodega y preparación de carga.",
    href: "/bodega",
    status: "soon",
    statusLabel: "Próximamente",
  },
  {
    id: "supervision",
    profile: "supervisor",
    title: "Supervisión",
    description: "Seguimiento operativo de equipos en terreno.",
    href: "/supervision",
    status: "soon",
    statusLabel: "Próximamente",
  },
]

const ACTIVE_KEY = "pwa_active_module"

export function modulesForProfiles(profiles: string[] | null | undefined): PwaModuleDef[] {
  const set = new Set((profiles || []).map((p) => String(p || "").trim().toLowerCase()))
  return PWA_MODULES.filter((m) => set.has(m.profile))
}

export function hasAnyPwaModule(profiles: string[] | null | undefined): boolean {
  return modulesForProfiles(profiles).length > 0
}

export function getActiveModuleId(): PwaModuleId | null {
  if (typeof window === "undefined") return null
  try {
    const raw = sessionStorage.getItem(ACTIVE_KEY)
    if (raw === "choferes" || raw === "vendedores" || raw === "bodega" || raw === "supervision") {
      return raw
    }
  } catch {
    /* private mode */
  }
  return null
}

export function setActiveModuleId(id: PwaModuleId | null) {
  if (typeof window === "undefined") return
  try {
    if (!id) sessionStorage.removeItem(ACTIVE_KEY)
    else sessionStorage.setItem(ACTIVE_KEY, id)
  } catch {
    /* ignore */
  }
}

export function moduleLabel(id: PwaModuleId | null | undefined): string {
  if (!id) return "Operativa"
  return PWA_MODULES.find((m) => m.id === id)?.title || "Operativa"
}

/**
 * Destino post-login.
 * Chofer único → /despachos (sin fricción).
 * Varios módulos o sin chofer ready → selector.
 */
export function resolvePostLoginPath(profiles: string[]): string {
  const mods = modulesForProfiles(profiles)
  if (mods.length === 0) return "/login?denied=profile"
  const ready = mods.filter((m) => m.status === "ready")
  if (ready.length === 1 && ready[0].id === "choferes") {
    setActiveModuleId("choferes")
    return "/despachos"
  }
  if (mods.length === 1 && mods[0].status === "ready") {
    setActiveModuleId(mods[0].id)
    return mods[0].href
  }
  return "/modulos"
}
