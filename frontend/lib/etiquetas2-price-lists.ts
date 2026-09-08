/**
 * Configuración de listas Normal / Socio / Wow para Etiquetas V2.
 *
 * Al crear la lista definitiva SOCIOS QUILLOTANA, cambiar solo `socio`
 * por empresa (no hardcodear el nombre "Ruta Factura" en la lógica).
 */

export type PriceListRefLike = {
  id: number
  name: string
}

export type Etiquetas2ListIds = {
  /** Lista precio normal (p. ej. Supermercado) */
  normal: number | null
  /** Lista precio socio (provisional o definitiva) */
  socio: number | null
  /** Reservado — no implementar WOW en esta iteración */
  wow: number | null
}

export type Etiquetas2CompanyPriceConfig = {
  /** company_id Bsale/app si se conoce; vacío = solo match por nombre */
  companyIds: number[]
  /** Subcadenas del nombre de empresa (normalizado lowercase) */
  nameIncludes: string[]
  lists: Etiquetas2ListIds
  /** Solo UI admin — nunca se imprime en la etiqueta */
  socioProvisional: boolean
}

/**
 * Configuración estática por empresa.
 * La Quillotana SpA: normal=12 Supermercado, socio=13 Ruta Factura (PROVISIONAL).
 * Minimarket / Ancud: pendiente hasta definir IDs reales.
 */
export const PRICE_LISTS_BY_COMPANY: readonly Etiquetas2CompanyPriceConfig[] = [
  {
    companyIds: [],
    nameIncludes: ["la quillotana spa"],
    lists: {
      normal: 12,
      socio: 13,
      wow: null,
    },
    socioProvisional: true,
  },
]

export type Etiquetas2ResolvedLists = {
  configured: boolean
  pendingReason: string | null
  normal: { id: number; name: string } | null
  socio: { id: number; name: string; provisional: boolean } | null
  wow: { id: number; name: string } | null
  /** Config estática usada (si hubo match de empresa) */
  staticLists: Etiquetas2ListIds | null
}

function normalizeCompanyName(name: string): string {
  return name.trim().toLowerCase()
}

export function findStaticPriceConfigForCompany(
  companyId: number,
  companyName: string,
): Etiquetas2CompanyPriceConfig | null {
  const norm = normalizeCompanyName(companyName)
  for (const cfg of PRICE_LISTS_BY_COMPANY) {
    if (cfg.companyIds.includes(companyId)) return cfg
    if (cfg.nameIncludes.some((frag) => norm.includes(frag))) return cfg
  }
  return null
}

function findListById(
  lists: PriceListRefLike[],
  id: number | null,
): PriceListRefLike | null {
  if (id == null || !Number.isFinite(id) || id < 1) return null
  return lists.find((pl) => Number(pl.id) === Number(id)) ?? null
}

/**
 * Resuelve Normal/Socio/Wow contra las listas reales de GET /price-lists.
 * No inventa IDs: si el ID configurado no está en la respuesta → pendiente.
 */
export function resolveEtiquetas2PriceLists(
  companyId: number,
  companyName: string,
  liveLists: PriceListRefLike[],
): Etiquetas2ResolvedLists {
  const staticCfg = findStaticPriceConfigForCompany(companyId, companyName)
  if (!staticCfg) {
    return {
      configured: false,
      pendingReason: "CONFIGURACIÓN DE LISTAS PENDIENTE",
      normal: null,
      socio: null,
      wow: null,
      staticLists: null,
    }
  }

  const normalPl = findListById(liveLists, staticCfg.lists.normal)
  const socioPl = findListById(liveLists, staticCfg.lists.socio)
  const wowPl = findListById(liveLists, staticCfg.lists.wow)

  if (!normalPl || !socioPl) {
    const missing: string[] = []
    if (!normalPl && staticCfg.lists.normal != null) {
      missing.push(`normal id=${staticCfg.lists.normal}`)
    }
    if (!socioPl && staticCfg.lists.socio != null) {
      missing.push(`socio id=${staticCfg.lists.socio}`)
    }
    return {
      configured: false,
      pendingReason: `CONFIGURACIÓN DE LISTAS PENDIENTE (${missing.join(", ")} no encontrada en /price-lists)`,
      normal: normalPl
        ? { id: Number(normalPl.id), name: normalPl.name }
        : null,
      socio: socioPl
        ? {
            id: Number(socioPl.id),
            name: socioPl.name,
            provisional: staticCfg.socioProvisional,
          }
        : null,
      wow: wowPl ? { id: Number(wowPl.id), name: wowPl.name } : null,
      staticLists: staticCfg.lists,
    }
  }

  return {
    configured: true,
    pendingReason: null,
    normal: { id: Number(normalPl.id), name: normalPl.name },
    socio: {
      id: Number(socioPl.id),
      name: socioPl.name,
      provisional: staticCfg.socioProvisional,
    },
    wow: wowPl ? { id: Number(wowPl.id), name: wowPl.name } : null,
    staticLists: staticCfg.lists,
  }
}
