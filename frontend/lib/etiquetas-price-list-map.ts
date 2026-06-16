import type { PriceListRef } from "@/lib/api"

/** Empresa (nombre normalizado) → lista de precios Bsale esperada */
export const ETIQUETAS_COMPANY_PRICE_LIST: Readonly<Record<string, string>> = {
  "la quillotana spa": "Supermercado La Quillotana",
  minimarket: "Minimarket",
  "carlos romero": "Quillotana V",
}

export function normalizeEtiquetasCompanyName(name: string): string {
  return name.trim().toLowerCase()
}

export function mappedPriceListNameForCompany(companyName: string): string | null {
  const key = normalizeEtiquetasCompanyName(companyName)
  if (!key) return null
  return ETIQUETAS_COMPANY_PRICE_LIST[key] ?? null
}

export function findPriceListByName(
  lists: PriceListRef[],
  targetName: string,
): PriceListRef | null {
  const norm = targetName.trim().toLowerCase()
  return lists.find((pl) => pl.name.trim().toLowerCase() === norm) ?? null
}

export type EtiquetasPriceListResolution = {
  auto: boolean
  priceListId: string
  priceListName: string
  mappedName: string | null
  warning: string | null
}

/**
 * Resuelve lista de precios para etiquetas según empresa.
 * auto=true → mapeo aplicado; auto=false → selector manual (primera lista por defecto).
 */
export function resolveEtiquetasPriceList(
  companyName: string,
  lists: PriceListRef[],
): EtiquetasPriceListResolution {
  const mappedName = mappedPriceListNameForCompany(companyName)

  if (mappedName) {
    const match = findPriceListByName(lists, mappedName)
    if (match) {
      return {
        auto: true,
        priceListId: String(match.id),
        priceListName: match.name,
        mappedName,
        warning: null,
      }
    }
    const warning = `[etiquetas] Lista mapeada "${mappedName}" no encontrada para empresa "${companyName}"`
    console.warn(warning)
    if (lists.length > 0) {
      return {
        auto: false,
        priceListId: String(lists[0].id),
        priceListName: lists[0].name,
        mappedName,
        warning,
      }
    }
    return {
      auto: false,
      priceListId: "",
      priceListName: "",
      mappedName,
      warning,
    }
  }

  const warning = companyName
    ? `[etiquetas] Sin mapeo de lista de precios para empresa "${companyName}"`
    : null
  if (warning) console.warn(warning)

  if (lists.length > 0) {
    return {
      auto: false,
      priceListId: String(lists[0].id),
      priceListName: lists[0].name,
      mappedName: null,
      warning,
    }
  }

  return {
    auto: false,
    priceListId: "",
    priceListName: "",
    mappedName: null,
    warning,
  }
}
