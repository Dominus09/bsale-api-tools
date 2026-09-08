export type SocioPriceStatus =
  | "OK"
  | "SIN_PRECIO_SOCIO"
  | "REVISAR_PRECIO_SOCIO"

export function socioPriceStatus(
  normalPrice: number | null,
  socioPrice: number | null,
): SocioPriceStatus {
  if (socioPrice == null || !Number.isFinite(socioPrice)) {
    return "SIN_PRECIO_SOCIO"
  }
  if (
    normalPrice != null &&
    Number.isFinite(normalPrice) &&
    socioPrice >= normalPrice
  ) {
    return "REVISAR_PRECIO_SOCIO"
  }
  return "OK"
}

export function statusLabel(status: SocioPriceStatus): string {
  switch (status) {
    case "OK":
      return "OK"
    case "SIN_PRECIO_SOCIO":
      return "Sin precio Socio"
    case "REVISAR_PRECIO_SOCIO":
      return "Revisar precio Socio"
  }
}
