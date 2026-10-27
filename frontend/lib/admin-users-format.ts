import { CHILE_TIMEZONE } from "@/lib/analytics-v2/chile-dates"

const chileDateTime = new Intl.DateTimeFormat("es-CL", {
  timeZone: CHILE_TIMEZONE,
  dateStyle: "short",
  timeStyle: "short",
})

export function formatChileDateTime(iso: string | null | undefined): string {
  if (!iso) return "—"
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return "—"
  return chileDateTime.format(d)
}

export function roleLabel(role: string | null | undefined): string {
  const r = (role || "").trim().toLowerCase()
  if (r === "adm" || r === "admin") return "Administración"
  if (r === "gerencia") return "Gerencia"
  if (r === "operativo") return "Operativo"
  if (r === "owner") return "Propietario"
  return role || "—"
}

/** Etiqueta de listado/ficha: OWNER prevalece sobre el rol almacenado. */
export function staffRoleDisplay(
  role: string | null | undefined,
  isOwner?: boolean | null,
): string {
  if (isOwner) return "Propietario"
  return roleLabel(role)
}
