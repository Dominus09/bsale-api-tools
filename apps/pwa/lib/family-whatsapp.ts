/** Enlace de grupo familiar de demostración (autorizado por administrador). */
export const FAMILY_WHATSAPP_INVITE_URL =
  "https://chat.whatsapp.com/K62IagsvHLD6ehQCknBnOI?s=cl&p=i&mlu=4&ilr=4&iam=2"

/**
 * Grupo operativo autorizado (solo reportes reales).
 * Vacío → no hay destino de grupo; se usa wa.me/?text= (selector de chat).
 * Nunca reutilizar el enlace familiar aquí.
 */
export const OPERATIONAL_WHATSAPP_INVITE_URL = (
  typeof process !== "undefined"
    ? (process.env.NEXT_PUBLIC_OPS_WHATSAPP_INVITE_URL || "").trim()
    : ""
)

/** URL oficial wa.me con texto precargado (el usuario elige el chat). */
export function buildWhatsappTextUrl(text: string): string {
  return `https://wa.me/?text=${encodeURIComponent(text)}`
}

/**
 * Conserva query params existentes del invite y fija/reemplaza `text`
 * sin duplicarlo (URLSearchParams.set).
 */
export function buildWhatsappInviteWithText(
  inviteUrl: string,
  message: string,
): string {
  const url = new URL(inviteUrl)
  url.searchParams.set("text", message)
  return url.toString()
}

export type WhatsappSendMode = "demo" | "real"

/**
 * Destino seguro según modo:
 * - demo → solo grupo familiar de pruebas
 * - real → grupo operativo si está configurado; si no, wa.me (nunca familiar)
 */
export function resolveWhatsappSendUrl(
  message: string,
  mode: WhatsappSendMode,
): { url: string; destination: "family_invite" | "ops_invite" | "wa_me" } {
  if (mode === "demo") {
    return {
      url: buildWhatsappInviteWithText(FAMILY_WHATSAPP_INVITE_URL, message),
      destination: "family_invite",
    }
  }
  if (OPERATIONAL_WHATSAPP_INVITE_URL) {
    return {
      url: buildWhatsappInviteWithText(OPERATIONAL_WHATSAPP_INVITE_URL, message),
      destination: "ops_invite",
    }
  }
  return {
    url: buildWhatsappTextUrl(message),
    destination: "wa_me",
  }
}

/** Abre el destino resuelto con gesto del usuario (sin envío automático). */
export function openWhatsappReport(message: string, mode: WhatsappSendMode): void {
  const { url } = resolveWhatsappSendUrl(message, mode)
  window.location.href = url
}

export function openWhatsappWithText(text: string): void {
  window.location.href = buildWhatsappTextUrl(text)
}

export async function copyTextSafe(text: string): Promise<boolean> {
  try {
    if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(text)
      return true
    }
  } catch {
    /* fallback */
  }
  try {
    const ta = document.createElement("textarea")
    ta.value = text
    ta.setAttribute("readonly", "")
    ta.style.position = "fixed"
    ta.style.left = "-9999px"
    document.body.appendChild(ta)
    ta.select()
    const ok = document.execCommand("copy")
    document.body.removeChild(ta)
    return ok
  } catch {
    return false
  }
}

export function openFamilyGroup(): void {
  // Navegación directa: iOS abre WhatsApp hacia el grupo de invitación.
  window.location.href = FAMILY_WHATSAPP_INVITE_URL
}

/**
 * Copia el reporte y abre el grupo familiar (recuperación interna / demo).
 * No envía mensajes automáticamente.
 */
export async function copyReportAndOpenFamily(reportText: string): Promise<{
  copied: boolean
  opened: boolean
}> {
  const copied = await copyTextSafe(reportText)
  try {
    openFamilyGroup()
    return { copied, opened: true }
  } catch {
    return { copied, opened: false }
  }
}
