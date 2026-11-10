/** Enlace de grupo familiar de demostración (autorizado por administrador). */
export const FAMILY_WHATSAPP_INVITE_URL =
  "https://chat.whatsapp.com/K62IagsvHLD6ehQCknBnOI?s=cl&p=i&mlu=4&ilr=4&iam=2"

/**
 * Grupo operativo autorizado (solo reportes reales).
 * Vacío → no hay destino de grupo; se usa wa.me/?text= (selector de chat).
 * Valor inyectado en build vía NEXT_PUBLIC_OPS_WHATSAPP_INVITE_URL (Coolify build-arg).
 * Nunca hardcodear aquí el enlace operativo ni reutilizar el familiar.
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
 * Lanza si la URL base es inválida.
 */
export function buildWhatsappInviteWithText(
  inviteUrl: string,
  message: string,
): string {
  const url = new URL(inviteUrl)
  if (url.protocol !== "http:" && url.protocol !== "https:") {
    throw new TypeError("invite URL protocol no soportado")
  }
  url.searchParams.set("text", message)
  return url.toString()
}

/** True si el invite se puede parsear como URL http(s). */
export function isUsableWhatsappInviteUrl(inviteUrl: string): boolean {
  const raw = (inviteUrl || "").trim()
  if (!raw) return false
  try {
    const url = new URL(raw)
    return url.protocol === "http:" || url.protocol === "https:"
  } catch {
    return false
  }
}

export type WhatsappSendMode = "demo" | "real"

export type WhatsappSendDestination = "family_invite" | "ops_invite" | "wa_me"

/**
 * Destino seguro según modo:
 * - demo → solo grupo familiar de pruebas
 * - real → grupo operativo si hay URL usable; si falta/inválida → wa.me (nunca familiar)
 *
 * `opsInviteUrl` permite tests sin mutar env; en producción usa
 * OPERATIONAL_WHATSAPP_INVITE_URL (build-time NEXT_PUBLIC).
 */
export function resolveWhatsappSendUrl(
  message: string,
  mode: WhatsappSendMode,
  opsInviteUrl: string = OPERATIONAL_WHATSAPP_INVITE_URL,
): { url: string; destination: WhatsappSendDestination } {
  if (mode === "demo") {
    return {
      url: buildWhatsappInviteWithText(FAMILY_WHATSAPP_INVITE_URL, message),
      destination: "family_invite",
    }
  }

  const ops = (opsInviteUrl || "").trim()
  if (isUsableWhatsappInviteUrl(ops)) {
    try {
      return {
        url: buildWhatsappInviteWithText(ops, message),
        destination: "ops_invite",
      }
    } catch {
      /* cae a wa.me */
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
