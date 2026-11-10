/** Enlace de grupo familiar de demostración (autorizado por administrador). */
export const FAMILY_WHATSAPP_INVITE_URL =
  "https://chat.whatsapp.com/K62IagsvHLD6ehQCknBnOI?s=cl&p=i&mlu=4&ilr=4&iam=2"

/** URL oficial wa.me con texto precargado (el usuario elige el chat). */
export function buildWhatsappTextUrl(text: string): string {
  return `https://wa.me/?text=${encodeURIComponent(text)}`
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
 * Copia el reporte y abre el grupo familiar.
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
