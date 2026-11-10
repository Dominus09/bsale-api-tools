/**
 * Identidad La Quillotana para la PWA operativa.
 * Logo oficial (Blob): mismo asset que login ERP / picking PDF.
 * No reinventar el arte; si el CDN falla, el login degrada a tipografía.
 */
export const QUILLOTANA_LOGO_GRUPO_URL =
  "https://hebbkx1anhila5yf.public.blob.vercel-storage.com/GRUPO%20QUILLOTANA%20PS-fK4da0sPbUwnmEpeEVmmumWdj977f0.png"

/** Emblema auxiliar (cuadrado) — opcional en topbar / splash. */
export const QUILLOTANA_EMBLEM_URL =
  "https://hebbkx1anhila5yf.public.blob.vercel-storage.com/Emblema%20auxiliar%20sin%20sucursal-3muphOJR8q7mpoZPwKQhJb7RbLYvdu.png"

export const BRAND = {
  name: "Quillotana Operativa",
  subtitle: "Gestión de despachos",
  /** Rojo institucional ERP (--lq-brand). */
  accent: "#c4293c",
  accentStrong: "#a42032",
} as const
