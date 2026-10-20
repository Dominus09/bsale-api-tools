/**
 * Design System La Quillotana — Analítica V2.
 *
 * Fuente única de tonos semánticos para los componentes compartidos. Los valores
 * de color viven como tokens CSS (`--color-lq-*` en app/globals.css, generados
 * desde `--lq-*`); aquí solo se componen las clases Tailwind que los consumen.
 *
 * Identidad: luminosa y cálida. El rojo institucional (`lq-brand`) es acento de
 * acción y navegación activa, no color de alerta: el peligro usa `lq-danger`.
 */

export type AnalyticTone = "neutral" | "success" | "warning" | "danger" | "info"

/** Superficie suave por tono (barras de estado y avisos). */
export const toneSurfaces: Record<AnalyticTone, string> = {
  neutral: "border-lq-line bg-lq-surface text-lq-ink",
  success: "border-emerald-200/80 bg-emerald-50/50 text-emerald-950",
  warning: "border-amber-200/80 bg-amber-50/50 text-amber-950",
  danger: "border-red-200/80 bg-red-50/50 text-red-950",
  info: "border-blue-200/80 bg-blue-50/50 text-blue-950",
}

/** Etiqueta compacta por tono. */
export const toneBadges: Record<AnalyticTone, string> = {
  neutral: "border-lq-line bg-lq-canvas text-lq-body",
  success: "border-emerald-200/80 bg-emerald-50 text-emerald-800",
  warning: "border-amber-200/80 bg-amber-50 text-amber-800",
  danger: "border-red-200/80 bg-red-50 text-red-800",
  info: "border-blue-200/80 bg-blue-50 text-blue-800",
}

/** Punto de estado (indicadores y filas de atención). */
export const toneDots: Record<AnalyticTone, string> = {
  neutral: "bg-lq-faint",
  success: "bg-lq-success",
  warning: "bg-lq-warning",
  danger: "bg-lq-danger",
  info: "bg-lq-info",
}

/** Color de texto protagonista por tono (cifras de atención). */
export const toneText: Record<AnalyticTone, string> = {
  neutral: "text-lq-muted",
  success: "text-lq-success",
  warning: "text-lq-warning",
  danger: "text-lq-danger",
  info: "text-lq-info",
}
