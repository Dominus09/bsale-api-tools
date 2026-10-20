/**
 * Design System La Quillotana — Analítica V2 (Graphite / Graphite Dark).
 *
 * Fuente única de tonos semánticos para los componentes compartidos. Los valores
 * de color viven como tokens CSS (`--color-lq-*` en app/globals.css, generados
 * desde `--lq-*`); aquí solo se componen las clases Tailwind que los consumen.
 *
 * Identidad: grafito como superficie de acento (sidebar, paneles ejecutivos),
 * rojo institucional (`lq-brand`) solo como acento de acción y navegación, y
 * peligro con su propio rojo semántico (`lq-danger`), diferenciado de marca.
 * Cada tono declara su variante `dark:`: nada se invierte automáticamente.
 */

export type AnalyticTone = "neutral" | "success" | "warning" | "danger" | "info"

/** Superficie suave por tono (barras de estado y avisos). */
export const toneSurfaces: Record<AnalyticTone, string> = {
  neutral: "border-lq-line bg-lq-surface text-lq-ink",
  success:
    "border-emerald-200/80 bg-emerald-50/50 text-emerald-950 dark:border-emerald-400/25 dark:bg-emerald-400/10 dark:text-emerald-100",
  warning:
    "border-amber-200/80 bg-amber-50/50 text-amber-950 dark:border-amber-400/25 dark:bg-amber-400/10 dark:text-amber-100",
  danger:
    "border-red-200/80 bg-red-50/50 text-red-950 dark:border-red-400/25 dark:bg-red-400/10 dark:text-red-100",
  info: "border-blue-200/80 bg-blue-50/50 text-blue-950 dark:border-blue-400/25 dark:bg-blue-400/10 dark:text-blue-100",
}

/** Etiqueta compacta por tono. */
export const toneBadges: Record<AnalyticTone, string> = {
  neutral: "border-lq-line bg-lq-canvas text-lq-body",
  success: "border-emerald-200/80 bg-emerald-50 text-emerald-800 dark:border-emerald-400/25 dark:bg-emerald-400/10 dark:text-emerald-200",
  warning: "border-amber-200/80 bg-amber-50 text-amber-800 dark:border-amber-400/25 dark:bg-amber-400/10 dark:text-amber-200",
  danger: "border-red-200/80 bg-red-50 text-red-800 dark:text-red-200 dark:border-red-400/25 dark:bg-red-400/10 dark:text-red-200",
  info: "border-blue-200/80 bg-blue-50 text-blue-800 dark:border-blue-400/25 dark:bg-blue-400/10 dark:text-blue-200",
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
