/** Presentación de peonetas del despacho (compatible con esquemas sin peonetas_recorded). */

export function formatPeonetasDisplay(opts: {
  assistant_count?: number | null
  peonetas_recorded?: boolean | null
}): string {
  const raw = opts.assistant_count
  const count =
    raw == null || Number.isNaN(Number(raw)) ? 0 : Math.max(0, Math.floor(Number(raw)))
  if (opts.peonetas_recorded === true) return String(count)
  if (count > 0) return String(count)
  return "No informado"
}
