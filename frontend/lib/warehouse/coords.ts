/**
 * Transformación métrica (SoT) ↔ pantalla del editor.
 *
 * Convención backend (inmutable):
 * - Origen SW del límite
 * - +X = largo / longitudinal (metros), entrada en x≈0
 * - +Y = ancho / transversal (metros); y≈0 = sector IZ (izquierda)
 * - Rotación ° antihoraria respecto al centro del rectángulo
 *
 * Vista canónica del editor (orientación 0°):
 * - Plano longitudinal: largo vertical, ancho horizontal
 * - Entrada (x≈0) abajo · fondo (x→length) arriba
 * - IZ (y≈0) izquierda · DER (y→width) derecha
 * - Konva rota en sentido horario → rotation_konva = −rotation_deg − viewOrientation
 *
 * viewOrientationDeg (0|90|180|270) solo afecta la vista; no muta geometría persistida.
 */

export type MetricRect = {
  x_m: number
  y_m: number
  width_m: number
  depth_m: number
  rotation_deg: number
}

export type ScreenRect = {
  x: number
  y: number
  width: number
  height: number
  /** Grados Konva (horario) */
  rotation: number
}

export type ViewOrientationDeg = 0 | 90 | 180 | 270

export type ViewportTransform = {
  scale: number
  offsetX: number
  offsetY: number
  floorLengthM: number
  floorWidthM: number
  /** Solo vista; no persiste */
  viewOrientationDeg: ViewOrientationDeg
}

/** Extensión del contenido en pantalla según orientación de vista. */
export function viewContentSize(
  floorLengthM: number,
  floorWidthM: number,
  viewOrientationDeg: ViewOrientationDeg,
): { contentW: number; contentH: number } {
  if (viewOrientationDeg === 90 || viewOrientationDeg === 270) {
    return { contentW: floorLengthM, contentH: floorWidthM }
  }
  return { contentW: floorWidthM, contentH: floorLengthM }
}

/**
 * Punto métrico → plano de vista (u derecha, v hacia el fondo canónico),
 * luego orientación de vista, luego pantalla (v invertida → entrada abajo en 0°).
 */
export function metricPointToScreen(
  x_m: number,
  y_m: number,
  vp: ViewportTransform,
): { x: number; y: number } {
  const { scale, offsetX, offsetY, floorLengthM, floorWidthM, viewOrientationDeg } = vp
  // Canónico longitudinal: u = Y (transversal), v = X (longitudinal, 0 = entrada)
  let u = y_m
  let v = x_m
  const fu = floorWidthM / 2
  const fv = floorLengthM / 2
  const rad = (viewOrientationDeg * Math.PI) / 180
  const du = u - fu
  const dv = v - fv
  const ru = du * Math.cos(rad) - dv * Math.sin(rad)
  const rv = du * Math.sin(rad) + dv * Math.cos(rad)
  const { contentW, contentH } = viewContentSize(floorLengthM, floorWidthM, viewOrientationDeg)
  // Re-centrar en el bbox de contenido de esa orientación
  u = ru + contentW / 2
  v = rv + contentH / 2
  return {
    x: offsetX + u * scale,
    y: offsetY + (contentH - v) * scale,
  }
}

export function screenPointToMetric(
  sx: number,
  sy: number,
  vp: ViewportTransform,
): { x_m: number; y_m: number } {
  const { scale, offsetX, offsetY, floorLengthM, floorWidthM, viewOrientationDeg } = vp
  const { contentW, contentH } = viewContentSize(floorLengthM, floorWidthM, viewOrientationDeg)
  const u = (sx - offsetX) / scale
  const v = contentH - (sy - offsetY) / scale
  const fu = contentW / 2
  const fv = contentH / 2
  // Invertir rotación de vista
  const rad = (-viewOrientationDeg * Math.PI) / 180
  const du = u - fu
  const dv = v - fv
  const ru = du * Math.cos(rad) - dv * Math.sin(rad)
  const rv = du * Math.sin(rad) + dv * Math.cos(rad)
  // De canónico: u=Y, v=X centrado en floor
  const y_m = ru + floorWidthM / 2
  const x_m = rv + floorLengthM / 2
  return { x_m, y_m }
}

function rotateMetricPoint(
  x: number,
  y: number,
  cx: number,
  cy: number,
  deg: number,
): { x: number; y: number } {
  const rad = (deg * Math.PI) / 180
  const dx = x - cx
  const dy = y - cy
  return {
    x: cx + dx * Math.cos(rad) - dy * Math.sin(rad),
    y: cy + dx * Math.sin(rad) + dy * Math.cos(rad),
  }
}

export function metricToScreen(rect: MetricRect, vp: ViewportTransform): ScreenRect {
  const c = metricCenter(rect)
  const corners = [
    { x: rect.x_m, y: rect.y_m },
    { x: rect.x_m + rect.width_m, y: rect.y_m },
    { x: rect.x_m + rect.width_m, y: rect.y_m + rect.depth_m },
    { x: rect.x_m, y: rect.y_m + rect.depth_m },
  ].map((p) => rotateMetricPoint(p.x, p.y, c.x_m, c.y_m, rect.rotation_deg || 0))

  const scr = corners.map((p) => metricPointToScreen(p.x, p.y, vp))
  const p0 = scr[0]
  const p1 = scr[1]
  const p3 = scr[3]
  const width = Math.hypot(p1.x - p0.x, p1.y - p0.y)
  const height = Math.hypot(p3.x - p0.x, p3.y - p0.y)
  const rotation = (Math.atan2(p1.y - p0.y, p1.x - p0.x) * 180) / Math.PI
  const cx = (scr[0].x + scr[2].x) / 2
  const cy = (scr[0].y + scr[2].y) / 2
  return {
    x: cx - width / 2,
    y: cy - height / 2,
    width,
    height,
    rotation,
  }
}

export function screenToMetric(
  screen: { x: number; y: number; width: number; height: number; rotation: number },
  vp: ViewportTransform,
): MetricRect {
  const cx = screen.x + screen.width / 2
  const cy = screen.y + screen.height / 2
  const rad = (screen.rotation * Math.PI) / 180
  // Esquinas locales del rect Konva → pantalla
  const hx = screen.width / 2
  const hy = screen.height / 2
  const local = [
    { x: -hx, y: -hy },
    { x: hx, y: -hy },
    { x: hx, y: hy },
    { x: -hx, y: hy },
  ]
  const screenCorners = local.map((p) => ({
    x: cx + p.x * Math.cos(rad) - p.y * Math.sin(rad),
    y: cy + p.x * Math.sin(rad) + p.y * Math.cos(rad),
  }))
  const metricCorners = screenCorners.map((p) => screenPointToMetric(p.x, p.y, vp))
  // Reconstruir AABB métrico alineado a ejes a partir del centro y lados
  // Usamos el centro y los vectores p0→p1 (width) y p0→p3 (depth) en métrica.
  const m0 = metricCorners[0]
  const m1 = metricCorners[1]
  const m3 = metricCorners[3]
  const width_m = Math.hypot(m1.x_m - m0.x_m, m1.y_m - m0.y_m)
  const depth_m = Math.hypot(m3.x_m - m0.x_m, m3.y_m - m0.y_m)
  const edgeAngle =
    (Math.atan2(m1.y_m - m0.y_m, m1.x_m - m0.x_m) * 180) / Math.PI
  const mc = {
    x_m: (metricCorners[0].x_m + metricCorners[2].x_m) / 2,
    y_m: (metricCorners[0].y_m + metricCorners[2].y_m) / 2,
  }
  // Rect no rotado con mismo centro: esquina SW = centro - half extents en ejes locales
  // rotation_deg métrica = ángulo del borde width respecto a +X
  const rotation_deg = edgeAngle
  const halfW = width_m / 2
  const halfD = depth_m / 2
  const rr = (rotation_deg * Math.PI) / 180
  // Esquina local (-halfW, -halfD) rotada + centro
  const x_m =
    mc.x_m + (-halfW) * Math.cos(rr) - (-halfD) * Math.sin(rr)
  const y_m =
    mc.y_m + (-halfW) * Math.sin(rr) + (-halfD) * Math.cos(rr)
  return {
    x_m,
    y_m,
    width_m,
    depth_m,
    rotation_deg,
  }
}

/** Centro métrico del rectángulo (pivote de rotación). */
export function metricCenter(rect: MetricRect): { x_m: number; y_m: number } {
  return {
    x_m: rect.x_m + rect.width_m / 2,
    y_m: rect.y_m + rect.depth_m / 2,
  }
}

export function snapToGrid(valueM: number, gridM: number): number {
  if (!gridM || gridM <= 0) return valueM
  return Math.round(valueM / gridM) * gridM
}

export function fitScale(
  floorLengthM: number,
  floorWidthM: number,
  viewW: number,
  viewH: number,
  paddingPx = 48,
  viewOrientationDeg: ViewOrientationDeg = 0,
): number {
  const { contentW, contentH } = viewContentSize(floorLengthM, floorWidthM, viewOrientationDeg)
  const usableW = Math.max(1, viewW - paddingPx * 2)
  const usableH = Math.max(1, viewH - paddingPx * 2)
  return Math.min(usableW / contentW, usableH / contentH)
}

export function fitOffsets(
  floorLengthM: number,
  floorWidthM: number,
  viewW: number,
  viewH: number,
  scale: number,
  viewOrientationDeg: ViewOrientationDeg = 0,
): { offsetX: number; offsetY: number } {
  const { contentW, contentH } = viewContentSize(floorLengthM, floorWidthM, viewOrientationDeg)
  return {
    offsetX: (viewW - contentW * scale) / 2,
    offsetY: (viewH - contentH * scale) / 2,
  }
}

export function nextViewOrientation(current: ViewOrientationDeg): ViewOrientationDeg {
  const order: ViewOrientationDeg[] = [0, 90, 180, 270]
  return order[(order.indexOf(current) + 1) % order.length]
}

export function viewOrientationLabel(deg: ViewOrientationDeg): string {
  switch (deg) {
    case 0:
      return "Longitudinal (entrada abajo)"
    case 90:
      return "Vista +90°"
    case 180:
      return "Vista +180°"
    case 270:
      return "Vista +270°"
  }
}
