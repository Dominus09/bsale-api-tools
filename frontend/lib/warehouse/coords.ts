/**
 * Transformación métrica (SoT) ↔ pantalla del editor.
 *
 * Convención backend:
 * - Origen SW del límite
 * - +X = largo (longitudinal)
 * - +Y = ancho (transversal); y≈0 = entrada sur
 * - Rotación ° antihoraria respecto al centro del rectángulo
 *
 * Pantalla:
 * - Entrada (sur) abajo → se invierte el eje Y en canvas
 * - Konva rota en sentido horario → rotation_konva = −rotation_deg
 * No se mutan las coordenadas persistidas para lograr la orientación.
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

export type ViewportTransform = {
  scale: number
  offsetX: number
  offsetY: number
  /** Ancho métrico del piso (eje Y), usado para invertir */
  floorWidthM: number
}

export function metricToScreen(rect: MetricRect, vp: ViewportTransform): ScreenRect {
  const { scale, offsetX, offsetY, floorWidthM } = vp
  const x = offsetX + rect.x_m * scale
  // Norte (y_m + depth) arriba en pantalla → y_screen menor
  const y = offsetY + (floorWidthM - rect.y_m - rect.depth_m) * scale
  return {
    x,
    y,
    width: rect.width_m * scale,
    height: rect.depth_m * scale,
    rotation: -rect.rotation_deg,
  }
}

export function screenToMetric(
  screen: { x: number; y: number; width: number; height: number; rotation: number },
  vp: ViewportTransform,
): MetricRect {
  const { scale, offsetX, offsetY, floorWidthM } = vp
  const width_m = screen.width / scale
  const depth_m = screen.height / scale
  const x_m = (screen.x - offsetX) / scale
  const y_m = floorWidthM - (screen.y - offsetY) / scale - depth_m
  return {
    x_m,
    y_m,
    width_m,
    depth_m,
    rotation_deg: -screen.rotation,
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
): number {
  const usableW = Math.max(1, viewW - paddingPx * 2)
  const usableH = Math.max(1, viewH - paddingPx * 2)
  return Math.min(usableW / floorLengthM, usableH / floorWidthM)
}

export function fitOffsets(
  floorLengthM: number,
  floorWidthM: number,
  viewW: number,
  viewH: number,
  scale: number,
): { offsetX: number; offsetY: number } {
  const contentW = floorLengthM * scale
  const contentH = floorWidthM * scale
  return {
    offsetX: (viewW - contentW) / 2,
    offsetY: (viewH - contentH) / 2,
  }
}
