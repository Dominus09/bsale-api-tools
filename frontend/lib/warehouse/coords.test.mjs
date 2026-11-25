/**
 * Prueba transformación métrica ↔ pantalla (sin Next).
 * node frontend/lib/warehouse/coords.test.mjs
 */

function metricToScreen(rect, vp) {
  const { scale, offsetX, offsetY, floorWidthM } = vp
  const x = offsetX + rect.x_m * scale
  const y = offsetY + (floorWidthM - rect.y_m - rect.depth_m) * scale
  return {
    x,
    y,
    width: rect.width_m * scale,
    height: rect.depth_m * scale,
    rotation: -rect.rotation_deg,
  }
}

function screenToMetric(screen, vp) {
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

const vp = { scale: 10, offsetX: 0, offsetY: 0, floorWidthM: 13.7 }
// Portón en y=0 (sur) debe quedar abajo (y_screen alto)
const gate = { x_m: 9.375, y_m: 0, width_m: 4.35, depth_m: 0.4, rotation_deg: 15 }
const s = metricToScreen(gate, vp)
const backY = metricToScreen({ x_m: 0, y_m: 12, width_m: 1, depth_m: 1, rotation_deg: 0 }, vp)
if (!(s.y > backY.y)) {
  throw new Error("Entrada sur debe quedar más abajo en pantalla que el fondo norte")
}
if (s.rotation !== -15) throw new Error("Rotación Konva debe ser −CCW métrica")
const roundTrip = screenToMetric(s, vp)
const eps = 1e-9
for (const k of ["x_m", "y_m", "width_m", "depth_m", "rotation_deg"]) {
  if (Math.abs(roundTrip[k] - gate[k]) > eps) {
    throw new Error(`round-trip falló en ${k}: ${roundTrip[k]} vs ${gate[k]}`)
  }
}
console.log("coords.test.mjs OK")
