/**
 * Prueba transformación métrica ↔ pantalla (vista longitudinal).
 * node frontend/lib/warehouse/coords.test.mjs
 */

function viewContentSize(floorLengthM, floorWidthM, viewOrientationDeg) {
  if (viewOrientationDeg === 90 || viewOrientationDeg === 270) {
    return { contentW: floorLengthM, contentH: floorWidthM }
  }
  return { contentW: floorWidthM, contentH: floorLengthM }
}

function metricPointToScreen(x_m, y_m, vp) {
  const { scale, offsetX, offsetY, floorLengthM, floorWidthM, viewOrientationDeg } = vp
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
  u = ru + contentW / 2
  v = rv + contentH / 2
  return {
    x: offsetX + u * scale,
    y: offsetY + (contentH - v) * scale,
  }
}

function screenPointToMetric(sx, sy, vp) {
  const { scale, offsetX, offsetY, floorLengthM, floorWidthM, viewOrientationDeg } = vp
  const { contentW, contentH } = viewContentSize(floorLengthM, floorWidthM, viewOrientationDeg)
  const u = (sx - offsetX) / scale
  const v = contentH - (sy - offsetY) / scale
  const fu = contentW / 2
  const fv = contentH / 2
  const rad = (-viewOrientationDeg * Math.PI) / 180
  const du = u - fu
  const dv = v - fv
  const ru = du * Math.cos(rad) - dv * Math.sin(rad)
  const rv = du * Math.sin(rad) + dv * Math.cos(rad)
  return { x_m: rv + floorLengthM / 2, y_m: ru + floorWidthM / 2 }
}

function metricCenter(rect) {
  return { x_m: rect.x_m + rect.width_m / 2, y_m: rect.y_m + rect.depth_m / 2 }
}

function rotateMetricPoint(x, y, cx, cy, deg) {
  const rad = (deg * Math.PI) / 180
  const dx = x - cx
  const dy = y - cy
  return {
    x: cx + dx * Math.cos(rad) - dy * Math.sin(rad),
    y: cy + dx * Math.sin(rad) + dy * Math.cos(rad),
  }
}

function metricToScreen(rect, vp) {
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
  return { x: cx - width / 2, y: cy - height / 2, width, height, rotation }
}

function screenToMetric(screen, vp) {
  const cx = screen.x + screen.width / 2
  const cy = screen.y + screen.height / 2
  const rad = (screen.rotation * Math.PI) / 180
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
  const m0 = metricCorners[0]
  const m1 = metricCorners[1]
  const m3 = metricCorners[3]
  const width_m = Math.hypot(m1.x_m - m0.x_m, m1.y_m - m0.y_m)
  const depth_m = Math.hypot(m3.x_m - m0.x_m, m3.y_m - m0.y_m)
  const edgeAngle = (Math.atan2(m1.y_m - m0.y_m, m1.x_m - m0.x_m) * 180) / Math.PI
  const mc = {
    x_m: (metricCorners[0].x_m + metricCorners[2].x_m) / 2,
    y_m: (metricCorners[0].y_m + metricCorners[2].y_m) / 2,
  }
  const rr = (edgeAngle * Math.PI) / 180
  const halfW = width_m / 2
  const halfD = depth_m / 2
  return {
    x_m: mc.x_m + -halfW * Math.cos(rr) - -halfD * Math.sin(rr),
    y_m: mc.y_m + -halfW * Math.sin(rr) + -halfD * Math.cos(rr),
    width_m,
    depth_m,
    rotation_deg: edgeAngle,
  }
}

const L = 23.1
const W = 13.7
const vp0 = {
  scale: 10,
  offsetX: 0,
  offsetY: 0,
  floorLengthM: L,
  floorWidthM: W,
  viewOrientationDeg: 0,
}

// 1) Entrada (x≈0) más abajo en pantalla que el fondo (x alto)
const entrance = metricPointToScreen(0.2, W / 2, vp0)
const back = metricPointToScreen(L - 0.2, W / 2, vp0)
if (!(entrance.y > back.y)) {
  throw new Error(`Entrada debe quedar abajo: y_ent=${entrance.y} y_fondo=${back.y}`)
}

// 2) IZ (y bajo) a la izquierda de DER (y alto)
const iz = metricPointToScreen(L / 2, 1, vp0)
const der = metricPointToScreen(L / 2, W - 1, vp0)
if (!(iz.x < der.x)) {
  throw new Error(`IZ debe quedar a la izquierda: iz.x=${iz.x} der.x=${der.x}`)
}

// 3) Contenido longitudinal: ancho pantalla = W, alto = L
const { contentW, contentH } = viewContentSize(L, W, 0)
if (Math.abs(contentW - W) > 1e-9 || Math.abs(contentH - L) > 1e-9) {
  throw new Error(`Vista 0° debe ser ${W}×${L}, got ${contentW}×${contentH}`)
}

// 4) P1 (x bajo) más abajo que P7 (x alto)
const p1 = metricPointToScreen(2, 10, vp0)
const p7 = metricPointToScreen(20, 10, vp0)
if (!(p1.y > p7.y)) {
  throw new Error("P1 debe quedar más abajo que P7 en vista longitudinal")
}

// 5) Portón en muro corto de entrada (x≈0, span en Y) — centro abajo
const gateRect = { x_m: 0, y_m: 4.675, width_m: 0.4, depth_m: 4.35, rotation_deg: 0 }
const gateScr = metricToScreen(gateRect, vp0)
if (!(gateScr.y + gateScr.height / 2 > contentH * vp0.scale * 0.7)) {
  throw new Error("Portón de entrada debe renderizar cerca del borde inferior")
}

// 6) Round-trip punto
for (const pt of [
  { x_m: 1.5, y_m: 2.0 },
  { x_m: 11.5, y_m: 7.0 },
  { x_m: 21.0, y_m: 12.0 },
]) {
  const s = metricPointToScreen(pt.x_m, pt.y_m, vp0)
  const backPt = screenPointToMetric(s.x, s.y, vp0)
  if (Math.abs(backPt.x_m - pt.x_m) > 1e-9 || Math.abs(backPt.y_m - pt.y_m) > 1e-9) {
    throw new Error(`round-trip punto falló: ${JSON.stringify(pt)} → ${JSON.stringify(backPt)}`)
  }
}

// 7) Round-trip rect sin rotación
const rect = { x_m: 1.5, y_m: 7.35, width_m: 2.8, depth_m: 6.0, rotation_deg: 0 }
const sRect = metricToScreen(rect, vp0)
const round = screenToMetric(sRect, vp0)
const eps = 1e-6
for (const k of ["x_m", "y_m", "width_m", "depth_m"]) {
  if (Math.abs(round[k] - rect[k]) > eps) {
    throw new Error(`round-trip rect ${k}: ${round[k]} vs ${rect[k]}`)
  }
}
if (Math.abs(((round.rotation_deg % 360) + 360) % 360) > 0.05) {
  throw new Error(`rotation round-trip: ${round.rotation_deg}`)
}

// 8) Orientación 90° no muta métrica al round-trip de punto
const vp90 = { ...vp0, viewOrientationDeg: 90 }
const s90 = metricPointToScreen(5, 3, vp90)
const m90 = screenPointToMetric(s90.x, s90.y, vp90)
if (Math.abs(m90.x_m - 5) > 1e-9 || Math.abs(m90.y_m - 3) > 1e-9) {
  throw new Error(`vista 90° mutó métrica: ${JSON.stringify(m90)}`)
}

console.log("coords.test.mjs OK — longitudinal + viewOrientation")
