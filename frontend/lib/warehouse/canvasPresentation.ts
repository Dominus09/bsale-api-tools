import type { ScreenRect } from "./coords"
import type { WarehouseElementType } from "./types"

type Point = { x: number; y: number }
export type CanvasBox = { x: number; y: number; width: number; height: number }

export type CanvasLabelInput = {
  key: string
  code: string
  type: WarehouseElementType
  rect: ScreenRect
  dimensions?: string
  underStair?: boolean
  sectorWithAisles?: boolean
  selected?: boolean
}

export type CanvasLabel = CanvasBox & {
  key: string
  text: string
  subtitle?: string
  fontSize: number
  subtitleSize: number
  muted: boolean
  special: boolean
  callout: boolean
  anchor?: Point
}

export function screenBounds(rect: ScreenRect): CanvasBox {
  const rad = rect.rotation * Math.PI / 180
  const width = Math.abs(Math.cos(rad)) * rect.width + Math.abs(Math.sin(rad)) * rect.height
  const height = Math.abs(Math.sin(rad)) * rect.width + Math.abs(Math.cos(rad)) * rect.height
  return {
    x: rect.x + (rect.width - width) / 2,
    y: rect.y + (rect.height - height) / 2,
    width,
    height,
  }
}

/** Segmentos diagonales recortados matemáticamente al rectángulo local. */
export function clippedHatchLines(width: number, height: number, step = 7): number[][] {
  const lines: number[][] = []
  if (width <= 0 || height <= 0 || step <= 0) return lines
  for (let x = -height; x < width; x += step) {
    const y0 = Math.max(0, -x)
    const y1 = Math.min(height, width - x)
    if (y1 > y0) lines.push([x + y0, y0, x + y1, y1])
  }
  return lines
}

/**
 * Marco local Konva: eje horizontal = +X métrico; eje vertical = +Y.
 * La subida observada de ESC es +Y. Los peldaños recorren X y
 * la flecha recorre Y. El grupo aplica la rotación visual.
 */
export function stairDecoration(width: number, height: number): {
  steps: number[][]
  arrow: number[] | null
} {
  const steps: number[][] = []
  if (width < 5 || height < 8) return { steps, arrow: null }
  const inset = Math.min(3, width / 8, height / 8)
  const count = Math.max(4, Math.min(12, Math.floor(height / 9)))
  for (let i = 1; i < count; i++) {
    const y = height * i / count
    steps.push([inset, y, width - inset, y])
  }
  const arrowX = width * 0.16
  return {
    steps,
    arrow: height > 20 ? [arrowX, inset + 2, arrowX, height - inset - 2] : null,
  }
}

export function boxesOverlap(a: CanvasBox, b: CanvasBox, gap = 0): boolean {
  return a.x < b.x + b.width + gap && a.x + a.width + gap > b.x &&
    a.y < b.y + b.height + gap && a.y + a.height + gap > b.y
}

function boxInsideRotatedRect(box: CanvasBox, rect: ScreenRect): boolean {
  const cx = rect.x + rect.width / 2
  const cy = rect.y + rect.height / 2
  const rad = -rect.rotation * Math.PI / 180
  return [box.x, box.x + box.width].every((x) =>
    [box.y, box.y + box.height].every((y) => {
      const dx = x - cx
      const dy = y - cy
      const lx = dx * Math.cos(rad) - dy * Math.sin(rad)
      const ly = dx * Math.sin(rad) + dy * Math.cos(rad)
      return Math.abs(lx) <= rect.width / 2 + 1e-6 &&
        Math.abs(ly) <= rect.height / 2 + 1e-6
    }),
  )
}

function priority(item: CanvasLabelInput): number {
  if (item.underStair) return 0
  if (item.selected) return 1
  if (item.type === "position") return 2
  if (item.type === "stair" || item.type === "loose_storage") return 3
  if (item.type === "aisle" || item.type === "office") return 4
  return 5
}

/**
 * Etiquetas independientes de la rotación de la figura.
 * Tamaño en pantalla estable al hacer zoom. Si no caben, se omiten los
 * detalles secundarios; las estructuras especiales conservan una llamada.
 * Los rectángulos originales nunca se modifican.
 */
export function placeCanvasLabels(
  items: CanvasLabelInput[],
  zoom: number,
  reserved: CanvasBox[] = [],
): CanvasLabel[] {
  const unit = 1 / Math.max(0.25, zoom)
  const gap = 3 * unit
  const occupied = [...reserved]
  const labels: CanvasLabel[] = []
  const ordered = [...items].sort((a, b) => priority(a) - priority(b))

  for (const item of ordered) {
    if (item.type === "facility_boundary") continue
    const bounds = screenBounds(item.rect)
    const cx = bounds.x + bounds.width / 2
    const cy = bounds.y + bounds.height / 2
    const muted = item.type === "sector" || item.type === "circulation"
    const special = Boolean(item.underStair)
    const aisle = item.type === "aisle" || item.type === "office"
    const text = item.type === "stair" ? "ESC · 2° piso" : item.code
    const fontSize = (aisle ? 14 : special ? 10 : item.type === "position" ? 10 : 12) * unit
    const subtitleSize = 10 * unit
    const candidates: Array<{ x: number; y: number }> = [{ x: cx, y: cy }]
    if (aisle || muted) {
      candidates.push({ x: cx, y: bounds.y + bounds.height * 0.25 },
        { x: cx, y: bounds.y + bounds.height * 0.75 })
    }

    function tryLabel(
      center: Point,
      subtitle: string | undefined,
      callout: boolean,
    ): boolean {
      const width = Math.max(text.length * fontSize * 0.68, (subtitle?.length ?? 0) * subtitleSize * 0.62) + 8 * unit
      const height = fontSize * 1.3 + (subtitle ? subtitleSize * 1.3 : 0) + 4 * unit
      const box = { x: center.x - width / 2, y: center.y - height / 2, width, height }
      if (!callout && !boxInsideRotatedRect(box, item.rect)) return false
      if (occupied.some((other) => boxesOverlap(box, other, gap))) return false
      occupied.push(box)
      labels.push({
        ...box, key: item.key, text, subtitle, fontSize, subtitleSize,
        muted, special, callout,
        anchor: callout ? { x: cx, y: cy } : undefined,
      })
      return true
    }

    const externalOnly = item.type === "stair" || Boolean(item.sectorWithAisles)
    let placed = false
    if (!externalOnly) {
      const subtitle = aisle ? item.dimensions : item.type === "loose_storage" ? "Sueltos" : undefined
      for (const candidate of candidates) {
        if (subtitle && tryLabel(candidate, subtitle, false)) {
          placed = true
          break
        }
        if (tryLabel(candidate, undefined, false)) {
          placed = true
          break
        }
      }
    }
    if (placed) continue

    // Las posiciones normales pequeñas no deben llenar el plano de llamadas.
    // ESC, BAE, portón y elementos seleccionados conservan su código legible.
    if (!externalOnly && !special && !item.selected &&
      item.type !== "gate" && item.type !== "loose_storage") continue

    const w = text.length * fontSize * 0.68 + 8 * unit
    const h = fontSize * 1.3 + 4 * unit
    const below = { x: cx, y: bounds.y + bounds.height + h / 2 + gap }
    const above = { x: cx, y: bounds.y - h / 2 - gap }
    const left = { x: bounds.x - w / 2 - gap, y: cy }
    const right = { x: bounds.x + bounds.width + w / 2 + gap, y: cy }
    const outside = item.sectorWithAisles ? [right, left, below, above]
      : item.type === "gate" ? [above, below, left, right]
      : [below, above, left, right]
    for (const center of outside) {
      if (tryLabel(center, undefined, true)) break
    }
  }
  return labels
}
