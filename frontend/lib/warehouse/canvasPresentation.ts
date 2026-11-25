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
  parentKey?: string
  /** Punto hacia el acceso real, proyectado en la vista actual. */
  entryPoint?: Point
  selected?: boolean
}

export type CanvasLabel = CanvasBox & {
  key: string
  text: string
  subtitle?: string
  fontSize: number
  subtitleSize: number
  paddingY: number
  compact: boolean
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
  // Flecha próxima al borde de acceso, sin cruzar los centros de los BAE.
  const arrowX = width * 0.12
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
  if (item.selected) return 0
  if (item.underStair) return 1
  if (item.type === "stair") return 2
  if (item.type === "aisle" || item.type === "office") return 3
  if (item.type === "position") return 4
  return 5
}

/** Identificador compacto y reversible dentro de una huella pequeña.
 * No modifica el código físico. La selección/zoom muestran el código completo.
 */
export function compactPositionCode(code: string): string {
  const paired = code.match(/-(\d+)-(D|A)$/i)
  if (paired) return paired[1] + paired[2].toUpperCase()
  const numbered = code.match(/-(\d+)$/)
  return numbered ? numbered[1] : code
}

function unionBoxes(boxes: CanvasBox[]): CanvasBox {
  const x = Math.min(...boxes.map(b => b.x))
  const y = Math.min(...boxes.map(b => b.y))
  const right = Math.max(...boxes.map(b => b.x + b.width))
  const bottom = Math.max(...boxes.map(b => b.y + b.height))
  return { x, y, width: right - x, height: bottom - y }
}

type Side = "left" | "right" | "above" | "below"

function preferredSide(center: Point, entry?: Point): Side {
  if (!entry) return "below"
  const dx = entry.x - center.x, dy = entry.y - center.y
  return Math.abs(dx) > Math.abs(dy) ? (dx < 0 ? "left" : "right") : (dy < 0 ? "above" : "below")
}

/**
 * Etiquetas horizontales desacopladas de la geometría.
 * En plantas pobladas, ningún rótulo de pasillo se pone sobre sus pallets.
 * Las etiquetas BAE se quedan dentro de la huella cuando hay espacio;
 * en zoom reducido se abrevian en vez de crear llamadas cruzadas.
 */
export function placeCanvasLabels(
  items: CanvasLabelInput[],
  zoom: number,
  reserved: CanvasBox[] = [],
): CanvasLabel[] {
  const unit = 1 / Math.max(0.25, zoom)
  const gap = 2 * unit
  const occupied = [...reserved]
  const labels: CanvasLabel[] = []
  const ordered = [...items].sort((a, b) => priority(a) - priority(b))
  const byKey = new Map(items.map(item => [item.key, item]))
  const byParent = new Map<string, CanvasLabelInput[]>()
  const boxes = new Map(items.map(item => [item.key, screenBounds(item.rect)]))
  for (const item of items) {
    if (item.parentKey) {
      const list = byParent.get(item.parentKey) || []
      list.push(item)
      byParent.set(item.parentKey, list)
    }
  }
  const shapes = items.filter(item => !["facility_boundary", "sector", "aisle", "circulation"].includes(item.type))
    .map(item => ({ key: item.key, box: boxes.get(item.key)! }))

  for (const item of ordered) {
    if (item.type === "facility_boundary") continue
    const bounds = boxes.get(item.key)!
    const cx = bounds.x + bounds.width / 2
    const cy = bounds.y + bounds.height / 2
    const center = { x: cx, y: cy }
    const muted = item.type === "sector" || item.type === "circulation"
    const special = Boolean(item.underStair)
    const aisle = item.type === "aisle"
    const position = item.type === "position"
    const stair = item.type === "stair"
    const children = byParent.get(item.key) || []
    const childPositions = children.filter(child => child.type === "position")
    const childBoxes = childPositions.map(child => boxes.get(child.key)!)
    const populated = childBoxes.length > 0
    const isSectorWithPositions = item.type === "sector" && populated
    const fullText = stair ? item.code + " · 2° piso" : item.code
    const compactText = position ? compactPositionCode(item.code) : stair ? item.code : item.code
    const defaultFont = position ? 10 : stair ? 11 : aisle ? 14 : 12
    const subtitleSize = (stair ? 9 : 10) * unit
    // Las cajas físicas son reserva sólo para etiquetas de contenedores y llamadas;
    // el texto de cada pallet puede aparecer dentro de su propia huella.
    const noShapes = shapes.filter(shape => shape.key !== item.key).map(shape => shape.box)
    const avoidInside = (aisle || muted) && populated ? childBoxes : []

    function metrics(text: string, subtitle: string | undefined, px: number, padding: number) {
      const fontSize = px * unit
      const width = Math.max(text.length * fontSize * 0.66, (subtitle?.length ?? 0) * subtitleSize * 0.62) + 2 * padding * unit
      const height = fontSize * 1.3 + (subtitle ? subtitleSize * 1.3 : 0) + 2 * padding * unit
      return { width, height, fontSize, paddingY: padding * unit }
    }

    function tryLabel(at: Point, text: string, subtitle: string | undefined, px: number, padding: number,
      options: { callout?: boolean; anchor?: Point; avoid?: CanvasBox[]; inside?: ScreenRect } = {},
    ): boolean {
      const m = metrics(text, subtitle, px, padding)
      const box = { x: at.x - m.width / 2, y: at.y - m.height / 2, width: m.width, height: m.height }
      if (!options.callout && !boxInsideRotatedRect(box, item.rect)) return false
      if (options.inside && !boxInsideRotatedRect(box, options.inside)) return false
      if (occupied.some(other => boxesOverlap(box, other, gap))) return false
      if (options.avoid?.some(other => boxesOverlap(box, other, unit * 0.5))) return false
      occupied.push(box)
      labels.push({
        ...box, key: item.key, text, subtitle, fontSize: m.fontSize, subtitleSize, paddingY: m.paddingY,
        muted, special, compact: text !== fullText, callout: Boolean(options.callout), anchor: options.anchor,
      })
      return true
    }

    function tryOutside(text: string, subtitle: string | undefined, px: number, padding: number,
      sides: Side[], extent: CanvasBox, preferredParent?: CanvasLabelInput,
    ): boolean {
      const m = metrics(text, subtitle, px, padding)
      const parentBox = preferredParent ? boxes.get(preferredParent.key) : undefined
      for (const side of sides) {
        const horizontal = side === "left" || side === "right"
        const anchor: Point = horizontal
          ? { x: side === "left" ? extent.x : extent.x + extent.width, y: cy }
          : { x: cx, y: side === "above" ? extent.y : extent.y + extent.height }
        const normal = side === "left" || side === "above" ? -1 : 1
        const at: Point = horizontal
          ? { x: anchor.x + normal * (gap + m.width / 2), y: cy }
          : { x: cx, y: anchor.y + normal * (gap + m.height / 2) }
        // En la escalera priorizar una llamada dentro de CIRC y centrada
        // transversalmente. No se altera la posición del elemento.
        if (parentBox && preferredParent) {
          const within = horizontal
            ? { ...at, y: parentBox.y + parentBox.height / 2 }
            : { ...at, x: parentBox.x + parentBox.width / 2 }
          if (tryLabel(within, text, subtitle, px, padding,
            { callout: true, anchor, avoid: noShapes, inside: preferredParent.rect })) return true
          if (tryLabel(at, text, subtitle, px, padding,
            { callout: true, anchor, avoid: noShapes, inside: preferredParent.rect })) return true
        } else if (tryLabel(at, text, subtitle, px, padding, { callout: true, anchor, avoid: noShapes })) return true
      }
      return false
    }

    const side = preferredSide(center, item.entryPoint)
    const sides: Side[] = [side, ...(["below", "above", "left", "right"] as Side[]).filter(s => s !== side)]

    if (position) {
      if (tryLabel(center, fullText, undefined, defaultFont, 2)) continue
      if (item.selected) {
        if (tryOutside(fullText, undefined, defaultFont, 3, sides, bounds)) continue
      }
      if (compactText !== fullText && tryLabel(center, compactText, undefined, special ? 9 : 8.5, 1.5)) continue
      if (special && !item.selected) {
        // A escala extremadamente reducida no crear llamadas individuales
        // que puedan asociarse al BAE incorrecto. La etiqueta de ESC conserva el contexto.
        continue
      }
      continue
    }

    if (stair) {
      const parent = item.parentKey ? byKey.get(item.parentKey) : undefined
      const circulation = parent?.type === "circulation" ? parent : undefined
      const hasBae = children.some(child => child.underStair)
      const subtitle = hasBae ? "BAE debajo" : undefined
      if (circulation) {
        if (tryOutside(fullText, subtitle, 10, 2, sides, bounds, circulation)) continue
        if (tryOutside(item.code, hasBae ? "2° piso · BAE" : "2° piso", 11, 2, sides, bounds, circulation)) continue
      }
      if (tryOutside(fullText, subtitle, 10, 2, sides, bounds)) continue
      if (tryOutside(fullText, undefined, 10, 2, sides, bounds)) continue
      if (tryOutside(item.code, undefined, 10, 2, sides, bounds)) continue
      tryLabel(center, item.code, undefined, 10, 1)
      continue
    }

    if (aisle && populated) {
      // La dimensión completa se muestra sólo si cabe entre las dos filas.
      if (item.dimensions && tryLabel(center, fullText, item.dimensions, 12, 2, { avoid: avoidInside })) continue
      if (tryLabel(center, fullText, undefined, 12, 1, { avoid: avoidInside })) continue
      if (tryLabel(center, fullText, undefined, 10, 1, { avoid: avoidInside })) continue
      const extent = unionBoxes([bounds, ...childBoxes])
      tryOutside(fullText, undefined, 11, 2, sides, extent)
      continue
    }

    const externalOnly = Boolean(item.sectorWithAisles) || isSectorWithPositions
    if (!externalOnly) {
      const subtitle = aisle || item.type === "office" ? item.dimensions
        : item.type === "loose_storage" ? "Sueltos" : undefined
      const candidates = [center]
      if (aisle || muted) candidates.push({ x: cx, y: bounds.y + bounds.height * 0.25 },
        { x: cx, y: bounds.y + bounds.height * 0.75 })
      let placed = false
      for (const at of candidates) {
        if (subtitle && tryLabel(at, fullText, subtitle, defaultFont, 4, { avoid: avoidInside })) {
          placed = true
          break
        }
        if (tryLabel(at, fullText, undefined, defaultFont, 2, { avoid: avoidInside })) {
          placed = true
          break
        }
      }
      if (placed) continue
    }
    if (!externalOnly && !item.selected && item.type !== "gate" && item.type !== "loose_storage") continue
    const outside: Side[] = item.sectorWithAisles ? ["right", "left", "below", "above"]
      : isSectorWithPositions ? ["left", "right", "below", "above"]
      : item.type === "gate" ? ["above", "below", "left", "right"] : sides
    tryOutside(fullText, undefined, 11, 2, outside, bounds)
  }
  return labels
}
