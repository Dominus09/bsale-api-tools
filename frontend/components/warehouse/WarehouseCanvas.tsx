"use client"

import { useEffect, useMemo, useRef, useState } from "react"
import { Stage, Layer, Rect, Text, Line, Transformer, Group } from "react-konva"
import type Konva from "konva"
import type { WarehouseElement, EditorTool } from "@/lib/warehouse/types"
import {
  fitOffsets,
  fitScale,
  metricPointToScreen,
  metricToScreen,
  nextViewOrientation,
  screenPointToMetric,
  screenToMetric,
  snapToGrid,
  viewContentSize,
  viewOrientationLabel,
  type ViewOrientationDeg,
  type ViewportTransform,
} from "@/lib/warehouse/coords"
import {
  LAYOUT_COLORS,
  colorsForType,
  labelPriority,
  shouldShowDimensions,
} from "@/lib/warehouse/style"

type Props = {
  elements: WarehouseElement[]
  floorLengthM: number
  floorWidthM: number
  selectedKey: string | null
  tool: EditorTool
  gridM: number
  conflictKeys?: Set<string>
  onSelect: (key: string | null) => void
  onChangeElement: (key: string, patch: Partial<WarehouseElement["geometry"]>) => void
  onCreateAt: (x_m: number, y_m: number) => void
  fitRequest?: number
  viewOrientationDeg?: ViewOrientationDeg
  onViewOrientationChange?: (deg: ViewOrientationDeg) => void
}

export function WarehouseCanvas({
  elements,
  floorLengthM,
  floorWidthM,
  selectedKey,
  tool,
  gridM,
  conflictKeys,
  onSelect,
  onChangeElement,
  onCreateAt,
  fitRequest = 0,
  viewOrientationDeg = 0,
  onViewOrientationChange,
}: Props) {
  const wrapRef = useRef<HTMLDivElement>(null)
  const stageRef = useRef<Konva.Stage>(null)
  const trRef = useRef<Konva.Transformer>(null)
  const [size, setSize] = useState({ w: 800, h: 560 })
  const [scale, setScale] = useState(1)
  const [stagePos, setStagePos] = useState({ x: 0, y: 0 })
  const [base, setBase] = useState({ scale: 20, offsetX: 40, offsetY: 40 })
  const measureRef = useRef<{ x: number; y: number } | null>(null)
  const [measureLine, setMeasureLine] = useState<{
    x1: number
    y1: number
    x2: number
    y2: number
    distM: number
  } | null>(null)

  useEffect(() => {
    const el = wrapRef.current
    if (!el) return
    const ro = new ResizeObserver(() => {
      setSize({ w: el.clientWidth, h: el.clientHeight })
    })
    ro.observe(el)
    setSize({ w: el.clientWidth, h: el.clientHeight })
    return () => ro.disconnect()
  }, [])

  const recomputeFit = () => {
    const s = fitScale(floorLengthM, floorWidthM, size.w, size.h, 48, viewOrientationDeg)
    const off = fitOffsets(floorLengthM, floorWidthM, size.w, size.h, s, viewOrientationDeg)
    setBase({ scale: s, offsetX: off.offsetX, offsetY: off.offsetY })
    setScale(1)
    setStagePos({ x: 0, y: 0 })
  }

  useEffect(() => {
    recomputeFit()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [floorLengthM, floorWidthM, size.w, size.h, fitRequest, viewOrientationDeg])

  const vp: ViewportTransform = useMemo(
    () => ({
      scale: base.scale,
      offsetX: base.offsetX,
      offsetY: base.offsetY,
      floorLengthM,
      floorWidthM,
      viewOrientationDeg,
    }),
    [base, floorLengthM, floorWidthM, viewOrientationDeg],
  )

  const content = viewContentSize(floorLengthM, floorWidthM, viewOrientationDeg)

  useEffect(() => {
    const tr = trRef.current
    const stage = stageRef.current
    if (!tr || !stage) return
    const canTransform = tool === "select" || tool === "move" || tool === "resize" || tool === "rotate"
    if (!selectedKey || !canTransform) {
      tr.nodes([])
      tr.getLayer()?.batchDraw()
      return
    }
    const node = stage.findOne(`#el-${selectedKey}`)
    if (node) {
      tr.nodes([node])
      tr.getLayer()?.batchDraw()
    }
  }, [selectedKey, elements, tool, viewOrientationDeg])

  const gridLines = useMemo(() => {
    const lines: Array<{ points: number[] }> = []
    if (!gridM || gridM <= 0) return lines
    for (let x = 0; x <= floorLengthM + 1e-6; x += gridM) {
      const a = metricPointToScreen(x, 0, vp)
      const b = metricPointToScreen(x, floorWidthM, vp)
      lines.push({ points: [a.x, a.y, b.x, b.y] })
    }
    for (let y = 0; y <= floorWidthM + 1e-6; y += gridM) {
      const a = metricPointToScreen(0, y, vp)
      const b = metricPointToScreen(floorLengthM, y, vp)
      lines.push({ points: [a.x, a.y, b.x, b.y] })
    }
    return lines
  }, [gridM, floorLengthM, floorWidthM, vp])

  const sorted = useMemo(() => {
    return [...elements]
      .filter((e) => !e.pendingRetire && !e.pendingRemoveGeometry)
      .sort(
        (a, b) =>
          labelPriority(a.physical.element_type) - labelPriority(b.physical.element_type),
      )
  }, [elements])

  const markers = useMemo(() => {
    if (viewOrientationDeg !== 0) return null
    const front = metricPointToScreen(0.15, floorWidthM / 2, vp)
    const back = metricPointToScreen(floorLengthM - 0.15, floorWidthM / 2, vp)
    const left = metricPointToScreen(floorLengthM / 2, 0.2, vp)
    const right = metricPointToScreen(floorLengthM / 2, floorWidthM - 0.2, vp)
    return { front, back, left, right }
  }, [vp, floorLengthM, floorWidthM, viewOrientationDeg])

  const handleWheel = (e: Konva.KonvaEventObject<WheelEvent>) => {
    e.evt.preventDefault()
    const stage = stageRef.current
    if (!stage) return
    const oldScale = scale
    const pointer = stage.getPointerPosition()
    if (!pointer) return
    const scaleBy = 1.08
    const newScale = e.evt.deltaY > 0 ? oldScale / scaleBy : oldScale * scaleBy
    const clamped = Math.min(8, Math.max(0.25, newScale))
    const mousePointTo = {
      x: (pointer.x - stagePos.x) / oldScale,
      y: (pointer.y - stagePos.y) / oldScale,
    }
    setScale(clamped)
    setStagePos({
      x: pointer.x - mousePointTo.x * clamped,
      y: pointer.y - mousePointTo.y * clamped,
    })
  }

  const localPointer = () => {
    const stage = stageRef.current
    const p = stage?.getPointerPosition()
    if (!p || !stage) return null
    return {
      x: (p.x - stage.x()) / stage.scaleX(),
      y: (p.y - stage.y()) / stage.scaleY(),
    }
  }

  return (
    <div ref={wrapRef} className="relative h-full w-full overflow-hidden bg-slate-100">
      <Stage
        ref={stageRef}
        width={size.w}
        height={size.h}
        scaleX={scale}
        scaleY={scale}
        x={stagePos.x}
        y={stagePos.y}
        draggable={tool === "pan" || tool === "select"}
        onDragEnd={(e) => {
          if (e.target === stageRef.current) {
            setStagePos({ x: e.target.x(), y: e.target.y() })
          }
        }}
        onWheel={handleWheel}
        onMouseDown={(e) => {
          if (e.target === stageRef.current) {
            if (
              tool === "create_sector" ||
              tool === "create_position" ||
              tool === "create_structure"
            ) {
              const local = localPointer()
              if (!local) return
              const m = screenPointToMetric(local.x, local.y, vp)
              onCreateAt(snapToGrid(m.x_m, gridM), snapToGrid(m.y_m, gridM))
              return
            }
            if (tool === "measure") {
              const local = localPointer()
              if (!local) return
              if (!measureRef.current) {
                measureRef.current = local
                setMeasureLine(null)
              } else {
                const a = measureRef.current
                const m1 = screenPointToMetric(a.x, a.y, vp)
                const m2 = screenPointToMetric(local.x, local.y, vp)
                const dist = Math.hypot(m2.x_m - m1.x_m, m2.y_m - m1.y_m)
                setMeasureLine({
                  x1: a.x,
                  y1: a.y,
                  x2: local.x,
                  y2: local.y,
                  distM: dist,
                })
                measureRef.current = null
              }
              return
            }
            onSelect(null)
          }
        }}
      >
        <Layer>
          <Rect
            x={vp.offsetX}
            y={vp.offsetY}
            width={content.contentW * vp.scale}
            height={content.contentH * vp.scale}
            fill={LAYOUT_COLORS.background}
            stroke="#94a3b8"
            strokeWidth={1.5}
          />
          {gridLines.map((ln, i) => (
            <Line key={`g-${i}`} points={ln.points} stroke={LAYOUT_COLORS.grid} strokeWidth={1} />
          ))}
          {markers ? (
            <>
              <Text
                x={markers.front.x - 36}
                y={markers.front.y + 6}
                width={72}
                align="center"
                text="FRENTE"
                fontSize={11}
                fontStyle="bold"
                fill={LAYOUT_COLORS.marker}
                listening={false}
              />
              <Text
                x={markers.back.x - 36}
                y={markers.back.y - 18}
                width={72}
                align="center"
                text="FONDO"
                fontSize={11}
                fontStyle="bold"
                fill={LAYOUT_COLORS.marker}
                listening={false}
              />
              <Text
                x={markers.left.x - 28}
                y={markers.left.y - 8}
                width={56}
                align="center"
                text="IZ"
                fontSize={10}
                fill={LAYOUT_COLORS.labelMuted}
                listening={false}
              />
              <Text
                x={markers.right.x - 28}
                y={markers.right.y - 8}
                width={56}
                align="center"
                text="DER"
                fontSize={10}
                fill={LAYOUT_COLORS.labelMuted}
                listening={false}
              />
            </>
          ) : null}
          {sorted.map((el) => {
            const scr = metricToScreen(
              {
                x_m: el.geometry.x_m,
                y_m: el.geometry.y_m,
                width_m: el.geometry.width_m,
                depth_m: el.geometry.depth_m,
                rotation_deg: el.geometry.rotation_deg || 0,
              },
              vp,
            )
            const colors = colorsForType(el.physical.element_type, {
              ...((el.physical.meta_json as Record<string, unknown>) || {}),
              ...((el.geometry.constraints_json as Record<string, unknown>) || {}),
            })
            const selected = el.clientKey === selectedKey
            const pending = el.geometry.validation_status === "pending_validation"
            const conflict = conflictKeys?.has(el.clientKey)
            const cx = scr.x + scr.width / 2
            const cy = scr.y + scr.height / 2
            const type = el.physical.element_type
            const showCode = type !== "facility_boundary" && (scr.width > 16 || scr.height > 16)
            // Sectores/circulación: etiqueta discreta para no tapar pasillos/estructuras
            const sectorQuiet = type === "sector" || type === "circulation"
            const fontSize = Math.max(
              sectorQuiet ? 10 : 9,
              Math.min(sectorQuiet ? 13 : 15, Math.min(scr.width, scr.height) / (sectorQuiet ? 8 : 5)),
            )
            const showDims = shouldShowDimensions(type, scr.width, scr.height)
            return (
              <Group
                key={el.clientKey}
                id={`el-${el.clientKey}`}
                x={cx}
                y={cy}
                rotation={scr.rotation}
                offsetX={scr.width / 2}
                offsetY={scr.height / 2}
                draggable={tool === "move" || (tool === "select" && selected)}
                onClick={(e) => {
                  e.cancelBubble = true
                  onSelect(el.clientKey)
                }}
                onTap={(e) => {
                  e.cancelBubble = true
                  onSelect(el.clientKey)
                }}
                onDragEnd={(e) => {
                  const node = e.target
                  const topLeft = {
                    x: node.x() - node.offsetX(),
                    y: node.y() - node.offsetY(),
                    width: scr.width,
                    height: scr.height,
                    rotation: node.rotation(),
                  }
                  let m = screenToMetric(topLeft, vp)
                  if (gridM > 0) {
                    m = {
                      ...m,
                      x_m: snapToGrid(m.x_m, gridM),
                      y_m: snapToGrid(m.y_m, gridM),
                    }
                  }
                  onChangeElement(el.clientKey, {
                    x_m: m.x_m,
                    y_m: m.y_m,
                    rotation_deg: m.rotation_deg,
                  })
                }}
                onTransformEnd={(e) => {
                  const node = e.target
                  const scaleX = node.scaleX()
                  const scaleY = node.scaleY()
                  node.scaleX(1)
                  node.scaleY(1)
                  const w = Math.max(0.05, scr.width * scaleX)
                  const h = Math.max(0.05, scr.height * scaleY)
                  const topLeft = {
                    x: node.x() - w / 2,
                    y: node.y() - h / 2,
                    width: w,
                    height: h,
                    rotation: node.rotation(),
                  }
                  const m = screenToMetric(topLeft, vp)
                  onChangeElement(el.clientKey, {
                    x_m: m.x_m,
                    y_m: m.y_m,
                    width_m: m.width_m,
                    depth_m: m.depth_m,
                    rotation_deg: m.rotation_deg,
                  })
                }}
              >
                <Rect
                  width={scr.width}
                  height={scr.height}
                  fill={conflict ? LAYOUT_COLORS.geom_conflict.fill : colors.fill}
                  stroke={
                    selected
                      ? LAYOUT_COLORS.selection.stroke
                      : conflict
                        ? LAYOUT_COLORS.geom_conflict.stroke
                        : colors.stroke
                  }
                  strokeWidth={selected ? LAYOUT_COLORS.selection.strokeWidth : type === "aisle" ? 1.75 : 1.25}
                  dash={pending ? LAYOUT_COLORS.pending_validation.dash : colors.dash}
                />
                {showCode ? (
                  <Text
                    text={el.physical.code}
                    width={scr.width}
                    height={Math.min(22, scr.height)}
                    y={sectorQuiet ? Math.max(4, scr.height * 0.08) : 3}
                    align="center"
                    verticalAlign="middle"
                    fontSize={fontSize}
                    fontStyle={sectorQuiet ? "normal" : "bold"}
                    fill={sectorQuiet ? LAYOUT_COLORS.labelMuted : LAYOUT_COLORS.label}
                    listening={false}
                  />
                ) : null}
                {showDims ? (
                  <Text
                    text={`${el.geometry.width_m.toFixed(2)}×${el.geometry.depth_m.toFixed(2)} m`}
                    width={scr.width}
                    y={scr.height - Math.min(18, scr.height * 0.35)}
                    align="center"
                    fontSize={Math.max(8, Math.min(11, scr.width / 10))}
                    fill={LAYOUT_COLORS.labelMuted}
                    listening={false}
                  />
                ) : null}
              </Group>
            )
          })}
          {measureLine ? (
            <>
              <Line
                points={[measureLine.x1, measureLine.y1, measureLine.x2, measureLine.y2]}
                stroke="#0ea5e9"
                strokeWidth={2}
                dash={[4, 4]}
              />
              <Text
                x={(measureLine.x1 + measureLine.x2) / 2}
                y={(measureLine.y1 + measureLine.y2) / 2 - 14}
                text={`${measureLine.distM.toFixed(2)} m`}
                fontSize={12}
                fill="#0369a1"
              />
            </>
          ) : null}
          <Transformer
            ref={trRef}
            rotateEnabled={tool === "rotate" || tool === "select"}
            enabledAnchors={
              tool === "resize" || tool === "select"
                ? undefined
                : tool === "rotate"
                  ? []
                  : ["top-left", "top-right", "bottom-left", "bottom-right"]
            }
            boundBoxFunc={(oldBox, newBox) => {
              if (newBox.width < 4 || newBox.height < 4) return oldBox
              return newBox
            }}
          />
        </Layer>
      </Stage>
      <div className="pointer-events-none absolute bottom-2 left-2 rounded bg-white/95 px-2 py-1 text-[11px] text-slate-600 shadow">
        Zoom {(scale * 100).toFixed(0)}% · cuadrícula {gridM} m ·{" "}
        {viewOrientationLabel(viewOrientationDeg)}
      </div>
      {onViewOrientationChange ? (
        <button
          type="button"
          className="absolute bottom-2 right-2 rounded border border-slate-300 bg-white/95 px-2 py-1 text-[11px] font-medium text-slate-700 shadow hover:bg-slate-50"
          title="Girar solo la vista (no modifica geometría guardada)"
          onClick={() => onViewOrientationChange(nextViewOrientation(viewOrientationDeg))}
        >
          Girar vista ({viewOrientationDeg}°)
        </button>
      ) : null}
    </div>
  )
}
