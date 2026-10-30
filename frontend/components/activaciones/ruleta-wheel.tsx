"use client"

import { useMemo } from "react"

import type { ActiveSlot } from "./ruleta-q1-segments"
import {
  RULETA_HUB_DIAMETER_FRAC,
  RULETA_SLICE_DEG,
  RULETA_R_INNER_FRAC,
  RULETA_R_OUTER_FRAC,
} from "./ruleta-q1-segments"

/** Foto oficial de la ruleta física (textura que gira). Recorte cuadrado del disco. */
export const RULETA_FISICA_SRC = "/activaciones/ruleta/ruleta-fisica-q1.jpg"
/** Emblema circular original extraído del centro de la foto (capa fija). */
export const RULETA_HUB_LOGO_SRC = "/activaciones/ruleta/quillotana-hub-logo.png"

type RuletaWheelProps = {
  slots: ActiveSlot[]
  rotationDeg: number
  spinning: boolean
  transitionMs?: number
  size?: number
  className?: string
}

function polar(cx: number, cy: number, r: number, angleDeg: number) {
  const rad = ((angleDeg - 90) * Math.PI) / 180
  return { x: cx + r * Math.cos(rad), y: cy + r * Math.sin(rad) }
}

/** Arco de sector en coordenadas de la foto (0° = arriba, horario). */
function slicePath(
  cx: number,
  cy: number,
  rOuter: number,
  rInner: number,
  startDeg: number,
  spanDeg: number,
) {
  const absoluteEnd = startDeg + spanDeg
  const large = spanDeg > 180 ? 1 : 0
  const o0 = polar(cx, cy, rOuter, startDeg)
  const o1 = polar(cx, cy, rOuter, absoluteEnd)
  const i1 = polar(cx, cy, rInner, absoluteEnd)
  const i0 = polar(cx, cy, rInner, startDeg)
  return [
    `M ${o0.x} ${o0.y}`,
    `A ${rOuter} ${rOuter} 0 ${large} 1 ${o1.x} ${o1.y}`,
    `L ${i1.x} ${i1.y}`,
    `A ${rInner} ${rInner} 0 ${large} 0 ${i0.x} ${i0.y}`,
    "Z",
  ].join(" ")
}

export function RuletaWheel({
  slots,
  rotationDeg,
  spinning,
  transitionMs = 6200,
  size = 420,
  className,
}: RuletaWheelProps) {
  const cx = size / 2
  const cy = size / 2
  const radius = size / 2
  const rOuter = radius * RULETA_R_OUTER_FRAC
  const rInner = radius * RULETA_R_INNER_FRAC
  const labelR = (rOuter + rInner) / 2
  const hubPx = size * RULETA_HUB_DIAMETER_FRAC
  const pointerW = Math.max(14, size * 0.038)
  const pointerH = Math.max(22, size * 0.055)
  /** Inset angular para no tapar las divisiones blancas (~1° por lado). */
  const edgeInset = 1.1

  const overlays = useMemo(
    () =>
      slots
        .filter((s) => s.override !== "original")
        .map((slot) => {
          const span = Math.max(1, slot.spanDeg - edgeInset * 2)
          const start = slot.midDeg - span / 2
          return {
            slot,
            startDeg: start,
            spanDeg: span,
            labelPos: polar(cx, cy, labelR, slot.midDeg),
          }
        }),
    [slots, cx, cy, labelR],
  )

  return (
    <div
      className={className}
      style={{
        width: size,
        height: size,
        position: "relative",
        flexShrink: 0,
        /* reserva visual del puntero sin cortarlo */
        marginBottom: pointerH + 8,
        filter: spinning
          ? "drop-shadow(0 0 36px rgba(255,80,40,0.4))"
          : "drop-shadow(0 22px 50px rgba(0,0,0,0.55))",
      }}
    >
      {/* Disco + overlays: única transformación de rotación */}
      <div
        style={{
          width: size,
          height: size,
          borderRadius: "50%",
          overflow: "hidden",
          transform: `rotate(${rotationDeg}deg)`,
          transition: spinning
            ? `transform ${transitionMs}ms cubic-bezier(0.12, 0.75, 0.08, 1)`
            : "none",
          willChange: spinning ? "transform" : "auto",
          position: "relative",
        }}
      >
        <img
          src={RULETA_FISICA_SRC}
          alt="Ruleta física Quillotana"
          draggable={false}
          style={{
            width: "100%",
            height: "100%",
            objectFit: "cover",
            objectPosition: "center center",
            display: "block",
            userSelect: "none",
            pointerEvents: "none",
          }}
        />

        {overlays.length > 0 ? (
          <svg
            width={size}
            height={size}
            viewBox={`0 0 ${size} ${size}`}
            style={{ position: "absolute", inset: 0, pointerEvents: "none" }}
            aria-hidden
          >
            {overlays.map(({ slot, startDeg, spanDeg, labelPos }) => {
              const words = slot.displayShortLabel.toUpperCase().split(/\s+/)
              const lineH = Math.max(11, size * 0.026)
              const fontSize = Math.max(9, size * 0.024)
              return (
                <g key={`ov-${slot.index}-${slot.displayCode}`}>
                  <path
                    d={slicePath(cx, cy, rOuter, rInner, startDeg, spanDeg)}
                    fill={slot.displayColor}
                    fillOpacity={1}
                    stroke="#ffffff"
                    strokeWidth={Math.max(1.5, size * 0.003)}
                    strokeLinejoin="round"
                  />
                  <text
                    x={labelPos.x}
                    y={labelPos.y - ((words.length - 1) * lineH) / 2}
                    fill="#ffffff"
                    stroke="#000000"
                    strokeWidth={Math.max(1.1, size * 0.003)}
                    paintOrder="stroke fill"
                    fontSize={fontSize}
                    fontWeight={800}
                    textAnchor="middle"
                    dominantBaseline="middle"
                    transform={`rotate(${slot.midDeg} ${labelPos.x} ${labelPos.y})`}
                    style={{
                      fontFamily: "Impact, Haettenschweiler, Arial Black, sans-serif",
                      letterSpacing: "0.02em",
                    }}
                  >
                    {words.map((w, wi) => (
                      <tspan key={`${slot.index}-${wi}`} x={labelPos.x} dy={wi === 0 ? 0 : lineH}>
                        {w}
                      </tspan>
                    ))}
                  </text>
                </g>
              )
            })}
          </svg>
        ) : null}
      </div>

      {/* Emblema central FIJO: máscara opaca + logo original (fuera del rotate) */}
      <div
        aria-hidden
        className="pointer-events-none absolute left-1/2 top-1/2 z-10 -translate-x-1/2 -translate-y-1/2"
        style={{ width: hubPx, height: hubPx }}
      >
        <div
          style={{
            position: "absolute",
            inset: 0,
            borderRadius: "50%",
            background: "#F5F3EE",
            boxShadow: "0 0 0 2px rgba(255,255,255,0.85), 0 4px 14px rgba(0,0,0,0.35)",
          }}
        />
        <img
          src={RULETA_HUB_LOGO_SRC}
          alt=""
          draggable={false}
          style={{
            position: "absolute",
            inset: 0,
            width: "100%",
            height: "100%",
            borderRadius: "50%",
            objectFit: "cover",
            display: "block",
            userSelect: "none",
          }}
        />
      </div>

      {/* Triángulo rojo fijo inferior */}
      <div
        aria-hidden
        className="pointer-events-none absolute left-1/2 z-20 -translate-x-1/2"
        style={{ bottom: -(pointerH + 2) }}
      >
        <div
          style={{
            width: 0,
            height: 0,
            borderLeft: `${pointerW}px solid transparent`,
            borderRight: `${pointerW}px solid transparent`,
            borderBottom: `${pointerH}px solid #E53935`,
            filter: "drop-shadow(0 2px 4px rgba(0,0,0,0.55))",
          }}
        />
        <div
          style={{
            width: pointerW * 0.5,
            height: pointerW * 0.5,
            margin: `${-pointerH * 0.12}px auto 0`,
            borderRadius: "50%",
            background: "#B71C1C",
            border: "2px solid #fff",
            boxShadow: "0 1px 3px rgba(0,0,0,0.45)",
          }}
        />
      </div>

      <span className="sr-only">
        Ruleta física Quillotana, {360 / RULETA_SLICE_DEG} sectores de {RULETA_SLICE_DEG}°,
        probabilidad uniforme 1/{360 / RULETA_SLICE_DEG}
      </span>
    </div>
  )
}
