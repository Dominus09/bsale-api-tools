"use client"

import { useMemo } from "react"

import type { ActiveSlot } from "./ruleta-q1-segments"
import {
  RULETA_HUB_DIAMETER_FRAC,
  RULETA_R_INNER_FRAC,
  RULETA_R_OUTER_FRAC,
  RULETA_SLICE_DEG,
} from "./ruleta-q1-segments"

/** Disco físico (PNG circular, hub ya enchapado; gira). */
export const RULETA_FISICA_SRC = "/activaciones/ruleta/ruleta-fisica-q1.png"
/** Emblema circular original, capa fija centrada. */
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

function slicePath(
  cx: number,
  cy: number,
  rOuter: number,
  rInner: number,
  startDeg: number,
  spanDeg: number,
) {
  const endDeg = startDeg + spanDeg
  const large = spanDeg > 180 ? 1 : 0
  const o0 = polar(cx, cy, rOuter, startDeg)
  const o1 = polar(cx, cy, rOuter, endDeg)
  const i1 = polar(cx, cy, rInner, endDeg)
  const i0 = polar(cx, cy, rInner, startDeg)
  return [
    `M ${o0.x} ${o0.y}`,
    `A ${rOuter} ${rOuter} 0 ${large} 1 ${o1.x} ${o1.y}`,
    `L ${i1.x} ${i1.y}`,
    `A ${rInner} ${rInner} 0 ${large} 0 ${i0.x} ${i0.y}`,
    "Z",
  ].join(" ")
}

/**
 * Tres capas concéntricas del mismo `size`:
 * A) disco giratorio (+ overlays de sustitución)
 * B) hub / logo fijo
 * C) puntero fijo inferior
 */
export function RuletaWheel({
  slots,
  rotationDeg,
  spinning,
  transitionMs = 6200,
  size = 480,
  className,
}: RuletaWheelProps) {
  const cx = size / 2
  const cy = size / 2
  const radius = size / 2
  const rOuter = radius * RULETA_R_OUTER_FRAC
  const rInner = radius * RULETA_R_INNER_FRAC
  const labelR = (rOuter + rInner) / 2
  const hubPx = Math.round(size * RULETA_HUB_DIAMETER_FRAC)
  const pointerW = Math.max(16, Math.round(size * 0.036))
  const pointerH = Math.max(24, Math.round(size * 0.052))
  const edgeInset = 0.85

  const overlays = useMemo(
    () =>
      slots
        .filter((s) => s.override !== "original")
        .map((slot) => {
          const span = Math.max(1, slot.spanDeg - edgeInset * 2)
          return {
            slot,
            startDeg: slot.midDeg - span / 2,
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
        marginBottom: pointerH + 6,
      }}
    >
      {/* Sombra suave en el marco (no deforma el círculo) */}
      <div
        aria-hidden
        style={{
          position: "absolute",
          inset: 0,
          borderRadius: "50%",
          boxShadow: spinning
            ? "0 0 40px rgba(255,90,40,0.35), 0 18px 40px rgba(0,0,0,0.45)"
            : "0 18px 48px rgba(0,0,0,0.5)",
          pointerEvents: "none",
        }}
      />

      {/* Capa A — disco giratorio */}
      <div
        style={{
          position: "absolute",
          inset: 0,
          borderRadius: "50%",
          overflow: "hidden",
          transform: `rotate(${rotationDeg}deg)`,
          transition: spinning
            ? `transform ${transitionMs}ms cubic-bezier(0.12, 0.75, 0.08, 1)`
            : "none",
          willChange: spinning ? "transform" : "auto",
        }}
      >
        <img
          src={RULETA_FISICA_SRC}
          alt="Ruleta física Quillotana"
          width={size}
          height={size}
          draggable={false}
          style={{
            width: size,
            height: size,
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
              const lineH = Math.max(12, size * 0.027)
              const fontSize = Math.max(10, size * 0.025)
              return (
                <g key={`ov-${slot.index}-${slot.displayCode}`}>
                  <path
                    d={slicePath(cx, cy, rOuter, rInner, startDeg, spanDeg)}
                    fill={slot.displayColor}
                    stroke="#ffffff"
                    strokeWidth={Math.max(1.5, size * 0.0028)}
                    strokeLinejoin="round"
                  />
                  <text
                    x={labelPos.x}
                    y={labelPos.y - ((words.length - 1) * lineH) / 2}
                    fill="#ffffff"
                    stroke="#111111"
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

      {/* Capa B — logo central fijo (mismo centro geométrico, sin anillos extra) */}
      <div
        className="pointer-events-none absolute left-1/2 top-1/2 z-10"
        style={{
          width: hubPx,
          height: hubPx,
          transform: "translate(-50%, -50%)",
        }}
      >
        <img
          src={RULETA_HUB_LOGO_SRC}
          alt="Quillotana"
          width={hubPx}
          height={hubPx}
          draggable={false}
          style={{
            width: hubPx,
            height: hubPx,
            display: "block",
            borderRadius: "50%",
            userSelect: "none",
          }}
        />
      </div>

      {/* Capa C — puntero fijo inferior */}
      <div
        aria-hidden
        className="pointer-events-none absolute left-1/2 z-20"
        style={{
          bottom: -(pointerH + 2),
          transform: "translateX(-50%)",
          width: pointerW * 2,
        }}
      >
        <div
          style={{
            margin: "0 auto",
            width: 0,
            height: 0,
            borderLeft: `${pointerW}px solid transparent`,
            borderRight: `${pointerW}px solid transparent`,
            borderBottom: `${pointerH}px solid #E53935`,
            filter: "drop-shadow(0 2px 3px rgba(0,0,0,0.45))",
          }}
        />
        <div
          style={{
            width: Math.round(pointerW * 0.48),
            height: Math.round(pointerW * 0.48),
            margin: `${-Math.round(pointerH * 0.1)}px auto 0`,
            borderRadius: "50%",
            background: "#B71C1C",
            border: "2px solid #fff",
            boxShadow: "0 1px 2px rgba(0,0,0,0.4)",
          }}
        />
      </div>

      <span className="sr-only">
        Ruleta Quillotana, {360 / RULETA_SLICE_DEG} casillas de {RULETA_SLICE_DEG}°, probabilidad
        1/{360 / RULETA_SLICE_DEG} cada una
      </span>
    </div>
  )
}
