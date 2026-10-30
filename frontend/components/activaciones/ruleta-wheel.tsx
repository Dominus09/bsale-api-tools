"use client"

import type { ActiveSlot, RuletaIconKind } from "./ruleta-q1-segments"
import {
  RULETA_CENTRO_LOGO_SRC,
  RULETA_SLICE_DEG,
  RULETA_SLOT_COUNT,
} from "./ruleta-q1-segments"

/** viewBox único — todo el disco giratorio comparte este centro. */
const VB = 1000
const CX = 500
const CY = 500
const R_RIM_OUTER = 498
const R_RIM_MID = 486
const R_RIM_INNER = 472
const R_DISK = 468
const R_INNER = 168
/** Radio de la única imagen central fija (cubre uniones de sectores). */
const R_CENTRO = R_INNER + 22
const R_LABEL = 312
const R_ICON = 405
/** Separación vertical entre líneas cuando el % es grande (evita solape 5%/3%/10%). */
const BIG_LINE_H = 56
const NORMAL_LINE_H = 28
const R_DIVIDER_OUTER = R_DISK - 2
const R_DIVIDER_INNER = R_INNER + 2
const RIVET_R = 7
const R_RIVET = (R_RIM_OUTER + R_RIM_INNER) / 2

type RuletaWheelProps = {
  slots: ActiveSlot[]
  rotationDeg: number
  spinning: boolean
  transitionMs?: number
  size?: number
  className?: string
}

/** 0° = arriba, horario (compatible con SVG rotate). */
function polar(r: number, angleDeg: number) {
  const rad = ((angleDeg - 90) * Math.PI) / 180
  return { x: CX + r * Math.cos(rad), y: CY + r * Math.sin(rad) }
}

function sectorPath(startDeg: number, spanDeg: number, rOuter: number, rInner: number) {
  const endDeg = startDeg + spanDeg
  const large = spanDeg > 180 ? 1 : 0
  const o0 = polar(rOuter, startDeg)
  const o1 = polar(rOuter, endDeg)
  const i1 = polar(rInner, endDeg)
  const i0 = polar(rInner, startDeg)
  return [
    `M ${o0.x} ${o0.y}`,
    `A ${rOuter} ${rOuter} 0 ${large} 1 ${o1.x} ${o1.y}`,
    `L ${i1.x} ${i1.y}`,
    `A ${rInner} ${rInner} 0 ${large} 0 ${i0.x} ${i0.y}`,
    "Z",
  ].join(" ")
}

function SectorIcon({ kind, fill }: { kind: RuletaIconKind; fill: string }) {
  const stroke = fill === "#1A1208" ? "#111" : "#111"
  switch (kind) {
    case "gift":
      return (
        <g>
          <rect x={-18} y={-6} width={36} height={28} rx={3} fill="#fff" stroke={stroke} strokeWidth={2} />
          <rect x={-18} y={-14} width={36} height={10} rx={2} fill="#fff" stroke={stroke} strokeWidth={2} />
          <path d="M0 -14 V22 M-18 0 H18" stroke="#E53935" strokeWidth={3} fill="none" />
          <path d="M-6 -18 Q0 -26 6 -18" stroke="#E53935" strokeWidth={2.5} fill="none" />
        </g>
      )
    case "cups":
      return (
        <g>
          <path d="M-22 -8 h16 l2 28 h-20 z" fill="#fff" stroke={stroke} strokeWidth={2} />
          <path d="M6 -8 h16 l-2 28 h-20 z" fill="#fff" stroke={stroke} strokeWidth={2} />
          <circle cx={-14} cy={6} r={5} fill="#E53935" />
          <circle cx={14} cy={6} r={5} fill="#E53935" />
          <text x={-14} y={9} textAnchor="middle" fontSize={8} fontWeight={800} fill="#fff">
            Q
          </text>
          <text x={14} y={9} textAnchor="middle" fontSize={8} fontWeight={800} fill="#fff">
            Q
          </text>
        </g>
      )
    case "can":
      return (
        <g>
          <rect x={-12} y={-22} width={24} height={44} rx={6} fill="#E53935" stroke={stroke} strokeWidth={2} />
          <ellipse cx={0} cy={-22} rx={12} ry={5} fill="#ff6b6b" stroke={stroke} strokeWidth={1.5} />
          <circle cx={0} cy={2} r={7} fill="#fff" />
          <text x={0} y={5} textAnchor="middle" fontSize={10} fontWeight={800} fill="#E53935">
            Q
          </text>
        </g>
      )
    case "sad":
      return (
        <g>
          <circle cx={0} cy={0} r={22} fill="#fff" stroke={stroke} strokeWidth={2} />
          <circle cx={-7} cy={-4} r={2.5} fill="#333" />
          <circle cx={7} cy={-4} r={2.5} fill="#333" />
          <path d="M-10 10 Q0 2 10 10" stroke="#333" strokeWidth={2.5} fill="none" strokeLinecap="round" />
        </g>
      )
    case "retry":
      return (
        <g>
          <path
            d="M14 -6 A16 16 0 1 0 14 8"
            fill="none"
            stroke="#fff"
            strokeWidth={5}
            strokeLinecap="round"
          />
          <path d="M14 -6 L22 -2 L12 4 Z" fill="#fff" stroke="#111" strokeWidth={1} />
        </g>
      )
    case "random":
      return (
        <g>
          <circle cx={0} cy={0} r={20} fill="#fff" stroke={stroke} strokeWidth={2} />
          <text x={0} y={7} textAnchor="middle" fontSize={22} fontWeight={900} fill="#6D28D9">
            ?
          </text>
        </g>
      )
    case "percent":
    case "stars":
    default:
      return (
        <g>
          <path d="M0 -10 L3 -2 L11 -2 L4 3 L7 11 L0 6 L-7 11 L-4 3 L-11 -2 L-3 -2 Z" fill="#fff" stroke={stroke} strokeWidth={1} />
          <path d="M18 6 L19.5 10 L24 10 L20.5 13 L22 17 L18 14.5 L14 17 L15.5 13 L12 10 L16.5 10 Z" fill="#fff" opacity={0.9} />
          <path d="M-18 8 L-16.8 11 L-13 11 L-16 13.2 L-14.8 16.5 L-18 14.5 L-21.2 16.5 L-20 13.2 L-23 11 L-19.2 11 Z" fill="#fff" opacity={0.85} />
        </g>
      )
  }
}

function DecorDots({ midDeg, color }: { midDeg: number; color: string }) {
  const dots = [
    { r: 250, a: -12, s: 3.5 },
    { r: 270, a: 10, s: 2.5 },
    { r: 360, a: -8, s: 3 },
    { r: 380, a: 11, s: 2.2 },
  ]
  return (
    <g opacity={0.35}>
      {dots.map((d, i) => {
        const p = polar(d.r, midDeg + d.a)
        return <circle key={i} cx={p.x} cy={p.y} r={d.s} fill={color === "#FDD835" ? "#fff" : "#fff"} />
      })}
    </g>
  )
}

function SectorContent({ slot }: { slot: ActiveSlot }) {
  const labelPos = polar(R_LABEL, slot.midDeg)
  const lines = slot.displayLines
  const big = slot.displayEmphasizeFirstLine
  // Texto promocional: blanco + borde negro (mismo estilo 3%, 5% y 10%).
  const fill = "#FFFFFF"
  const stroke = "#000000"
  const lineH = big ? BIG_LINE_H : NORMAL_LINE_H
  const startY = -((lines.length - 1) * lineH) / 2
  // Ícono un poco más afuera en sectores con % grande para no chocar con el texto.
  const iconR = big ? R_ICON + 8 : R_ICON
  const iconAt = polar(iconR, slot.midDeg)

  return (
    <g>
      <DecorDots midDeg={slot.midDeg} color={slot.displayColor} />
      <g transform={`translate(${iconAt.x} ${iconAt.y}) rotate(${slot.midDeg})`}>
        <SectorIcon kind={slot.displayIcon} fill={fill} />
      </g>
      <g transform={`translate(${labelPos.x} ${labelPos.y}) rotate(${slot.midDeg})`}>
        {lines.map((line, i) => {
          const fontSize = big && i === 0 ? 46 : big ? 19 : 24
          const y = startY + i * lineH
          const common = {
            x: 0 as const,
            y,
            textAnchor: "middle" as const,
            dominantBaseline: "central" as const,
            fontSize,
            fontWeight: 800 as const,
            strokeLinejoin: "round" as const,
            style: {
              fontFamily: "Impact, Haettenschweiler, 'Arial Black', sans-serif",
              letterSpacing: "0.02em",
            },
          }
          // Dos pasadas (borde luego relleno): evita ghosting de paint-order en ángulos extremos (p.ej. 5%).
          return (
            <g key={`${slot.index}-${i}`}>
              <text {...common} fill="none" stroke={stroke} strokeWidth={big ? 7 : 6}>
                {line}
              </text>
              <text {...common} fill={fill} stroke="none">
                {line}
              </text>
            </g>
          )
        })}
      </g>
    </g>
  )
}

export function RuletaWheel({
  slots,
  rotationDeg,
  spinning,
  transitionMs = 6200,
  size = 520,
  className,
}: RuletaWheelProps) {
  const pointerW = Math.max(16, size * 0.038)
  const pointerH = Math.max(26, size * 0.055)

  const rivets = Array.from({ length: RULETA_SLOT_COUNT * 2 }, (_, i) => {
    const ang = (i * 360) / (RULETA_SLOT_COUNT * 2)
    return polar(R_RIVET, ang)
  })

  return (
    <div
      className={className}
      style={{
        width: size,
        height: size,
        position: "relative",
        flexShrink: 0,
        marginBottom: pointerH + 8,
      }}
    >
      <div
        aria-hidden
        style={{
          position: "absolute",
          inset: 0,
          borderRadius: "50%",
          boxShadow: spinning
            ? "0 0 42px rgba(255,90,40,0.35), 0 20px 48px rgba(0,0,0,0.5)"
            : "0 20px 52px rgba(0,0,0,0.55)",
          pointerEvents: "none",
        }}
      />

      <svg
        width={size}
        height={size}
        viewBox={`0 0 ${VB} ${VB}`}
        style={{ display: "block", overflow: "visible" }}
        role="img"
        aria-label="Ruleta Quillotana"
      >
        <defs>
          <radialGradient id="ruleta-rim-grad" cx="50%" cy="45%" r="55%">
            <stop offset="0%" stopColor="#ffffff" />
            <stop offset="55%" stopColor="#e8e8ea" />
            <stop offset="100%" stopColor="#b0b3ba" />
          </radialGradient>
          <linearGradient id="ruleta-rim-shine" x1="0%" y1="0%" x2="0%" y2="100%">
            <stop offset="0%" stopColor="#fff" stopOpacity="0.7" />
            <stop offset="45%" stopColor="#cfd2d8" stopOpacity="0.35" />
            <stop offset="100%" stopColor="#8e929a" stopOpacity="0.55" />
          </linearGradient>
          <filter id="ruleta-soft" x="-20%" y="-20%" width="140%" height="140%">
            <feDropShadow dx="0" dy="3" stdDeviation="4" floodOpacity="0.28" />
          </filter>
        </defs>

        {/* Grupo único giratorio: sectores + aro + remaches + textos */}
        <g
          style={{
            transformBox: "view-box",
            transformOrigin: "50% 50%",
            transform: `rotate(${rotationDeg}deg)`,
            transition: spinning
              ? `transform ${transitionMs}ms cubic-bezier(0.12, 0.75, 0.08, 1)`
              : "none",
            willChange: spinning ? "transform" : "auto",
          }}
        >
          {/* Aro metálico concéntrico */}
          <circle cx={CX} cy={CY} r={R_RIM_OUTER} fill="url(#ruleta-rim-grad)" filter="url(#ruleta-soft)" />
          <circle cx={CX} cy={CY} r={R_RIM_MID} fill="none" stroke="url(#ruleta-rim-shine)" strokeWidth={10} />
          <circle cx={CX} cy={CY} r={R_RIM_INNER} fill="#d9dbe0" />
          <circle cx={CX} cy={CY} r={R_DISK} fill="#1a1a1a" />

          {/* Sectores */}
          {slots.map((slot) => (
            <path
              key={`sec-${slot.index}`}
              d={sectorPath(slot.startDeg, slot.spanDeg, R_DISK - 1, R_INNER)}
              fill={slot.displayColor}
            />
          ))}

          {/* Divisiones blancas */}
          {slots.map((slot) => {
            const a = slot.startDeg
            const o = polar(R_DIVIDER_OUTER, a)
            const i = polar(R_DIVIDER_INNER, a)
            return (
              <line
                key={`div-${slot.index}`}
                x1={i.x}
                y1={i.y}
                x2={o.x}
                y2={o.y}
                stroke="#ffffff"
                strokeWidth={6}
                strokeLinecap="round"
              />
            )
          })}

          {/* Contenido por sector (texto + ícono del premio activo) */}
          {slots.map((slot) => (
            <SectorContent key={`content-${slot.index}-${slot.displayCode}`} slot={slot} />
          ))}

          {/* Remaches del aro */}
          {rivets.map((p, i) => (
            <g key={`rivet-${i}`}>
              <circle cx={p.x} cy={p.y} r={RIVET_R + 1.5} fill="#9aa0a8" />
              <circle cx={p.x} cy={p.y} r={RIVET_R} fill="#f4f5f7" stroke="#7a7f88" strokeWidth={1.2} />
              <circle cx={p.x - 1.5} cy={p.y - 1.5} r={2} fill="#fff" opacity={0.7} />
            </g>
          ))}
        </g>

        {/* Única pieza central fija (PNG ya trae círculo blanco + logo). Fuera del rotate. */}
        <image
          href={RULETA_CENTRO_LOGO_SRC}
          x={CX - R_CENTRO}
          y={CY - R_CENTRO}
          width={R_CENTRO * 2}
          height={R_CENTRO * 2}
          preserveAspectRatio="xMidYMid meet"
        />
      </svg>

      {/* Puntero rojo fijo inferior */}
      <div
        aria-hidden
        className="pointer-events-none absolute left-1/2 z-20"
        style={{
          bottom: -(pointerH + 2),
          transform: "translateX(-50%)",
        }}
      >
        <div
          style={{
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
        Ruleta SVG Quillotana, {RULETA_SLOT_COUNT} casillas de {RULETA_SLICE_DEG}°, probabilidad
        1/{RULETA_SLOT_COUNT}
      </span>
    </div>
  )
}
