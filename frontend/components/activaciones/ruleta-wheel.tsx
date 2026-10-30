"use client"

import { useMemo } from "react"

import type { ActiveSlot } from "./ruleta-q1-segments"

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

function slicePath(cx: number, cy: number, r: number, startDeg: number, endDeg: number) {
  const start = polar(cx, cy, r, startDeg)
  const end = polar(cx, cy, r, endDeg)
  const large = endDeg - startDeg > 180 ? 1 : 0
  return `M ${cx} ${cy} L ${start.x} ${start.y} A ${r} ${r} 0 ${large} 1 ${end.x} ${end.y} Z`
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
  const outer = size * 0.485
  const labelR = size * 0.34

  const slices = useMemo(
    () =>
      slots
        .filter((s) => s.spanDeg > 0.05)
        .map((slot) => {
          const labelPos = polar(cx, cy, labelR, slot.midDeg)
          const useShort = size < 400
          const fontSize = Math.max(11, size * 0.028)
          return { slot, labelPos, useShort, fontSize }
        }),
    [slots, cx, cy, labelR, size],
  )

  const pointer = Math.max(12, size * 0.032)

  return (
    <div
      className={className}
      style={{
        width: size,
        height: size,
        maxWidth: "100%",
        position: "relative",
        filter: spinning
          ? "drop-shadow(0 0 32px rgba(255,140,40,0.5))"
          : "drop-shadow(0 20px 48px rgba(0,0,0,0.55))",
      }}
    >
      <div
        aria-hidden
        className="pointer-events-none absolute inset-0 rounded-full"
        style={{
          background:
            "conic-gradient(from 0deg, #ff6b00, #7c3aed, #111827, #f59e0b, #ff6b00)",
          WebkitMask:
            "radial-gradient(farthest-side, transparent calc(100% - 11px), #000 calc(100% - 10px))",
          mask: "radial-gradient(farthest-side, transparent calc(100% - 11px), #000 calc(100% - 10px))",
        }}
      />

      <svg
        width={size}
        height={size}
        viewBox={`0 0 ${size} ${size}`}
        role="img"
        aria-label="Ruleta Quillotana"
        style={{
          transform: `rotate(${rotationDeg}deg)`,
          transition: spinning
            ? `transform ${transitionMs}ms cubic-bezier(0.12, 0.75, 0.08, 1)`
            : "none",
          borderRadius: "50%",
          willChange: spinning ? "transform" : "auto",
        }}
      >
        <defs>
          <filter id="ruleta-inner-shadow" x="-20%" y="-20%" width="140%" height="140%">
            <feDropShadow dx="0" dy="2" stdDeviation="3" floodOpacity="0.35" />
          </filter>
        </defs>
        <circle cx={cx} cy={cy} r={outer + 3} fill="#1a1025" />
        <g filter="url(#ruleta-inner-shadow)">
          {slices.map(({ slot, labelPos, useShort, fontSize }) => (
            <g key={`${slot.index}-${slot.displayCode}`}>
              <path
                d={slicePath(cx, cy, outer, slot.startDeg, slot.endDeg)}
                fill={slot.displayColor}
                stroke="#0f0a14"
                strokeWidth={1.25}
              />
              <text
                x={labelPos.x}
                y={labelPos.y}
                fill={slot.displayTextColor}
                fontSize={fontSize}
                fontWeight={700}
                textAnchor="middle"
                dominantBaseline="middle"
                transform={`rotate(${slot.midDeg}, ${labelPos.x}, ${labelPos.y})`}
                style={{
                  fontFamily: "ui-sans-serif, system-ui, Segoe UI, sans-serif",
                  letterSpacing: "0.01em",
                  paintOrder: "stroke",
                  stroke: "rgba(0,0,0,0.25)",
                  strokeWidth: 0.6,
                }}
              >
                {useShort ? slot.displayShortLabel : slot.displayLabel}
              </text>
            </g>
          ))}
        </g>
        <circle
          cx={cx}
          cy={cy}
          r={size * 0.08}
          fill="#120a1a"
          stroke="#f59e0b"
          strokeWidth={Math.max(2, size * 0.006)}
        />
        <circle cx={cx} cy={cy} r={size * 0.03} fill="#fbbf24" />
      </svg>

      <div
        aria-hidden
        className="absolute left-1/2 top-0 z-10 -translate-x-1/2"
        style={{ marginTop: -2 }}
      >
        <div
          style={{
            width: 0,
            height: 0,
            borderLeft: `${pointer}px solid transparent`,
            borderRight: `${pointer}px solid transparent`,
            borderTop: `${pointer * 1.9}px solid #fbbf24`,
            filter: "drop-shadow(0 2px 4px rgba(0,0,0,0.5))",
          }}
        />
      </div>
    </div>
  )
}
