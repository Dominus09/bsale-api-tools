"use client"

import { useMemo } from "react"

import { RULETA_SEGMENTS, RULETA_SLICE_DEG } from "./ruleta-q1-segments"

type RuletaWheelProps = {
  rotationDeg: number
  spinning: boolean
  /** Duración del ease-out en ms (rango típico 5200–7000). */
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
  rotationDeg,
  spinning,
  transitionMs = 6200,
  size = 420,
  className,
}: RuletaWheelProps) {
  const cx = size / 2
  const cy = size / 2
  const outer = size * 0.48
  const labelR = size * 0.33

  const slices = useMemo(
    () =>
      RULETA_SEGMENTS.map((seg, i) => {
        const start = i * RULETA_SLICE_DEG
        const end = start + RULETA_SLICE_DEG
        const mid = start + RULETA_SLICE_DEG / 2
        const labelPos = polar(cx, cy, labelR, mid)
        return { seg, start, end, mid, labelPos }
      }),
    [cx, cy, labelR],
  )

  return (
    <div
      className={className}
      style={{
        width: size,
        height: size,
        position: "relative",
        filter: spinning
          ? "drop-shadow(0 0 28px rgba(255,140,40,0.45))"
          : "drop-shadow(0 18px 40px rgba(0,0,0,0.55))",
      }}
    >
      {/* Marco Halloween */}
      <div
        aria-hidden
        className="pointer-events-none absolute inset-0 rounded-full"
        style={{
          background:
            "conic-gradient(from 0deg, #ff6b00, #7c3aed, #111827, #f59e0b, #ff6b00)",
          padding: 8,
          WebkitMask:
            "radial-gradient(farthest-side, transparent calc(100% - 10px), #000 calc(100% - 9px))",
          mask: "radial-gradient(farthest-side, transparent calc(100% - 10px), #000 calc(100% - 9px))",
        }}
      />

      <svg
        width={size}
        height={size}
        viewBox={`0 0 ${size} ${size}`}
        role="img"
        aria-label="Ruleta Quillotana de nueve casillas"
        style={{
          transform: `rotate(${rotationDeg}deg)`,
          transition: spinning
            ? `transform ${transitionMs}ms cubic-bezier(0.12, 0.75, 0.08, 1)`
            : "none",
          borderRadius: "50%",
        }}
      >
        <defs>
          <filter id="ruleta-inner-shadow" x="-20%" y="-20%" width="140%" height="140%">
            <feDropShadow dx="0" dy="2" stdDeviation="3" floodOpacity="0.35" />
          </filter>
        </defs>
        <circle cx={cx} cy={cy} r={outer + 4} fill="#1a1025" />
        <g filter="url(#ruleta-inner-shadow)">
          {slices.map(({ seg, start, end, labelPos }) => (
            <g key={seg.code}>
              <path
                d={slicePath(cx, cy, outer, start, end)}
                fill={seg.color}
                stroke="#0f0a14"
                strokeWidth={1.5}
              />
              <text
                x={labelPos.x}
                y={labelPos.y}
                fill={seg.textColor}
                fontSize={size < 360 ? 9.5 : 11}
                fontWeight={700}
                textAnchor="middle"
                dominantBaseline="middle"
                transform={`rotate(${start + RULETA_SLICE_DEG / 2}, ${labelPos.x}, ${labelPos.y})`}
                style={{
                  fontFamily: "ui-sans-serif, system-ui, Segoe UI, sans-serif",
                  letterSpacing: "0.02em",
                }}
              >
                {seg.label.length > 16 ? seg.label.slice(0, 14) + "…" : seg.label}
              </text>
            </g>
          ))}
        </g>
        <circle cx={cx} cy={cy} r={size * 0.09} fill="#120a1a" stroke="#f59e0b" strokeWidth={3} />
        <circle cx={cx} cy={cy} r={size * 0.035} fill="#fbbf24" />
      </svg>

      {/* Puntero superior fijo */}
      <div
        aria-hidden
        className="absolute left-1/2 top-0 z-10 -translate-x-1/2"
        style={{ marginTop: -4 }}
      >
        <div
          style={{
            width: 0,
            height: 0,
            borderLeft: "14px solid transparent",
            borderRight: "14px solid transparent",
            borderTop: "28px solid #fbbf24",
            filter: "drop-shadow(0 2px 4px rgba(0,0,0,0.5))",
          }}
        />
      </div>
    </div>
  )
}
