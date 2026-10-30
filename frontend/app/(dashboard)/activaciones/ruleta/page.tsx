"use client"

import { useCallback, useEffect, useRef, useState } from "react"
import { Maximize2, Minimize2, RotateCcw, Sparkles } from "lucide-react"

import { RuletaWheel } from "@/components/activaciones/ruleta-wheel"
import {
  pickUniformSegmentIndex,
  RULETA_SEGMENTS,
  RULETA_SLICE_DEG,
  type RuletaSegment,
} from "@/components/activaciones/ruleta-q1-segments"
import { Button } from "@/components/ui/button"
import { QUILLOTANA_LOGO_GRUPO_URL } from "@/lib/quillotana-brand"
import { cn } from "@/lib/utils"

const SPIN_MS_MIN = 5200
const SPIN_MS_MAX = 7000

function randomSpinDurationMs(): number {
  if (typeof crypto !== "undefined" && crypto.getRandomValues) {
    const buf = new Uint32Array(1)
    crypto.getRandomValues(buf)
    const t = (buf[0]! % 1000) / 1000
    return Math.round(SPIN_MS_MIN + t * (SPIN_MS_MAX - SPIN_MS_MIN))
  }
  return 6200
}

function targetRotationForIndex(index: number, currentRotation: number): number {
  // Centro de la casilla bajo el puntero superior; siempre avanzar (varias vueltas).
  const centerOffset = index * RULETA_SLICE_DEG + RULETA_SLICE_DEG / 2
  const normalized = ((currentRotation % 360) + 360) % 360
  const desiredMod = (360 - centerOffset) % 360
  let delta = desiredMod - normalized
  if (delta <= 0) delta += 360
  const extraTurns = 5 + (index % 3) // 5–7 vueltas visuales
  return currentRotation + delta + extraTurns * 360
}

export default function RuletaQuillotanaPage() {
  const stageRef = useRef<HTMLDivElement>(null)
  const [rotation, setRotation] = useState(0)
  const [spinning, setSpinning] = useState(false)
  const [durationMs, setDurationMs] = useState(6200)
  const [winner, setWinner] = useState<RuletaSegment | null>(null)
  const [spinCount, setSpinCount] = useState(0)
  const [isFullscreen, setIsFullscreen] = useState(false)
  const [wheelSize, setWheelSize] = useState(420)
  const pendingWinner = useRef<RuletaSegment | null>(null)

  useEffect(() => {
    const apply = () => {
      const w = typeof window !== "undefined" ? window.innerWidth : 1024
      if (w < 420) setWheelSize(Math.min(320, w - 32))
      else if (w < 768) setWheelSize(360)
      else setWheelSize(440)
    }
    apply()
    window.addEventListener("resize", apply)
    return () => window.removeEventListener("resize", apply)
  }, [])

  useEffect(() => {
    const onFs = () => setIsFullscreen(Boolean(document.fullscreenElement))
    document.addEventListener("fullscreenchange", onFs)
    return () => document.removeEventListener("fullscreenchange", onFs)
  }, [])

  const toggleFullscreen = useCallback(async () => {
    const el = stageRef.current
    if (!el) return
    try {
      if (!document.fullscreenElement) {
        await el.requestFullscreen()
      } else {
        await document.exitFullscreen()
      }
    } catch {
      // Algunos navegadores móviles restringen fullscreen; la UI sigue usable.
    }
  }, [])

  const spin = useCallback(() => {
    if (spinning) return
    const index = pickUniformSegmentIndex()
    const segment = RULETA_SEGMENTS[index]!
    pendingWinner.current = segment
    setWinner(null)
    const ms = randomSpinDurationMs()
    setDurationMs(ms)
    setSpinning(true)
    setRotation((prev) => targetRotationForIndex(index, prev))
    window.setTimeout(() => {
      setSpinning(false)
      setWinner(pendingWinner.current)
      setSpinCount((n) => n + 1)
    }, ms + 40)
  }, [spinning])

  const reset = useCallback(() => {
    if (spinning) return
    setWinner(null)
    pendingWinner.current = null
  }, [spinning])

  return (
    <div
      ref={stageRef}
      className={cn(
        "relative min-h-[calc(100dvh-4rem)] overflow-hidden rounded-xl",
        "bg-[#0b0612] text-amber-50",
      )}
    >
      {/* Atmósfera Halloween */}
      <div
        aria-hidden
        className="pointer-events-none absolute inset-0"
        style={{
          background: `
            radial-gradient(ellipse 80% 50% at 50% -10%, rgba(251,146,60,0.28), transparent 55%),
            radial-gradient(ellipse 60% 40% at 100% 100%, rgba(124,58,237,0.22), transparent 50%),
            radial-gradient(ellipse 50% 35% at 0% 80%, rgba(220,38,38,0.18), transparent 45%),
            linear-gradient(180deg, #12081c 0%, #0b0612 55%, #1a0a08 100%)
          `,
        }}
      />
      <div
        aria-hidden
        className="pointer-events-none absolute inset-0 opacity-[0.07]"
        style={{
          backgroundImage:
            "url(\"data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='60' height='60'%3E%3Cpath d='M30 4c2 8-6 12-6 20 8-2 14 6 14 12 0-8 8-12 10-20-8 4-12-4-18-12z' fill='%23f59e0b'/%3E%3C/svg%3E\")",
          backgroundSize: "72px 72px",
        }}
      />

      <div className="relative z-10 mx-auto flex w-full max-w-5xl flex-col gap-6 px-4 py-6 sm:px-6 lg:py-8">
        <header className="flex flex-col items-center gap-3 text-center sm:flex-row sm:items-end sm:justify-between sm:text-left">
          <div className="flex flex-col items-center gap-3 sm:flex-row sm:items-center sm:gap-4">
            {/* Logo oficial sin redibujar */}
            <img
              src={QUILLOTANA_LOGO_GRUPO_URL}
              alt="Grupo Quillotana"
              className="h-14 w-auto object-contain sm:h-16"
              draggable={false}
            />
            <div>
              <p className="text-[11px] font-semibold uppercase tracking-[0.22em] text-amber-400/90">
                Activación · Quillotana 1
              </p>
              <h1 className="font-serif text-3xl font-bold tracking-tight text-amber-50 sm:text-4xl">
                Ruleta Quillotana
              </h1>
              <p className="mt-1 max-w-md text-sm text-amber-100/70">
                Respaldo visual de la ruleta física. Nueve casillas, misma probabilidad.
                Solo demostración en entorno de pruebas.
              </p>
            </div>
          </div>
          <div className="flex items-center gap-2">
            <Button
              type="button"
              variant="outline"
              size="sm"
              onClick={toggleFullscreen}
              className="border-amber-500/40 bg-black/30 text-amber-100 hover:bg-amber-500/15 hover:text-amber-50"
            >
              {isFullscreen ? (
                <Minimize2 className="mr-1.5 h-4 w-4" />
              ) : (
                <Maximize2 className="mr-1.5 h-4 w-4" />
              )}
              Pantalla completa
            </Button>
            <Button
              type="button"
              variant="outline"
              size="sm"
              onClick={reset}
              disabled={spinning || !winner}
              className="border-amber-500/40 bg-black/30 text-amber-100 hover:bg-amber-500/15 hover:text-amber-50 disabled:opacity-40"
            >
              <RotateCcw className="mr-1.5 h-4 w-4" />
              Reiniciar
            </Button>
          </div>
        </header>

        <div className="grid items-center gap-8 lg:grid-cols-[1fr_minmax(280px,340px)]">
          <div className="flex flex-col items-center gap-5">
            <RuletaWheel
              rotationDeg={rotation}
              spinning={spinning}
              transitionMs={durationMs}
              size={wheelSize}
              className="mx-auto"
            />

            <Button
              type="button"
              size="lg"
              disabled={spinning}
              onClick={spin}
              className={cn(
                "h-14 min-w-[240px] rounded-full px-10 text-base font-bold tracking-wide shadow-lg",
                "bg-gradient-to-b from-amber-400 to-orange-600 text-[#1a0a08]",
                "hover:from-amber-300 hover:to-orange-500",
                "disabled:cursor-not-allowed disabled:opacity-60",
                spinning && "animate-pulse",
              )}
            >
              <Sparkles className="mr-2 h-5 w-5" />
              {spinning ? "Girando…" : "GIRAR RULETA"}
            </Button>
            <p className="text-xs text-amber-100/50">
              Giros en esta sesión: {spinCount}
              {spinning ? " · espera a que se detenga" : ""}
            </p>
          </div>

          <aside className="flex flex-col gap-4">
            <div
              className={cn(
                "rounded-2xl border border-amber-500/25 bg-black/40 p-5 backdrop-blur-sm",
                winner && !spinning && "ring-2 ring-amber-400/50",
              )}
            >
              <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-amber-400/80">
                Resultado
              </p>
              {spinning ? (
                <p className="mt-3 text-lg text-amber-100/80">La ruleta está girando…</p>
              ) : winner ? (
                <div className="mt-3 space-y-2">
                  <div
                    className="inline-flex items-center rounded-full px-3 py-1 text-xs font-bold uppercase tracking-wide"
                    style={{ backgroundColor: winner.color, color: winner.textColor }}
                  >
                    {winner.code}
                  </div>
                  <p className="font-serif text-2xl font-bold text-amber-50">{winner.label}</p>
                  <p className="text-sm text-amber-100/65">
                    Casilla {winner.index + 1} de 9 · detención alineada al puntero.
                  </p>
                </div>
              ) : (
                <p className="mt-3 text-sm text-amber-100/60">
                  Presiona <strong className="text-amber-200">GIRAR RULETA</strong> para
                  sortear. Cada casilla tiene la misma probabilidad (1/9).
                </p>
              )}
            </div>

            <div className="rounded-2xl border border-white/10 bg-black/30 p-4">
              <p className="mb-2 text-[11px] font-semibold uppercase tracking-[0.18em] text-amber-400/70">
                Casillas
              </p>
              <ul className="grid grid-cols-1 gap-1.5 sm:grid-cols-1">
                {RULETA_SEGMENTS.map((s) => (
                  <li
                    key={s.code}
                    className={cn(
                      "flex items-center gap-2 rounded-lg px-2 py-1.5 text-sm",
                      winner?.code === s.code && !spinning
                        ? "bg-amber-400/15 ring-1 ring-amber-400/40"
                        : "bg-white/5",
                    )}
                  >
                    <span
                      className="h-3 w-3 shrink-0 rounded-full ring-1 ring-black/40"
                      style={{ backgroundColor: s.color }}
                    />
                    <span className="truncate text-amber-50/90">{s.label}</span>
                  </li>
                ))}
              </ul>
            </div>
          </aside>
        </div>
      </div>

    </div>
  )
}
