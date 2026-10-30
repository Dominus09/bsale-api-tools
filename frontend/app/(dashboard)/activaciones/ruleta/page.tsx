"use client"

import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import { Maximize2, Minimize2, RotateCcw, Settings2, Sparkles } from "lucide-react"

import { RuletaResultModal } from "@/components/activaciones/ruleta-result-modal"
import { RuletaWheel } from "@/components/activaciones/ruleta-wheel"
import {
  buildActiveSlots,
  clearConfigStorage,
  defaultConfig,
  loadConfigFromStorage,
  pickUniformSlotIndex,
  saveConfigToStorage,
  type ActiveSlot,
  type RuletaLocalConfig,
  type SlotOverride,
  RULETA_SEGMENTS,
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

/**
 * Alinea el centro del sector (midDeg en coordenadas de la foto) con el indicador fijo inferior (180°).
 * midDeg + rotación ≡ 180 (mod 360). Siempre avanza en sentido horario.
 */
function targetRotationForMid(midDeg: number, currentRotation: number): number {
  const normalized = ((currentRotation % 360) + 360) % 360
  const desiredMod = (180 - midDeg + 360) % 360
  let delta = desiredMod - normalized
  if (delta <= 0) delta += 360
  const extraTurns = 5 + Math.floor(midDeg / 120)
  return currentRotation + delta + extraTurns * 360
}

/**
 * Diámetro máximo 1:1 sin cortes.
 * `wheelCol` ya está bajo el header: solo reservamos puntero + botón + gaps dentro de esa columna.
 */
function computeWheelSize(opts: {
  stage: HTMLElement | null
  wheelCol: HTMLElement | null
  fullscreen: boolean
  adminOpen: boolean
}): number {
  if (typeof window === "undefined") return 560
  const { stage, wheelCol, fullscreen, adminOpen } = opts
  const vv = window.visualViewport
  const vw = vv?.width ?? window.innerWidth
  const vh = vv?.height ?? window.innerHeight
  const stageRect = stage?.getBoundingClientRect()
  const colRect = wheelCol?.getBoundingClientRect()

  const stageW = stageRect?.width || vw
  const stageH = stageRect?.height || vh

  const buttonH = fullscreen ? 72 : 90
  const pointerReserve = 42
  const gap = fullscreen ? 10 : 14
  const padX = fullscreen ? 12 : 20
  const legendReserve = !fullscreen && !adminOpen ? 12 : 0

  let availW = (colRect?.width && colRect.width > 40 ? colRect.width : stageW) - padX

  let availH: number
  if (colRect && colRect.height > 120) {
    availH = colRect.height - buttonH - pointerReserve - gap * 2 - legendReserve
  } else {
    const headerH = fullscreen ? 56 : 92
    availH = stageH - headerH - buttonH - pointerReserve - gap * 3 - legendReserve
  }

  const side = Math.floor(Math.min(availW, Math.max(availH, 200), Math.min(vw, vh) * 0.94))
  const minSide = 220
  const maxSide = fullscreen ? 1040 : 860
  return Math.max(minSide, Math.min(side, maxSide))
}

export default function RuletaQuillotanaPage() {
  const stageRef = useRef<HTMLDivElement>(null)
  const wheelColRef = useRef<HTMLDivElement>(null)
  const [config, setConfig] = useState<RuletaLocalConfig>(() => defaultConfig())
  const [hydrated, setHydrated] = useState(false)
  const [rotation, setRotation] = useState(0)
  const [spinning, setSpinning] = useState(false)
  const [durationMs, setDurationMs] = useState(6200)
  const [winner, setWinner] = useState<ActiveSlot | null>(null)
  const [showResult, setShowResult] = useState(false)
  const [spinCount, setSpinCount] = useState(0)
  const [isFullscreen, setIsFullscreen] = useState(false)
  const [wheelSize, setWheelSize] = useState(560)
  const [adminOpen, setAdminOpen] = useState(false)
  const pendingWinner = useRef<ActiveSlot | null>(null)
  const rotationRef = useRef(0)

  const slots = useMemo(() => buildActiveSlots(config), [config])

  useEffect(() => {
    setConfig(loadConfigFromStorage())
    setHydrated(true)
  }, [])

  useEffect(() => {
    if (!hydrated) return
    saveConfigToStorage(config)
  }, [config, hydrated])

  useEffect(() => {
    rotationRef.current = rotation
  }, [rotation])

  const refreshSize = useCallback(() => {
    setWheelSize(
      computeWheelSize({
        stage: stageRef.current,
        wheelCol: wheelColRef.current,
        fullscreen: isFullscreen,
        adminOpen,
      }),
    )
  }, [isFullscreen, adminOpen])

  useEffect(() => {
    refreshSize()
    const el = stageRef.current
    const col = wheelColRef.current
    const ro =
      typeof ResizeObserver !== "undefined" ? new ResizeObserver(() => refreshSize()) : null
    if (el && ro) ro.observe(el)
    if (col && ro) ro.observe(col)
    window.addEventListener("resize", refreshSize)
    window.addEventListener("orientationchange", refreshSize)
    window.visualViewport?.addEventListener("resize", refreshSize)
    return () => {
      ro?.disconnect()
      window.removeEventListener("resize", refreshSize)
      window.removeEventListener("orientationchange", refreshSize)
      window.visualViewport?.removeEventListener("resize", refreshSize)
    }
  }, [refreshSize])

  useEffect(() => {
    const onFs = () => {
      const fs = Boolean(document.fullscreenElement)
      setIsFullscreen(fs)
      if (fs) setAdminOpen(false)
    }
    document.addEventListener("fullscreenchange", onFs)
    return () => document.removeEventListener("fullscreenchange", onFs)
  }, [])

  const toggleFullscreen = useCallback(async () => {
    const el = stageRef.current
    if (!el) return
    try {
      if (!document.fullscreenElement) await el.requestFullscreen()
      else await document.exitFullscreen()
    } catch {
      /* Safari iPad */
    }
  }, [])

  const spin = useCallback(() => {
    if (spinning) return
    // Sorteo uniforme 1/9 por sector (CSPRNG). Sustituciones no alteran la probabilidad.
    const index = pickUniformSlotIndex(RULETA_SEGMENTS.length)
    const segment = slots[index]
    if (!segment) return
    pendingWinner.current = segment
    setWinner(null)
    setShowResult(false)
    const ms = randomSpinDurationMs()
    setDurationMs(ms)
    setSpinning(true)
    setRotation(targetRotationForMid(segment.midDeg, rotationRef.current))
    window.setTimeout(() => {
      setSpinning(false)
      setWinner(pendingWinner.current)
      setShowResult(true)
      setSpinCount((n) => n + 1)
    }, ms + 50)
  }, [spinning, slots])

  const clearResult = useCallback(() => {
    if (spinning) return
    setShowResult(false)
    setWinner(null)
    pendingWinner.current = null
  }, [spinning])

  const applyOverride = useCallback(
    (index: number, override: SlotOverride) => {
      if (spinning) return
      const base = RULETA_SEGMENTS[index]
      if (!base) return
      const label =
        override === "original"
          ? base.label
          : override === "RETRY"
            ? "¡Vuelve a girar!"
            : "¡Premio random!"
      if (
        !window.confirm(
          override === "original"
            ? `¿Restaurar casilla "${base.label}" a su premio original?`
            : `¿Sustituir "${base.label}" por "${label}"?\nEl tamaño de la casilla (40° · 1/9) no cambia.`,
        )
      ) {
        return
      }
      setConfig((prev) => {
        const overrides = [...prev.overrides]
        overrides[index] = override
        return { ...prev, overrides }
      })
    },
    [spinning],
  )

  const restoreOriginalRuleta = useCallback(() => {
    if (spinning) return
    if (
      !window.confirm(
        "¿Restaurar TODA la ruleta original (todas las casillas)? Esta acción no se puede deshacer.",
      )
    ) {
      return
    }
    clearConfigStorage()
    setConfig(defaultConfig())
    setShowResult(false)
    setWinner(null)
  }, [spinning])

  const showChrome = !isFullscreen

  return (
    <div
      ref={stageRef}
      className={cn(
        "relative bg-[#0b0612] text-amber-50",
        isFullscreen
          ? "flex h-dvh max-h-dvh flex-col overflow-hidden rounded-none"
          : "min-h-[calc(100dvh-4rem)] overflow-x-hidden rounded-xl",
      )}
      style={{
        paddingBottom: "env(safe-area-inset-bottom, 0px)",
        paddingTop: isFullscreen ? "env(safe-area-inset-top, 0px)" : undefined,
      }}
    >
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
        className={cn(
          "relative z-10 mx-auto flex w-full min-h-0 flex-1 flex-col",
          isFullscreen ? "max-w-none gap-2 px-3 py-2" : "max-w-7xl gap-4 px-3 py-4 sm:px-5 lg:px-6",
        )}
      >
        <header
          className={cn(
            "flex shrink-0 flex-wrap items-center justify-between gap-3",
            isFullscreen && "justify-center",
          )}
        >
          <div className={cn("flex items-center gap-3", isFullscreen && "gap-2")}>
            <img
              src={QUILLOTANA_LOGO_GRUPO_URL}
              alt="Grupo Quillotana"
              className={cn(
                "w-auto object-contain",
                isFullscreen ? "h-9 sm:h-11" : "h-12 sm:h-14",
              )}
              draggable={false}
            />
            {showChrome ? (
              <div>
                <p className="text-[10px] font-semibold uppercase tracking-[0.22em] text-amber-400/90">
                  Activación · Quillotana 1
                </p>
                <h1 className="font-serif text-2xl font-bold tracking-tight text-amber-50 sm:text-3xl">
                  Ruleta Quillotana
                </h1>
              </div>
            ) : null}
          </div>
          <div className="flex flex-wrap items-center gap-2">
            {showChrome ? (
              <Button
                type="button"
                variant="outline"
                size="sm"
                onClick={() => setAdminOpen((v) => !v)}
                disabled={spinning}
                className="border-amber-500/30 bg-black/30 text-amber-100/90 hover:bg-amber-500/10"
              >
                <Settings2 className="mr-1.5 h-4 w-4" />
                Administrar ruleta
              </Button>
            ) : null}
            <Button
              type="button"
              variant="outline"
              size="sm"
              onClick={toggleFullscreen}
              className="min-h-11 border-amber-500/40 bg-black/30 text-amber-100 hover:bg-amber-500/15"
            >
              {isFullscreen ? (
                <Minimize2 className="mr-1.5 h-4 w-4" />
              ) : (
                <Maximize2 className="mr-1.5 h-4 w-4" />
              )}
              {isFullscreen ? "Salir" : "Pantalla completa"}
            </Button>
            {showChrome ? (
              <Button
                type="button"
                variant="outline"
                size="sm"
                onClick={clearResult}
                disabled={spinning || !winner}
                className="border-amber-500/40 bg-black/30 text-amber-100 disabled:opacity-40"
              >
                <RotateCcw className="mr-1.5 h-4 w-4" />
                Reiniciar
              </Button>
            ) : null}
          </div>
        </header>

        <div
          className={cn(
            "grid min-h-0 flex-1 items-stretch gap-4",
            showChrome && adminOpen
              ? "lg:grid-cols-[minmax(0,1fr)_minmax(280px,360px)]"
              : "grid-cols-1",
          )}
        >
          <div
            ref={wheelColRef}
            className="flex min-h-0 min-w-0 flex-col items-center justify-center gap-3 overflow-hidden"
          >
            <RuletaWheel
              slots={slots}
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
                "h-14 shrink-0 min-w-[240px] rounded-full px-10 text-lg font-bold tracking-wide shadow-lg sm:h-16 sm:min-w-[300px]",
                "bg-gradient-to-b from-amber-400 to-orange-600 text-[#1a0a08]",
                "hover:from-amber-300 hover:to-orange-500",
                "disabled:cursor-not-allowed disabled:opacity-60",
                spinning && "animate-pulse",
              )}
            >
              <Sparkles className="mr-2 h-5 w-5" />
              {spinning ? "Girando…" : "GIRAR RULETA"}
            </Button>
            {showChrome ? (
              <p className="shrink-0 text-xs text-amber-100/50">
                Giros en esta sesión: {spinCount} · cada casilla 40° (probabilidad 1/9)
                {spinning ? " · espera a que se detenga" : ""}
              </p>
            ) : null}

            {showChrome && !adminOpen ? (
              <div className="w-full max-w-3xl shrink-0 rounded-2xl border border-white/10 bg-black/25 p-3">
                <p className="mb-2 text-[10px] font-semibold uppercase tracking-[0.18em] text-amber-400/70">
                  Leyenda
                </p>
                <ul className="grid grid-cols-1 gap-1.5 sm:grid-cols-2 lg:grid-cols-3">
                  {slots.map((s) => (
                    <li
                      key={s.index}
                      className="flex items-center gap-2 rounded-lg bg-white/5 px-2 py-1.5 text-sm"
                    >
                      <span
                        className="h-3 w-3 shrink-0 rounded-full"
                        style={{ backgroundColor: s.displayColor }}
                      />
                      <span className="min-w-0 flex-1 truncate text-amber-50/90">
                        {s.displayLabel}
                      </span>
                    </li>
                  ))}
                </ul>
              </div>
            ) : null}
          </div>

          {showChrome && adminOpen ? (
            <aside className="flex max-h-[min(70dvh,720px)] min-h-0 flex-col overflow-hidden rounded-2xl border border-amber-500/25 bg-black/45 p-4 backdrop-blur-sm lg:max-h-[calc(100dvh-10rem)]">
              <h2 className="shrink-0 text-sm font-bold uppercase tracking-[0.16em] text-amber-300">
                Administración
              </h2>
              <p className="mt-1 shrink-0 text-xs text-amber-100/60">
                Sustituye premios agotados. Las 9 casillas siguen midiendo 40° y 1/9 cada una.
                Cambios locales (localStorage).
              </p>

              <div className="mt-4 min-h-0 flex-1 space-y-3 overflow-y-auto overscroll-contain pr-1">
                {slots.map((slot) => (
                  <div
                    key={slot.index}
                    className="rounded-xl border border-white/10 bg-white/5 p-3 text-sm"
                  >
                    <p className="font-semibold text-amber-50">
                      Casilla {slot.index + 1}: {slot.label}
                    </p>
                    <p className="text-xs text-amber-100/55">
                      Actual: <strong className="text-amber-100">{slot.displayLabel}</strong>
                    </p>
                    <div className="mt-2 flex flex-col gap-1.5">
                      {(
                        [
                          ["original", "Mantener / restaurar original"],
                          ["RETRY", "Sustituir por ¡Vuelve a girar!"],
                          ["RANDOM", "Sustituir por ¡Premio random!"],
                        ] as const
                      ).map(([value, label]) => (
                        <Button
                          key={value}
                          type="button"
                          size="sm"
                          variant="outline"
                          disabled={spinning || slot.override === value}
                          onClick={() => applyOverride(slot.index, value)}
                          className="h-10 justify-start border-amber-500/25 bg-black/30 text-left text-amber-100"
                        >
                          {label}
                        </Button>
                      ))}
                    </div>
                  </div>
                ))}
              </div>

              <Button
                type="button"
                variant="destructive"
                disabled={spinning}
                onClick={restoreOriginalRuleta}
                className="mt-4 w-full shrink-0"
              >
                Restaurar ruleta original
              </Button>
            </aside>
          ) : null}
        </div>
      </div>

      {winner && showResult ? (
        <RuletaResultModal slot={winner} open={showResult} onContinue={clearResult} />
      ) : null}
    </div>
  )
}
