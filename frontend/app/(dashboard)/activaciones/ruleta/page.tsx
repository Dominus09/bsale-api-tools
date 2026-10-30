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
  pickWeightedSlotIndex,
  probabilitiesAreValid,
  saveConfigToStorage,
  sumProbabilities,
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

/** Lleva el midDeg del sector bajo el puntero superior, siempre avanzando. */
function targetRotationForMid(midDeg: number, currentRotation: number): number {
  const normalized = ((currentRotation % 360) + 360) % 360
  const desiredMod = (360 - midDeg) % 360
  let delta = desiredMod - normalized
  if (delta <= 0) delta += 360
  const extraTurns = 5 + Math.floor(midDeg / 120)
  return currentRotation + delta + extraTurns * 360
}

function computeWheelSize(stage: HTMLElement | null, fullscreen: boolean): number {
  if (typeof window === "undefined") return 520
  const vw = window.innerWidth
  const vh = window.innerHeight
  const rect = stage?.getBoundingClientRect()
  const availW = rect?.width ? rect.width - 24 : vw - 32
  const headerReserve = fullscreen ? vh * 0.14 : vh * 0.28
  const availH = Math.max(280, (rect?.height || vh) - headerReserve)
  if (fullscreen) {
    return Math.floor(Math.min(availW * 0.92, availH * 0.88, Math.min(vw, vh) * 0.86))
  }
  if (vw >= 1100) {
    return Math.floor(Math.min(availW * 0.78, availH * 0.82, 720))
  }
  if (vw >= 768) {
    // iPad / tablet
    return Math.floor(Math.min(availW * 0.9, availH * 0.72, 640))
  }
  return Math.floor(Math.min(availW * 0.96, availH * 0.62, 420))
}

export default function RuletaQuillotanaPage() {
  const stageRef = useRef<HTMLDivElement>(null)
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
  const [draftProbs, setDraftProbs] = useState<number[]>(() => defaultConfig().probabilities)
  const pendingWinner = useRef<ActiveSlot | null>(null)
  const rotationRef = useRef(0)

  const slots = useMemo(() => buildActiveSlots(config), [config])

  useEffect(() => {
    const loaded = loadConfigFromStorage()
    setConfig(loaded)
    setDraftProbs(loaded.probabilities)
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
    setWheelSize(computeWheelSize(stageRef.current, isFullscreen))
  }, [isFullscreen])

  useEffect(() => {
    refreshSize()
    const el = stageRef.current
    const ro = typeof ResizeObserver !== "undefined" && el ? new ResizeObserver(() => refreshSize()) : null
    ro?.observe(el!)
    window.addEventListener("resize", refreshSize)
    window.addEventListener("orientationchange", refreshSize)
    return () => {
      ro?.disconnect()
      window.removeEventListener("resize", refreshSize)
      window.removeEventListener("orientationchange", refreshSize)
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
      /* Safari iPad puede restringir; la UI sigue usable */
    }
  }, [])

  const spin = useCallback(() => {
    if (spinning) return
    if (!probabilitiesAreValid(config.probabilities)) return
    const index = pickWeightedSlotIndex(config.probabilities)
    const segment = slots[index]
    if (!segment || segment.spanDeg <= 0) return
    pendingWinner.current = segment
    setWinner(null)
    setShowResult(false)
    const ms = randomSpinDurationMs()
    setDurationMs(ms)
    setSpinning(true)
    const next = targetRotationForMid(segment.midDeg, rotationRef.current)
    setRotation(next)
    window.setTimeout(() => {
      setSpinning(false)
      setWinner(pendingWinner.current)
      setShowResult(true)
      setSpinCount((n) => n + 1)
    }, ms + 50)
  }, [spinning, config.probabilities, slots])

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
            : `¿Sustituir "${base.label}" por "${label}"?`,
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

  const applyProbabilities = useCallback(() => {
    if (spinning) return
    if (!probabilitiesAreValid(draftProbs)) {
      window.alert("Las probabilidades deben ser ≥ 0 y sumar exactamente 100%.")
      return
    }
    setConfig((prev) => ({ ...prev, probabilities: [...draftProbs] }))
  }, [draftProbs, spinning])

  const restoreOriginalRuleta = useCallback(() => {
    if (spinning) return
    if (
      !window.confirm(
        "¿Restaurar TODA la ruleta original (premios y probabilidades)? Esta acción no se puede deshacer.",
      )
    ) {
      return
    }
    const fresh = defaultConfig()
    clearConfigStorage()
    setConfig(fresh)
    setDraftProbs(fresh.probabilities)
    setShowResult(false)
    setWinner(null)
  }, [spinning])

  const probSum = sumProbabilities(draftProbs)
  const showChrome = !isFullscreen

  return (
    <div
      ref={stageRef}
      className={cn(
        "relative overflow-hidden bg-[#0b0612] text-amber-50",
        isFullscreen ? "min-h-dvh rounded-none" : "min-h-[calc(100dvh-4rem)] rounded-xl",
      )}
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
          "relative z-10 mx-auto flex w-full flex-col",
          isFullscreen ? "max-w-none gap-3 px-3 py-3" : "max-w-7xl gap-4 px-3 py-4 sm:px-5 lg:px-6",
        )}
      >
        <header
          className={cn(
            "flex flex-wrap items-center justify-between gap-3",
            isFullscreen && "justify-center",
          )}
        >
          <div className={cn("flex items-center gap-3", isFullscreen && "gap-2")}>
            <img
              src={QUILLOTANA_LOGO_GRUPO_URL}
              alt="Grupo Quillotana"
              className={cn("w-auto object-contain", isFullscreen ? "h-10 sm:h-12" : "h-12 sm:h-14")}
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
            "grid items-start gap-4",
            showChrome && adminOpen
              ? "lg:grid-cols-[minmax(0,1fr)_minmax(300px,380px)]"
              : "grid-cols-1",
          )}
        >
          <div className="flex min-w-0 flex-col items-center gap-4">
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
              disabled={spinning || !probabilitiesAreValid(config.probabilities)}
              onClick={spin}
              className={cn(
                "h-16 min-w-[260px] rounded-full px-12 text-lg font-bold tracking-wide shadow-lg sm:min-w-[300px]",
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
              <p className="text-xs text-amber-100/50">
                Giros en esta sesión: {spinCount}
                {spinning ? " · espera a que se detenga" : ""}
              </p>
            ) : null}

            {showChrome ? (
              <div className="w-full max-w-3xl rounded-2xl border border-white/10 bg-black/25 p-3">
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
                        style={{ backgroundColor: s.color }}
                      />
                      <span className="min-w-0 flex-1 truncate text-amber-50/90">
                        {s.displayLabel}
                      </span>
                      <span className="shrink-0 text-xs text-amber-100/50">{s.probability}%</span>
                    </li>
                  ))}
                </ul>
              </div>
            ) : null}
          </div>

          {showChrome && adminOpen ? (
            <aside className="rounded-2xl border border-amber-500/25 bg-black/45 p-4 backdrop-blur-sm">
              <h2 className="text-sm font-bold uppercase tracking-[0.16em] text-amber-300">
                Administración
              </h2>
              <p className="mt-1 text-xs text-amber-100/60">
                Cambios locales a este navegador (localStorage). No afectan otros equipos.
              </p>

              <div className="mt-4 space-y-3">
                <p className="text-[11px] font-semibold uppercase tracking-wide text-amber-400/80">
                  Probabilidades (deben sumar 100%)
                </p>
                {RULETA_SEGMENTS.map((base, i) => (
                  <label
                    key={base.code}
                    className="flex items-center justify-between gap-2 text-sm"
                  >
                    <span className="truncate text-amber-50/85">{base.label}</span>
                    <input
                      type="number"
                      min={0}
                      max={100}
                      step={1}
                      disabled={spinning}
                      value={draftProbs[i] ?? 0}
                      onChange={(e) => {
                        const next = [...draftProbs]
                        next[i] = Number(e.target.value)
                        setDraftProbs(next)
                      }}
                      className="h-10 w-20 rounded-lg border border-amber-500/30 bg-black/40 px-2 text-right text-amber-50"
                    />
                  </label>
                ))}
                <p
                  className={cn(
                    "text-xs font-semibold",
                    Math.abs(probSum - 100) < 0.001 ? "text-emerald-400" : "text-rose-400",
                  )}
                >
                  Suma actual: {probSum}%
                </p>
                <Button
                  type="button"
                  size="sm"
                  disabled={spinning || !probabilitiesAreValid(draftProbs)}
                  onClick={applyProbabilities}
                  className="w-full bg-amber-500/90 text-[#1a0a08] hover:bg-amber-400"
                >
                  Aplicar probabilidades
                </Button>
              </div>

              <div className="mt-6 space-y-3 border-t border-white/10 pt-4">
                <p className="text-[11px] font-semibold uppercase tracking-wide text-amber-400/80">
                  Sustituir casillas
                </p>
                {slots.map((slot) => (
                  <div
                    key={slot.index}
                    className="rounded-xl border border-white/10 bg-white/5 p-3 text-sm"
                  >
                    <p className="font-semibold text-amber-50">{slot.label}</p>
                    <p className="text-xs text-amber-100/55">
                      Actual: <strong className="text-amber-100">{slot.displayLabel}</strong> ·{" "}
                      {slot.probability}%
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
                className="mt-4 w-full"
              >
                Restaurar ruleta original
              </Button>
            </aside>
          ) : null}
        </div>
      </div>

      {winner && showResult ? (
        <RuletaResultModal
          slot={winner}
          open={showResult}
          onContinue={clearResult}
        />
      ) : null}
    </div>
  )
}
