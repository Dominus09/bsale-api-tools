"use client"

import { useEffect, useMemo } from "react"

import { Button } from "@/components/ui/button"
import { cn } from "@/lib/utils"

import type { ActiveSlot } from "./ruleta-q1-segments"
import { winnerTone } from "./ruleta-q1-segments"

type Props = {
  slot: ActiveSlot
  open: boolean
  onContinue: () => void
}

function ConfettiBurst() {
  const pieces = useMemo(
    () =>
      Array.from({ length: 56 }, (_, i) => ({
        id: i,
        left: `${(i * 17 + 7) % 100}%`,
        delay: `${(i % 12) * 0.05}s`,
        duration: `${1.6 + (i % 5) * 0.25}s`,
        color: ["#f59e0b", "#ef4444", "#a855f7", "#22c55e", "#38bdf8", "#f472b6"][i % 6],
        rot: `${(i * 47) % 360}deg`,
      })),
    [],
  )
  return (
    <div className="pointer-events-none absolute inset-0 overflow-hidden" aria-hidden>
      {pieces.map((p) => (
        <span
          key={p.id}
          className="absolute top-0 h-2.5 w-2 rounded-sm opacity-90"
          style={{
            left: p.left,
            background: p.color,
            transform: `rotate(${p.rot})`,
            animation: `ruleta-confetti-fall ${p.duration} ease-in ${p.delay} forwards`,
          }}
        />
      ))}
      <style>{`
        @keyframes ruleta-confetti-fall {
          0% { transform: translateY(-10%) rotate(0deg); opacity: 1; }
          100% { transform: translateY(110vh) rotate(720deg); opacity: 0; }
        }
        @keyframes ruleta-modal-in {
          0% { opacity: 0; transform: scale(0.88) translateY(14px); }
          100% { opacity: 1; transform: scale(1) translateY(0); }
        }
      `}</style>
    </div>
  )
}

export function RuletaResultModal({ slot, open, onContinue }: Props) {
  const tone = winnerTone(slot)

  useEffect(() => {
    if (!open) return
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape" || e.key === "Enter") onContinue()
    }
    window.addEventListener("keydown", onKey)
    return () => window.removeEventListener("keydown", onKey)
  }, [open, onContinue])

  if (!open) return null

  const title =
    tone === "none"
      ? "¡Casi!"
      : tone === "retry"
        ? "¡Otra oportunidad!"
        : "🎉 ¡FELICIDADES!"

  const headline =
    tone === "none"
      ? "¡Esta vez no ganaste!"
      : tone === "retry"
        ? "¡Vuelve a girar!"
        : tone === "random"
          ? "¡Premio random!"
          : slot.displayLabel

  const support =
    tone === "none"
      ? "Gracias por participar. ¡Suerte la próxima!"
      : tone === "retry"
        ? "Puedes girar de nuevo ahora mismo."
        : tone === "random"
          ? "El encargado entregará el regalo según el stock disponible."
          : "¡Disfruta tu premio Quillotana!"

  const showConfetti = tone === "win" || tone === "random"
  const continueLabel = tone === "retry" ? "GIRAR DE NUEVO" : "CONTINUAR"

  return (
    <div
      className="absolute inset-0 z-40 flex items-center justify-center p-4 sm:p-6"
      role="dialog"
      aria-modal="true"
      aria-labelledby="ruleta-result-title"
    >
      <button
        type="button"
        className="absolute inset-0 bg-black/70 backdrop-blur-[2px]"
        aria-label="Cerrar resultado"
        onClick={onContinue}
      />
      {showConfetti ? <ConfettiBurst /> : null}
      <div
        className={cn(
          "relative z-10 w-full max-w-2xl rounded-[2rem] border-2 p-7 text-center shadow-2xl sm:p-12",
          "bg-gradient-to-b from-[#1a1028] to-[#0d0814]",
        )}
        style={{
          borderColor: slot.displayColor,
          animation: "ruleta-modal-in 0.45s cubic-bezier(0.2, 0.9, 0.2, 1)",
        }}
      >
        <p
          id="ruleta-result-title"
          className="text-sm font-bold uppercase tracking-[0.28em] text-amber-300/90 sm:text-base"
        >
          {title}
        </p>
        <p
          className="mt-5 font-serif text-4xl font-black leading-[1.05] text-amber-50 sm:text-6xl"
          style={{ textShadow: "0 2px 28px rgba(0,0,0,0.45)" }}
        >
          {headline}
        </p>
        <div
          className="mx-auto mt-6 inline-flex max-w-full items-center justify-center rounded-full px-6 py-2.5 text-sm font-bold uppercase tracking-wide sm:text-base"
          style={{ backgroundColor: slot.displayColor, color: slot.displayTextColor }}
        >
          {slot.displayLabel}
        </div>
        <p className="mx-auto mt-5 max-w-md text-base text-amber-100/75 sm:text-lg">{support}</p>
        <Button
          type="button"
          size="lg"
          onClick={onContinue}
          className={cn(
            "mt-9 h-14 min-w-[220px] rounded-full px-12 text-base font-bold sm:h-16 sm:text-lg",
            "bg-gradient-to-b from-amber-400 to-orange-600 text-[#1a0a08]",
            "hover:from-amber-300 hover:to-orange-500",
          )}
        >
          {continueLabel}
        </Button>
      </div>
    </div>
  )
}
