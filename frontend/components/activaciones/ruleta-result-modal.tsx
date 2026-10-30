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
      Array.from({ length: 48 }, (_, i) => ({
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
          0% { opacity: 0; transform: scale(0.86) translateY(12px); }
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
        ? "¡Sigue participando!"
        : tone === "random"
          ? "🎉 ¡FELICIDADES!"
          : "🎉 ¡FELICIDADES!"

  const subtitle =
    tone === "none"
      ? "Esta vez no ganaste, ¡gracias por jugar!"
      : tone === "retry"
        ? "¡Vuelve a girar! Tienes otra oportunidad."
        : tone === "random"
          ? "¡GANASTE UN PREMIO RANDOM!"
          : `¡GANASTE ${slot.displayLabel.toUpperCase()}!`

  const showConfetti = tone === "win" || tone === "random"

  return (
    <div
      className="absolute inset-0 z-40 flex items-center justify-center p-4"
      role="dialog"
      aria-modal="true"
      aria-labelledby="ruleta-result-title"
    >
      <button
        type="button"
        className="absolute inset-0 bg-black/65 backdrop-blur-[2px]"
        aria-label="Cerrar resultado"
        onClick={onContinue}
      />
      {showConfetti ? <ConfettiBurst /> : null}
      <div
        className={cn(
          "relative z-10 w-full max-w-xl rounded-3xl border-2 p-6 text-center shadow-2xl sm:p-10",
          "bg-gradient-to-b from-[#1a1028] to-[#0d0814]",
        )}
        style={{
          borderColor: slot.color,
          animation: "ruleta-modal-in 0.45s cubic-bezier(0.2, 0.9, 0.2, 1)",
        }}
      >
        <p
          id="ruleta-result-title"
          className="text-sm font-bold uppercase tracking-[0.25em] text-amber-300/90 sm:text-base"
        >
          {title}
        </p>
        <p
          className="mt-4 font-serif text-3xl font-black leading-tight text-amber-50 sm:text-5xl"
          style={{ textShadow: "0 2px 24px rgba(0,0,0,0.45)" }}
        >
          {subtitle}
        </p>
        <div
          className="mx-auto mt-5 inline-flex max-w-full items-center justify-center rounded-full px-5 py-2 text-sm font-bold uppercase tracking-wide sm:text-base"
          style={{ backgroundColor: slot.color, color: slot.textColor }}
        >
          {slot.displayLabel}
        </div>
        {tone === "random" ? (
          <p className="mt-4 text-sm text-amber-100/70">
            El encargado entregará el regalo físico según el stock disponible.
          </p>
        ) : null}
        <Button
          type="button"
          size="lg"
          onClick={onContinue}
          className={cn(
            "mt-8 h-14 min-w-[200px] rounded-full px-10 text-base font-bold",
            "bg-gradient-to-b from-amber-400 to-orange-600 text-[#1a0a08]",
            "hover:from-amber-300 hover:to-orange-500",
          )}
        >
          CONTINUAR
        </Button>
      </div>
    </div>
  )
}
