"use client"

import Link from "next/link"
import { STEP_DEFS, type DeliveryStep } from "@/lib/delivery-flow"

type Props = {
  clientName: string
  step: DeliveryStep
  title: string
  backHref: string
  children: React.ReactNode
  footer?: React.ReactNode
}

export function DeliveryStepChrome({
  clientName,
  step,
  title,
  backHref,
  children,
  footer,
}: Props) {
  return (
    <div className="step-chrome">
      <header className="step-chrome-head card">
        <div className="step-chrome-top">
          <Link href={backHref} className="btn btn-ghost btn-compact">
            ← Volver
          </Link>
          <div className="step-chrome-client">
            <strong>{clientName || "Cliente"}</strong>
            <span className="muted" style={{ fontSize: "0.8rem" }}>
              {title}
            </span>
          </div>
        </div>
        <nav className="step-pills" aria-label="Pasos de entrega">
          {STEP_DEFS.map((s) => (
            <span
              key={s.step}
              className={`pill${s.step === step ? " pill-active" : s.step < step ? " pill-ok" : ""}`}
              aria-current={s.step === step ? "step" : undefined}
            >
              {s.step} {s.label}
            </span>
          ))}
        </nav>
      </header>

      <div className="stack-gap">{children}</div>

      {footer ? <footer className="step-chrome-footer">{footer}</footer> : null}
    </div>
  )
}
