"use client"

import { AppLink } from "@/components/app-link"
import type { RouteProgress } from "@/lib/api"

type Variant = "full" | "compact" | "overlay"

type Props = {
  progress: RouteProgress | null | undefined
  variant?: Variant
  nextHref?: string | null
  className?: string
}

export function RouteProgressCard({
  progress,
  variant = "full",
  nextHref,
  className = "",
}: Props) {
  if (!progress) return null

  const total = progress.total_clients ?? 0
  const managed = progress.managed_clients ?? 0
  const remaining = progress.remaining_clients ?? Math.max(0, total - managed)
  const percent = typeof progress.percent === "number" ? progress.percent : 0
  const by = progress.by_result || {
    completed: 0,
    partial: 0,
    not_delivered: 0,
    pending: 0,
    in_progress: 0,
    pending_review: 0,
  }
  const next = progress.next_customer

  if (variant === "overlay") {
    return (
      <div className={`route-progress-overlay ${className}`.trim()} role="status">
        {managed}/{total} clientes · {percent}%
      </div>
    )
  }

  if (variant === "compact") {
    return (
      <div className={`route-progress-compact ${className}`.trim()} role="status">
        <div className="route-progress-compact-row">
          <span>
            {managed} de {total} gestionados
          </span>
          <strong>{percent}%</strong>
        </div>
        <div
          className="route-progress-bar"
          role="progressbar"
          aria-valuemin={0}
          aria-valuemax={100}
          aria-valuenow={Math.round(percent)}
          aria-label={`Avance del recorrido ${percent} por ciento`}
        >
          <div
            className="route-progress-bar-fill"
            style={{ width: `${Math.min(100, Math.max(0, percent))}%` }}
          />
        </div>
        <p className="muted" style={{ margin: "6px 0 0", fontSize: "0.78rem" }}>
          Faltan {remaining} cliente{remaining === 1 ? "" : "s"}
        </p>
      </div>
    )
  }

  return (
    <div className={`card route-progress-card ${className}`.trim()}>
      <div className="despacho-hero-head">
        <h3 style={{ margin: 0, fontSize: "0.95rem", letterSpacing: "0.04em" }}>
          AVANCE DEL RECORRIDO
        </h3>
        <strong style={{ color: "var(--accent)", fontSize: "1.05rem" }}>
          {percent}%
        </strong>
      </div>

      <div
        className="route-progress-bar route-progress-bar-lg"
        role="progressbar"
        aria-valuemin={0}
        aria-valuemax={100}
        aria-valuenow={Math.round(percent)}
        aria-label={`Avance del recorrido ${percent} por ciento`}
      >
        <div
          className="route-progress-bar-fill"
          style={{ width: `${Math.min(100, Math.max(0, percent))}%` }}
        />
      </div>

      <p style={{ margin: "10px 0 4px", fontWeight: 650, fontSize: "0.92rem" }}>
        {managed} de {total} clientes gestionados
      </p>
      <p className="muted" style={{ margin: 0, fontSize: "0.82rem" }}>
        Faltan {remaining} cliente{remaining === 1 ? "" : "s"}
      </p>

      <ul className="route-progress-sem" aria-label="Resultado de entregas">
        <li>
          <span className="sem-dot sem-dot-green" aria-hidden />
          {by.completed} completa{by.completed === 1 ? "" : "s"}
        </li>
        <li>
          <span className="sem-dot sem-dot-yellow" aria-hidden />
          {by.partial} parcial{by.partial === 1 ? "" : "es"}
        </li>
        <li>
          <span className="sem-dot sem-dot-red" aria-hidden />
          {by.not_delivered} no realizada{by.not_delivered === 1 ? "" : "s"}
        </li>
        {(by.pending_review || 0) > 0 ? (
          <li>
            <span className="sem-dot sem-dot-neutral" aria-hidden />
            {by.pending_review} pendiente{by.pending_review === 1 ? "" : "s"} de
            revisión GPS
          </li>
        ) : null}
        {(by.in_progress || 0) > 0 ? (
          <li className="muted">
            {by.in_progress} en atención (no suma al avance)
          </li>
        ) : null}
      </ul>

      {next ? (
        <div className="route-progress-next">
          <div>
            <span className="muted" style={{ fontSize: "0.72rem" }}>
              Siguiente · {next.suggestion_reason_es || "Siguiente en ruta"}
            </span>
            <div style={{ fontWeight: 650, marginTop: 2 }}>
              {next.client_name || next.customer_key}
            </div>
          </div>
          {nextHref ? (
            <AppLink href={nextHref} className="btn btn-primary btn-compact">
              Ir
            </AppLink>
          ) : null}
        </div>
      ) : remaining === 0 && total > 0 ? (
        <p className="muted" style={{ margin: "10px 0 0", fontSize: "0.85rem" }}>
          Recorrido gestionado
        </p>
      ) : null}
    </div>
  )
}
