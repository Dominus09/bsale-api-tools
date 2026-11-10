"use client"

/**
 * Modo prueba familiar — mensajes ficticios para evaluar el aspecto en WhatsApp.
 * No escribe operaciones productivas ni incluye enlaces de invitación.
 */

import { useEffect, useState } from "react"
import Link from "next/link"
import { ApiError, listDemoWhatsappReports } from "@/lib/api"
import { WhatsappSharePanel } from "@/components/whatsapp-share-panel"

type Scenario = Awaited<ReturnType<typeof listDemoWhatsappReports>>["scenarios"][number]

export default function DemoWhatsappReportsPage() {
  const [scenarios, setScenarios] = useState<Scenario[] | null>(null)
  const [note, setNote] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [active, setActive] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    async function load() {
      try {
        const res = await listDemoWhatsappReports()
        if (cancelled) return
        setScenarios(res.scenarios || [])
        setNote(res.note_es || null)
        setActive(res.scenarios?.[0]?.scenario_key || null)
      } catch (e) {
        if (cancelled) return
        setError(e instanceof ApiError ? e.message : "No se pudieron cargar demos")
        setScenarios([])
      }
    }
    void load()
    return () => {
      cancelled = true
    }
  }, [])

  const current = scenarios?.find((s) => s.scenario_key === active) || null

  return (
    <div className="stack-gap">
      <Link href="/despachos" className="muted">
        ← Mis despachos
      </Link>

      <div className="card">
        <h2 style={{ margin: "0 0 6px", fontSize: "1.1rem" }}>
          🧪 Prueba — reportes WhatsApp
        </h2>
        <p className="muted" style={{ margin: 0, fontSize: "0.85rem" }}>
          DATOS FICTICIOS — NO CORRESPONDE A UNA ENTREGA REAL. El botón abre el grupo
          familiar de prueba con el mensaje precargado; confirme Enviar en WhatsApp.
        </p>
      </div>

      {note ? (
        <div className="banner banner-info" style={{ fontSize: "0.82rem" }}>
          {note}
        </div>
      ) : null}
      {error ? <div className="banner banner-error">{error}</div> : null}

      {!scenarios ? (
        <div className="empty">Cargando escenarios…</div>
      ) : (
        <>
          <div className="stack-gap">
            {scenarios.map((s) => (
              <button
                key={s.scenario_key}
                type="button"
                className={`drawer-item${active === s.scenario_key ? " pill-ok" : ""}`}
                onClick={() => setActive(s.scenario_key)}
              >
                {s.title_es}
              </button>
            ))}
          </div>

          {current ? (
            <div className="card">
              <WhatsappSharePanel
                reportText={current.report_text}
                share={current.share}
                header={current.header}
                demo
                onGenerate={async () => {
                  /* ya viene generado */
                }}
              />
            </div>
          ) : null}
        </>
      )}
    </div>
  )
}
