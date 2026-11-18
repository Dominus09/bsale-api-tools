"use client"

import Link from "next/link"
import { useRouter } from "next/navigation"
import { useMemo, useState } from "react"
import { DeliveryStepChrome } from "@/components/delivery-step-chrome"
import {
  ApiError,
  confirmVisitDocumentDelivery,
} from "@/lib/api"
import {
  buildEntregaHref,
  docKeyFromDocument,
  moneyCLP,
} from "@/lib/delivery-flow"
import {
  commercialProductLabel,
  productBarcodeLabel,
  productCategoryLabel,
} from "@/lib/delivery-labels"
import { breakdownBoxUnits } from "@/lib/box-units"
import { useDeliveryParams } from "@/lib/use-delivery-params"
import { useVisitSession } from "@/lib/use-visit-session"

type Mode = "simple" | "detailed"

function docLineProgress(
  doc: {
    related_document_id?: number | null
    document_number?: number | null
    products?: Array<{ source_detail_id?: number | null }>
    quantities_registrable?: boolean
    line_identity?: string
  },
  visitDocs: Array<Record<string, unknown>>,
  visitLines: Array<Record<string, unknown>>,
): {
  registrable: boolean
  expected: number
  registered: number
  pending: number
  complete: boolean
  label: string
  ok: boolean
  resultStatus: string
} {
  const key = docKeyFromDocument(doc)
  const expectedIds = (doc.products || [])
    .filter((p) => p.source_detail_id != null)
    .map((p) => Number(p.source_detail_id))
  const ambiguous =
    doc.line_identity === "ambiguous_display_only" ||
    (doc.products || []).some((p) => p.source_detail_id == null)
  const registrable =
    doc.quantities_registrable !== false && expectedIds.length > 0 && !ambiguous
  const docMeta = visitDocs.find(
    (d) => String(d.related_document_id ?? d.document_number ?? "") === key,
  )
  const resultStatus = String(docMeta?.result_status || "")
  if (!registrable) {
    return {
      registrable: false,
      expected: 0,
      registered: 0,
      pending: 0,
      complete: true,
      label: "Solo lectura",
      ok: false,
      resultStatus,
    }
  }
  const docResId = docMeta?.id != null ? String(docMeta.id) : null
  const registeredIds = new Set(
    (visitLines || [])
      .filter((ln) => docResId && String(ln.document_result_id ?? "") === docResId)
      .map((ln) => Number(ln.source_detail_id))
      .filter((n) => Number.isFinite(n)),
  )
  const registered = expectedIds.filter((id) => registeredIds.has(id)).length
  const pending = Math.max(0, expectedIds.length - registered)
  const complete = pending === 0
  let label = "Pendiente"
  if (complete) {
    if (resultStatus === "full") label = "Completo"
    else if (resultStatus === "partial") label = "Parcial"
    else if (resultStatus === "none") label = "Sin entrega"
    else label = "Registrado"
  } else if (registered > 0) {
    label = "En registro"
  }
  return {
    registrable: true,
    expected: expectedIds.length,
    registered,
    pending,
    complete,
    label,
    ok: complete,
    resultStatus,
  }
}

export default function EntregaDocumentosPage() {
  const { planId, customerKey } = useDeliveryParams()
  const router = useRouter()
  const session = useVisitSession(planId, customerKey)
  const fichaHref = buildEntregaHref(planId, customerKey, "ficha")
  const [gateErr, setGateErr] = useState<string | null>(null)
  const [mode, setMode] = useState<Mode>("simple")
  const [busyKey, setBusyKey] = useState<string | null>(null)
  const [confirmKey, setConfirmKey] = useState<string | null>(null)
  const [partialKey, setPartialKey] = useState<string | null>(null)
  const [partialNote, setPartialNote] = useState("")
  /** Productos con diferencia: cantidad REAL entregada (no forzar 0). */
  const [diffDelivered, setDiffDelivered] = useState<Record<number, string>>({})
  const [productQ, setProductQ] = useState("")
  const [expandedKey, setExpandedKey] = useState<string | null>(null)

  const savedDocs = session.visitDetail?.documents || []
  const savedLines = session.visitDetail?.lines || []
  const visitVersion =
    typeof session.visitDetail?.visit?.version === "number"
      ? session.visitDetail.visit.version
      : undefined

  const perDoc = useMemo(
    () =>
      session.documents.map((doc) => ({
        doc,
        prog: docLineProgress(doc, savedDocs, savedLines),
      })),
    [session.documents, savedDocs, savedLines],
  )

  const missingTotal = perDoc.reduce(
    (acc, x) => acc + (x.prog.registrable ? x.prog.pending : 0),
    0,
  )
  const allSaved = perDoc.length > 0 && missingTotal === 0

  async function confirmFull(doc: (typeof session.documents)[0]) {
    if (!session.activeVisit || doc.related_document_id == null) return
    if (busyKey) return
    const key = docKeyFromDocument(doc)
    if (
      !window.confirm(
        `¿Confirma entrega COMPLETA de ${doc.document_type || "Doc"} ${doc.document_number ?? ""}?\nSe registrarán todas las cantidades del pedido.`,
      )
    ) {
      return
    }
    setBusyKey(key)
    setConfirmKey(key)
    setGateErr(null)
    try {
      await confirmVisitDocumentDelivery(planId, session.activeVisit.id, {
        related_document_id: Number(doc.related_document_id),
        mode: "full",
        oc_document_id: doc.oc_document_id ?? null,
        document_number: doc.document_number ?? null,
        document_type: doc.document_type ?? null,
        document_total:
          typeof doc.document_total === "number" ? doc.document_total : null,
        expected_version: visitVersion ?? null,
        notes: "Confirmación masiva modo simple",
      })
      setConfirmKey(null)
      void session.softReloadVisitDetail()
    } catch (e) {
      setGateErr(e instanceof ApiError ? e.message : "No se pudo confirmar")
    } finally {
      setBusyKey(null)
    }
  }

  async function confirmPartial(doc: (typeof session.documents)[0]) {
    if (!session.activeVisit || doc.related_document_id == null) return
    if (busyKey) return
    const key = docKeyFromDocument(doc)
    const selected = (doc.products || []).filter(
      (p) => p.source_detail_id != null && diffDelivered[Number(p.source_detail_id)] != null,
    )
    if (!selected.length) {
      setGateErr("Seleccione al menos un producto con diferencia")
      return
    }
    const diffs: Array<{
      source_detail_id: number
      delivered_units: number
      undelivered_reason: string
      undelivered_note: string
    }> = []
    for (const p of selected) {
      const sid = Number(p.source_detail_id)
      const planned = typeof p.unidades === "number" ? p.unidades : 0
      const raw = String(diffDelivered[sid] ?? "").trim()
      if (raw === "") {
        setGateErr(
          `Indique unidades entregadas de «${commercialProductLabel(p)}» (0…${planned - 1}). Si desconoce la cantidad, use Incidencia.`,
        )
        return
      }
      const delivered = Number(raw.replace(",", "."))
      if (!Number.isFinite(delivered) || delivered < 0 || delivered > planned) {
        setGateErr(
          `Cantidad inválida en «${commercialProductLabel(p)}» (0…${planned})`,
        )
        return
      }
      if (delivered >= planned) {
        setGateErr(
          `«${commercialProductLabel(p)}» sin diferencia real: quítelo de la lista o use entrega completa`,
        )
        return
      }
      diffs.push({
        source_detail_id: sid,
        delivered_units: delivered,
        undelivered_reason: "physical_shortage",
        undelivered_note: partialNote.trim() || "Diferencia en modo simple",
      })
    }
    const summary = diffs
      .map((d) => {
        const p = selected.find((x) => Number(x.source_detail_id) === d.source_detail_id)
        const planned = typeof p?.unidades === "number" ? p.unidades : "?"
        return `${commercialProductLabel(p || {})}: ${d.delivered_units}/${planned}`
      })
      .join("\n")
    if (
      !window.confirm(
        `¿Registrar entrega PARCIAL?\n${summary}\n\nEl resto se registrará como entregado completo.`,
      )
    ) {
      return
    }
    setBusyKey(key)
    setGateErr(null)
    try {
      await confirmVisitDocumentDelivery(planId, session.activeVisit.id, {
        related_document_id: Number(doc.related_document_id),
        mode: "partial",
        oc_document_id: doc.oc_document_id ?? null,
        document_number: doc.document_number ?? null,
        document_type: doc.document_type ?? null,
        document_total:
          typeof doc.document_total === "number" ? doc.document_total : null,
        expected_version: visitVersion ?? null,
        difference_lines: diffs,
        rest_delivered_confirmed: true,
        notes: partialNote.trim() || null,
      })
      setPartialKey(null)
      setDiffDelivered({})
      setPartialNote("")
      void session.softReloadVisitDetail()
    } catch (e) {
      setGateErr(e instanceof ApiError ? e.message : "No se pudo registrar parcial")
    } finally {
      setBusyKey(null)
    }
  }

  async function confirmNone(doc: (typeof session.documents)[0]) {
    if (!session.activeVisit || doc.related_document_id == null) return
    const key = docKeyFromDocument(doc)
    if (
      !window.confirm(
        `¿Marcar documento como NO ENTREGADO?\nPara rechazo con foto use Incidencia.`,
      )
    ) {
      return
    }
    setBusyKey(key)
    setGateErr(null)
    try {
      await confirmVisitDocumentDelivery(planId, session.activeVisit.id, {
        related_document_id: Number(doc.related_document_id),
        mode: "none",
        oc_document_id: doc.oc_document_id ?? null,
        document_number: doc.document_number ?? null,
        document_type: doc.document_type ?? null,
        document_total:
          typeof doc.document_total === "number" ? doc.document_total : null,
        expected_version: visitVersion ?? null,
        notes: "No entregado (modo simple)",
      })
      void session.softReloadVisitDetail()
    } catch (e) {
      setGateErr(e instanceof ApiError ? e.message : "No se pudo registrar")
    } finally {
      setBusyKey(null)
    }
  }

  if (session.loading) return <div className="empty">Cargando documentos…</div>
  if (session.error) {
    return (
      <div className="stack-gap">
        <Link href={fichaHref} className="muted">
          ← Ficha
        </Link>
        <div className="banner banner-error">{session.error}</div>
      </div>
    )
  }

  if (!session.canOperateActive) {
    return (
      <div className="stack-gap">
        <div className="banner banner-info">
          {session.adminReviewPending
            ? "Existe una visita anterior pendiente de revisión administrativa. Consulte el resultado desde la ficha."
            : "No hay visita en atención. Inicie o retome desde la ficha."}
        </div>
        <Link href={fichaHref} className="btn btn-primary">
          Ir a ficha
        </Link>
      </div>
    )
  }

  return (
    <DeliveryStepChrome
      clientName={session.clientName}
      step={1}
      title="Paso 1 · Pedido"
      backHref={fichaHref}
      footer={
        <div className="btn-row">
          <Link
            href={buildEntregaHref(planId, customerKey, "incidencia")}
            className="btn btn-ghost"
          >
            Incidencia
          </Link>
          <button
            type="button"
            className="btn btn-primary"
            onClick={() => {
              if (!allSaved) {
                setGateErr(
                  `Faltan ${missingTotal} línea(s) por confirmar antes del pago`,
                )
                return
              }
              setGateErr(null)
              router.push(buildEntregaHref(planId, customerKey, "cobranza"))
            }}
          >
            Continuar a forma de pago
          </button>
        </div>
      }
    >
      <div className="filter-chips" role="group" aria-label="Modo de captura">
        <button
          type="button"
          className={`filter-chip${mode === "simple" ? " active" : ""}`}
          onClick={() => setMode("simple")}
        >
          Guía rápida
        </button>
        <button
          type="button"
          className={`filter-chip${mode === "detailed" ? " active" : ""}`}
          onClick={() => setMode("detailed")}
        >
          Modo detallado
        </button>
      </div>

      {mode === "simple" ? (
        <p className="muted" style={{ margin: 0, fontSize: "0.82rem" }}>
          Consulte el pedido y confirme el resultado por documento. No se
          marca entregado solo por abrir el listado.
        </p>
      ) : (
        <p className="muted" style={{ margin: 0, fontSize: "0.82rem" }}>
          Registro línea a línea (avanzado). Mismas reglas de persistencia.
        </p>
      )}

      {gateErr ? <div className="banner banner-error">{gateErr}</div> : null}

      <div className="stack-gap">
        {perDoc.map(({ doc, prog }) => {
          const key = docKeyFromDocument(doc) || String(Math.random())
          const href = buildEntregaHref(planId, customerKey, "documentos", {
            docKey: key,
          })
          const expanded = expandedKey === key || perDoc.length === 1
          const q = productQ.trim().toLowerCase()
          const products = (doc.products || []).filter((p) => {
            if (!q) return true
            const label = commercialProductLabel(p).toLowerCase()
            const code = (productBarcodeLabel(p) || "").toLowerCase()
            return label.includes(q) || code.includes(q)
          })
          return (
            <div key={key} className="card doc-card">
              <div className="doc-card-top">
                <h3 style={{ fontSize: "1rem", margin: 0 }}>
                  {doc.document_type || "Documento"} {doc.document_number ?? "—"}
                </h3>
                <span className={`pill${prog.ok ? " pill-ok" : ""}`}>{prog.label}</span>
              </div>
              <div className="muted" style={{ fontSize: "0.85rem", marginTop: 6 }}>
                {moneyCLP(doc.document_total)} · {(doc.products || []).length} producto(s)
              </div>

              {mode === "simple" && prog.registrable ? (
                <>
                  <button
                    type="button"
                    className="btn btn-ghost btn-compact"
                    style={{ marginTop: 8 }}
                    onClick={() =>
                      setExpandedKey((cur) => (cur === key ? null : key))
                    }
                  >
                    {expanded ? "Ocultar pedido" : "Ver pedido"}
                  </button>

                  {expanded ? (
                    <div className="stack-gap" style={{ marginTop: 10 }}>
                      {(doc.products || []).length > 8 ? (
                        <input
                          value={productQ}
                          onChange={(e) => setProductQ(e.target.value)}
                          placeholder="Buscar producto o código"
                          aria-label="Buscar productos"
                        />
                      ) : null}
                      {products.map((p, idx) => {
                        const units = typeof p.unidades === "number" ? p.unidades : 0
                        const box = breakdownBoxUnits(units, p.units_per_box ?? null)
                        return (
                          <div
                            key={`${key}-p-${p.source_detail_id ?? idx}`}
                            className="card guide-product-card"
                          >
                            <div className="muted" style={{ fontSize: "0.72rem" }}>
                              {productCategoryLabel(p) || "Producto"}
                              {productBarcodeLabel(p)
                                ? ` · ${productBarcodeLabel(p)}`
                                : ""}
                            </div>
                            <div className="product-name">{commercialProductLabel(p)}</div>
                            <div className="guide-product-qty">
                              <span className="pill pill-ok">
                                {units} u
                              </span>
                              {box.pillLabel ? (
                                <span className="pill">{box.pillLabel}</span>
                              ) : null}
                              {p.units_per_box ? (
                                <span className="pill">x{p.units_per_box}/caja</span>
                              ) : null}
                            </div>
                          </div>
                        )
                      })}
                    </div>
                  ) : null}

                  {!prog.complete ? (
                    <div className="doc-result-actions">
                      <button
                        type="button"
                        className="btn btn-primary"
                        disabled={busyKey === key}
                        onClick={() => void confirmFull(doc)}
                      >
                        {busyKey === key && confirmKey === key
                          ? "Guardando…"
                          : "Entrega completa"}
                      </button>
                      <button
                        type="button"
                        className="btn btn-secondary"
                        disabled={busyKey === key}
                        onClick={() => {
                          setPartialKey(key)
                          setDiffDelivered({})
                          setConfirmKey(null)
                        }}
                      >
                        Entrega parcial
                      </button>
                      <button
                        type="button"
                        className="btn btn-ghost"
                        disabled={busyKey === key}
                        onClick={() => void confirmNone(doc)}
                      >
                        No entregado
                      </button>
                      <Link href={href} className="btn btn-ghost btn-compact">
                        Abrir modo detallado
                      </Link>
                    </div>
                  ) : null}

                  {partialKey === key ? (
                    <div className="card" style={{ marginTop: 10 }}>
                      <p style={{ marginTop: 0, fontSize: "0.85rem" }}>
                        Marque solo los productos con diferencia e indique las
                        unidades realmente entregadas (ej. 35 de 60). El resto se
                        registra completo tras confirmar. Si no conoce la
                        cantidad, use Incidencia — no se inventan unidades.
                      </p>
                      <div className="stack-gap">
                        {(doc.products || [])
                          .filter((p) => p.source_detail_id != null)
                          .map((p) => {
                            const sid = Number(p.source_detail_id)
                            const planned =
                              typeof p.unidades === "number" ? p.unidades : 0
                            const selected = diffDelivered[sid] != null
                            return (
                              <div
                                key={`diff-${sid}`}
                                className="card guide-product-card"
                                style={{
                                  borderColor: selected
                                    ? "var(--accent)"
                                    : undefined,
                                }}
                              >
                                <label
                                  style={{
                                    display: "flex",
                                    gap: 8,
                                    alignItems: "flex-start",
                                    fontSize: "0.85rem",
                                  }}
                                >
                                  <input
                                    type="checkbox"
                                    checked={selected}
                                    onChange={(e) => {
                                      setDiffDelivered((prev) => {
                                        const next = { ...prev }
                                        if (e.target.checked) {
                                          next[sid] = ""
                                        } else {
                                          delete next[sid]
                                        }
                                        return next
                                      })
                                    }}
                                  />
                                  <span>
                                    {commercialProductLabel(p)}
                                    <span className="muted">
                                      {" "}
                                      · pedido {planned} u
                                    </span>
                                  </span>
                                </label>
                                {selected ? (
                                  <div
                                    className="field"
                                    style={{ marginTop: 8, marginBottom: 0 }}
                                  >
                                    <label htmlFor={`deliv-${key}-${sid}`}>
                                      Entregadas (0…{Math.max(0, planned - 1)})
                                    </label>
                                    <input
                                      id={`deliv-${key}-${sid}`}
                                      inputMode="decimal"
                                      value={diffDelivered[sid] ?? ""}
                                      placeholder="Ej. 35"
                                      onChange={(e) =>
                                        setDiffDelivered((prev) => ({
                                          ...prev,
                                          [sid]: e.target.value,
                                        }))
                                      }
                                    />
                                    <button
                                      type="button"
                                      className="btn btn-ghost btn-compact"
                                      style={{ marginTop: 6 }}
                                      onClick={() =>
                                        setDiffDelivered((prev) => ({
                                          ...prev,
                                          [sid]: "0",
                                        }))
                                      }
                                    >
                                      Nada entregado (0)
                                    </button>
                                  </div>
                                ) : null}
                              </div>
                            )
                          })}
                      </div>
                      <div className="field" style={{ marginTop: 10 }}>
                        <label htmlFor={`partial-note-${key}`}>Motivo / nota</label>
                        <input
                          id={`partial-note-${key}`}
                          value={partialNote}
                          onChange={(e) => setPartialNote(e.target.value)}
                          placeholder="Ej. faltante físico en 2 SKUs"
                        />
                      </div>
                      <p className="muted" style={{ fontSize: "0.78rem" }}>
                        Cantidad desconocida →{" "}
                        <Link
                          href={buildEntregaHref(planId, customerKey, "incidencia")}
                        >
                          registrar incidencia
                        </Link>
                        , sin inventar unidades.
                      </p>
                      <div className="btn-row" style={{ marginTop: 10 }}>
                        <button
                          type="button"
                          className="btn btn-ghost"
                          onClick={() => {
                            setPartialKey(null)
                            setDiffDelivered({})
                          }}
                        >
                          Cancelar
                        </button>
                        <button
                          type="button"
                          className="btn btn-primary"
                          disabled={busyKey === key}
                          onClick={() => void confirmPartial(doc)}
                        >
                          Confirmar parcial
                        </button>
                      </div>
                    </div>
                  ) : null}
                </>
              ) : null}

              {mode === "detailed" && prog.registrable ? (
                <div className="doc-card-actions">
                  <div className="line-progress" role="status">
                    Registrados {prog.registered} de {prog.expected} · Pendientes{" "}
                    {prog.pending}
                  </div>
                  <Link href={href} className="btn btn-secondary btn-block">
                    Abrir documento
                  </Link>
                </div>
              ) : null}

              {!prog.registrable ? (
                <p className="muted" style={{ fontSize: "0.82rem", marginTop: 8 }} role="status">
                  Sin detalle confiable: solo lectura. No se registra entrega
                  verificada de este documento.
                </p>
              ) : null}
            </div>
          )
        })}
      </div>
    </DeliveryStepChrome>
  )
}
