"use client"

import Link from "next/link"
import { useMemo, useRef, useState } from "react"
import { ApiError, finalizeDeliveryVisit } from "@/lib/api"
import { DeliverySignaturePad } from "@/components/delivery-signature-pad"
import { DeliveryStepChrome } from "@/components/delivery-step-chrome"
import {
  filterActiveCollections,
  summarizeCollections,
} from "@/lib/active-collections"
import { docStatusLabel, paymentMethodLabel } from "@/lib/delivery-labels"
import { buildEntregaHref, moneyCLP } from "@/lib/delivery-flow"
import {
  isClosedVisitStatus,
  navigateToResultado,
  resultadoHref,
} from "@/lib/finalize-visit-nav"
import { useDeliveryParams } from "@/lib/use-delivery-params"
import { useVisitSession } from "@/lib/use-visit-session"

export default function EntregaFirmaPage() {
  const { planId, customerKey } = useDeliveryParams()
  const session = useVisitSession(planId, customerKey)
  const cobranzaHref = buildEntregaHref(planId, customerKey, "cobranza")
  const [conformityOk, setConformityOk] = useState(false)
  const [busy, setBusy] = useState(false)
  const [err, setErr] = useState<string | null>(null)
  const [confirmEnd, setConfirmEnd] = useState(false)
  const [closingMsg, setClosingMsg] = useState<string | null>(null)
  const [detailOpen, setDetailOpen] = useState(false)
  const finalizeLock = useRef(false)

  const detail = session.visitDetail
  const lines = detail?.lines || []
  const docs = detail?.documents || []
  const allCols = detail?.collections || []
  const existingConf = detail?.conformity

  const docResIds = useMemo(
    () =>
      docs
        .map((d) => (d.id != null ? Number(d.id) : null))
        .filter((n): n is number => n != null && Number.isFinite(n)),
    [docs],
  )

  const cols = useMemo(
    () => filterActiveCollections(allCols, docResIds),
    [allCols, docResIds],
  )

  const { compactLines, detailLines, blockReason } = useMemo(() => {
    const compact: string[] = []
    const full: string[] = []
    compact.push(`Cliente: ${session.clientName}`)
    let totalAmt = 0
    for (const d of docs) {
      const tot = typeof d.document_total === "number" ? d.document_total : 0
      totalAmt += tot
      compact.push(
        `${d.document_type || "Doc"} ${d.document_number ?? "—"} · ${moneyCLP(tot)} · ${docStatusLabel(String(d.result_status || ""))}`,
      )
    }
    compact.push(`Total documentos: ${moneyCLP(totalAmt)}`)
    let fullN = 0
    let partialN = 0
    let noneN = 0
    for (const ln of lines) {
      const planned = Number(ln.planned_units || 0)
      const delivered = Number(ln.delivered_units || 0)
      if (delivered >= planned) fullN += 1
      else if (delivered <= 0) noneN += 1
      else partialN += 1
      const undel = Math.max(0, planned - delivered)
      let s = `${ln.product_name || "Producto"}: ${delivered}/${planned}`
      if (undel > 0 && ln.undelivered_reason) s += ` · ${ln.undelivered_reason}`
      full.push(s)
    }
    compact.push(
      `Líneas: ${fullN} completas · ${partialN} parciales · ${noneN} no entregadas`,
    )
    const summary = summarizeCollections(cols, totalAmt)
    if (summary.cash > 0) {
      compact.push(`Efectivo declarado: ${moneyCLP(summary.cash)}`)
    }
    if (summary.transferDeclared > 0) {
      compact.push(
        `Transferencias informadas: ${moneyCLP(summary.transferDeclared)}`,
      )
    }
    if (summary.credit > 0) {
      compact.push(`Crédito: ${moneyCLP(summary.credit)}`)
    }
    for (const [method, amt] of Array.from(summary.byMethod.entries())) {
      if (
        method === "cash" ||
        method === "transfer_declared" ||
        method === "credit" ||
        method === "pending"
      ) {
        continue
      }
      compact.push(`${paymentMethodLabel(method)}: ${moneyCLP(amt)}`)
    }
    if (summary.byMethod.size === 0) compact.push("Sin cobranza declarada")
    compact.push(`Saldo pendiente: ${moneyCLP(summary.pendingBalance)}`)

    const orphans = allCols.filter((c) => c.document_result_id == null)
    let block: string | null = null
    if (orphans.length > 0) {
      block =
        "Hay declaraciones antiguas sin documento. Vuelva a cobranza y guarde de nuevo."
    }
    if (summary.declaredAll - totalAmt > 0.009) {
      block =
        "La cobranza vigente no cuadra con el total. Corrija antes de firmar."
    }

    for (const c of cols) {
      full.push(
        `${paymentMethodLabel(String(c.method))}: ${moneyCLP(
          typeof c.declared_amount === "number" ? c.declared_amount : 0,
        )}${c.note ? ` · ${c.note}` : ""}`,
      )
    }

    if (!docs.length) compact.push("Sin documentos registrados")
    if (!lines.length) compact.push("Sin líneas guardadas")

    const confLabel = existingConf
      ? existingConf.invalidated_at
        ? "Conformidad invalidada (requiere nueva firma)"
        : existingConf.refused
          ? "Negativa registrada"
          : "Firmada"
      : "Pendiente de firma"
    compact.push(`Conformidad: ${confLabel}`)

    return { compactLines: compact, detailLines: full, blockReason: block }
  }, [session.clientName, docs, lines, cols, allCols, existingConf])

  const summaryLines = detailOpen
    ? [...compactLines, "— Detalle —", ...detailLines]
    : compactLines

  async function onFinalize() {
    if (finalizeLock.current || busy) return
    const visit = session.activeVisit
    if (!visit) return
    finalizeLock.current = true
    setBusy(true)
    setErr(null)
    setClosingMsg("Guardando cierre…")
    try {
      const gps = await new Promise<{
        lat: number
        lng: number
        accuracy: number | null
        at: string
      } | null>((resolve) => {
        if (!navigator.geolocation) {
          resolve(null)
          return
        }
        navigator.geolocation.getCurrentPosition(
          (pos) =>
            resolve({
              lat: pos.coords.latitude,
              lng: pos.coords.longitude,
              accuracy:
                typeof pos.coords.accuracy === "number"
                  ? pos.coords.accuracy
                  : null,
              at: new Date(pos.timestamp || Date.now()).toISOString(),
            }),
          () => resolve(null),
          { enableHighAccuracy: true, timeout: 20000, maximumAge: 0 },
        )
      })
      const hasPartial = lines.some(
        (ln) => Number(ln.delivered_units || 0) < Number(ln.planned_units || 0),
      )
      const status = hasPartial ? "partial" : "completed"
      const res = await finalizeDeliveryVisit(planId, visit.id, {
        expected_version: visit.version,
        status,
        latitude: gps?.lat ?? null,
        longitude: gps?.lng ?? null,
        accuracy_m: gps?.accuracy ?? null,
        gps_captured_at: gps?.at,
        gps_source: gps ? "device" : undefined,
        gps_exception_code: gps ? undefined : "gps_unavailable",
        gps_exception_note: gps ? undefined : "GPS no disponible al finalizar",
      })
      if (!res.persisted) {
        setErr("El servidor no confirmó el cierre.")
        setClosingMsg(null)
        return
      }
      const closedId = Number(res.visit?.id || visit.id)
      if (!Number.isFinite(closedId) || closedId < 1) {
        setErr("Cierre guardado, pero falta el id de visita. Abra la ficha.")
        setClosingMsg(null)
        return
      }
      setClosingMsg("Cierre confirmado. Abriendo resultado…")
      setConfirmEnd(false)
      // Navegación dura inmediata: no generar WhatsApp aquí (evita crash iOS/PWA).
      navigateToResultado(planId, customerKey, closedId)
    } catch (e) {
      if (e instanceof ApiError && e.status === 409) {
        const closed = session.latestVisit
        if (closed && isClosedVisitStatus(closed.status)) {
          setClosingMsg("La visita ya estaba cerrada. Abriendo resultado…")
          navigateToResultado(planId, customerKey, closed.id)
          return
        }
      }
      setErr(e instanceof ApiError ? e.message : "No se pudo finalizar")
      setClosingMsg(null)
    } finally {
      finalizeLock.current = false
      setBusy(false)
    }
  }

  if (session.loading) return <div className="empty">Cargando conformidad…</div>
  if (session.error) return <div className="banner banner-error">{session.error}</div>

  const latest = session.latestVisit
  if (latest && isClosedVisitStatus(latest.status) && !session.activeVisit) {
    return (
      <div className="stack-gap">
        <div className="banner banner-info">
          Esta visita ya fue cerrada. No se inicia otra automáticamente.
        </div>
        <Link
          href={resultadoHref(planId, customerKey, latest.id)}
          className="btn btn-primary"
        >
          Ver resultado
        </Link>
        <Link href={buildEntregaHref(planId, customerKey, "ficha")} className="btn btn-ghost">
          Ir a ficha
        </Link>
      </div>
    )
  }

  if (!session.activeVisit || session.activeVisit.status !== "in_progress") {
    return (
      <div className="stack-gap">
        <div className="banner banner-info">Visita no activa.</div>
        <Link href={buildEntregaHref(planId, customerKey, "ficha")} className="btn btn-primary">
          Ir a ficha
        </Link>
      </div>
    )
  }

  const confReady =
    conformityOk ||
    Boolean(
      existingConf &&
        !existingConf.invalidated_at &&
        (existingConf.refused ||
          existingConf.signature_object_key ||
          existingConf.receptor_name),
    )

  return (
    <DeliveryStepChrome
      clientName={session.clientName}
      step={3}
      title="Paso 3 · Firma y cierre"
      backHref={cobranzaHref}
      footer={
        confReady && !confirmEnd ? (
          <button
            type="button"
            className="btn btn-primary"
            disabled={busy || Boolean(blockReason)}
            onClick={() => setConfirmEnd(true)}
          >
            Finalizar visita
          </button>
        ) : confReady && confirmEnd ? (
          <div className="btn-row">
            <button
              type="button"
              className="btn btn-ghost"
              disabled={busy}
              onClick={() => setConfirmEnd(false)}
            >
              Cancelar
            </button>
            <button
              type="button"
              className="btn btn-primary"
              disabled={busy}
              onClick={() => void onFinalize()}
            >
              {busy ? "Guardando cierre…" : "Confirmar cierre"}
            </button>
          </div>
        ) : null
      }
    >
      {existingConf && !existingConf.invalidated_at ? (
        <div className="banner banner-info">
          Conformidad ya registrada
          {existingConf.receptor_name
            ? ` · ${String(existingConf.receptor_name)}`
            : ""}
        </div>
      ) : (
        <DeliverySignaturePad
          planId={planId}
          visitId={session.activeVisit.id}
          summaryLines={summaryLines}
          detailOpen={detailOpen}
          onToggleDetail={() => setDetailOpen((v) => !v)}
          blockReason={blockReason}
          onDone={() => {
            setConformityOk(true)
            session.reload()
          }}
        />
      )}

      {closingMsg ? <div className="banner banner-info">{closingMsg}</div> : null}
      {err ? <div className="banner banner-error">{err}</div> : null}
    </DeliveryStepChrome>
  )
}
