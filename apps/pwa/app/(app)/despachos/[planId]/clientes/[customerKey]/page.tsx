"use client"

import Link from "next/link"
import { useParams } from "next/navigation"
import { useEffect, useMemo, useState } from "react"
import { ApiError, getMyCliente } from "@/lib/api"
import { formatRutDisplay } from "@/lib/format-rut"
import { moneyCLP } from "@/lib/delivery-flow"
import { resolveSemaphore, validationLabelEs } from "@/lib/semaphore"
import { DeliveryLocalToolsHost } from "@/components/delivery-local-tools-host"
import { DeliveryVisitStart } from "@/components/delivery-visit-start"
import { asAmount } from "@/lib/money-display"

export default function ClienteFichaPage() {
  const params = useParams()
  const planId = Number(params.planId)
  const rawKey = params.customerKey
  const customerKey = decodeURIComponent(
    Array.isArray(rawKey) ? rawKey.join("/") : String(rawKey || ""),
  )
  const [data, setData] = useState<Awaited<ReturnType<typeof getMyCliente>> | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [reloadToken, setReloadToken] = useState(0)
  const [lightbox, setLightbox] = useState<string | null>(null)

  useEffect(() => {
    if (!Number.isFinite(planId) || planId < 1 || !customerKey) {
      setError("Cliente inválido")
      return
    }
    let cancelled = false
    async function load() {
      try {
        const res = await getMyCliente(planId, customerKey)
        if (!cancelled) {
          setData(res)
          setError(null)
        }
      } catch (err) {
        if (cancelled) return
        if (err instanceof ApiError && (err.status === 404 || err.status === 403)) {
          setError("Cliente no pertenece a tu despacho o no estás autorizado.")
        } else if (!navigator.onLine) {
          setError("Sin conexión. Reintenta cuando vuelva la red.")
        } else {
          setError(err instanceof ApiError ? err.message : "Error al cargar ficha")
        }
      }
    }
    void load()
    return () => {
      cancelled = true
    }
  }, [planId, customerKey, reloadToken])

  const totals = useMemo(() => {
    if (!data) return { count: 0, amount: null as number | null }
    const amount =
      asAmount(data.amount_total) ??
      asAmount(
        (data.documents || []).reduce(
          (acc, d) => acc + (typeof d.document_total === "number" ? d.document_total : 0),
          0,
        ),
      )
    return { count: data.document_count || data.documents?.length || 0, amount }
  }, [data])

  if (error) {
    return (
      <div className="stack-gap">
        <Link href={`/despachos/${planId}/clientes`} className="muted">
          ← Clientes
        </Link>
        <div className="banner banner-error">{error}</div>
      </div>
    )
  }

  if (!data) {
    return <div className="empty">Cargando ficha…</div>
  }

  const c = data.client
  const nav = data.navigation
  const facadeUrl = c.facade?.url || c.facade_photo || null
  const locLabel = c.location_registration?.label_es || "Ubicación no disponible"
  const gpsOk = Boolean(c.has_coordinates || c.location_registration?.verified)
  const visit = data.active_visit
  const closed = data.last_closed_visit
  const adminReview = Boolean(data.capabilities?.admin_review_pending)
  const operationalStatus = adminReview
    ? closed?.status
    : visit?.status === "in_progress"
      ? "in_progress"
      : closed?.status || data.latest_visit?.status || visit?.status
  const sem = resolveSemaphore({
    status: operationalStatus,
    resultReason: closed?.result_reason || visit?.result_reason,
  })
  const operationalLabel = adminReview
    ? `${sem.labelEs} · revisión admin`
    : visit?.status === "in_progress" && !adminReview
      ? "En atención"
      : sem.labelEs
  const valLabel = validationLabelEs(closed?.closure_review_status)
  const obs = (c.observations || "").trim()
  const backClients = `/despachos/${planId}/clientes`
  const loadGroup =
    (c.load_groups || [])[0]?.name ||
    ((c.load_groups || [])[0]?.load_batch_id
      ? `Grupo ${(c.load_groups || [])[0]?.load_batch_id}`
      : null)

  return (
    <div className="stack-gap">
      <Link href={backClients} className="muted" style={{ fontSize: "0.85rem" }}>
        ← Clientes
      </Link>

      <div className="card">
        <div className="ficha-head">
          <button
            type="button"
            className="facade-thumb"
            onClick={() => facadeUrl && setLightbox(facadeUrl)}
            aria-label={facadeUrl ? "Ampliar fachada" : "Local"}
            disabled={!facadeUrl}
          >
            {facadeUrl ? (
              // eslint-disable-next-line @next/next/no-img-element
              <img src={facadeUrl} alt="" />
            ) : (
              <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.7" aria-hidden>
                <path d="M4 10.5 12 4l8 6.5V20a1 1 0 0 1-1 1h-5v-6H10v6H5a1 1 0 0 1-1-1v-9.5Z" />
              </svg>
            )}
          </button>
          <div className="ficha-head-main">
            <h2 className="client-card-name" style={{ fontSize: "1.15rem" }}>
              {c.client_name || "Sin nombre"}
            </h2>
            {c.fantasy_name ? (
              <p className="muted" style={{ margin: "2px 0 8px", fontSize: "0.82rem" }}>
                {c.fantasy_name}
              </p>
            ) : null}
            <div className="ficha-summary" style={{ marginTop: 0 }}>
              <span className="pill">RUT {formatRutDisplay(c.rut) || "—"}</span>
              <span className={`pill${gpsOk ? " pill-ok" : ""}`}>
                {gpsOk ? "GPS disponible" : locLabel}
              </span>
            </div>
          </div>
        </div>

        <div className="ficha-compact-row">
          <span className="muted">Teléfono</span>
          <span>
            {c.phone ? (
              <a href={`tel:${c.phone}`} style={{ color: "var(--accent)", fontWeight: 650 }}>
                {c.phone}
              </a>
            ) : (
              "—"
            )}
          </span>
        </div>
        <div className="ficha-compact-row">
          <span className="muted">Dirección</span>
          <span style={{ textAlign: "right" }}>{c.address || "—"}</span>
        </div>
        <div className="ficha-compact-row">
          <span className="muted">Ciudad</span>
          <span>{c.city || "—"}</span>
        </div>

        <div className={obs ? "obs-block" : "obs-block obs-empty"}>
          {obs ? `Obs: ${obs}` : "Sin observaciones"}
        </div>

        <div className="ficha-kpi-grid">
          <div className="kpi">
            <span className="kpi-label">Monto</span>
            <span className="kpi-value" style={{ fontSize: "0.92rem" }}>
              {totals.amount != null ? moneyCLP(totals.amount) : "—"}
            </span>
          </div>
          <div className="kpi">
            <span className="kpi-label">Docs</span>
            <span className="kpi-value">{totals.count}</span>
          </div>
          <div className="kpi">
            <span className="kpi-label">Grupo</span>
            <span className="kpi-value" style={{ fontSize: "0.78rem" }}>
              {loadGroup || "—"}
            </span>
          </div>
        </div>

        <div className="group-row" style={{ marginTop: 10 }}>
          <span className={`sem-pill ${sem.cssClass}`}>{operationalLabel}</span>
          {valLabel ? (
            <span
              className={`pill${
                closed?.closure_review_status === "pending_review" ? " pill-warn" : ""
              }`}
            >
              {valLabel}
            </span>
          ) : null}
        </div>
      </div>

      {nav?.warning_es ? (
        <div className="banner banner-info" style={{ fontSize: "0.82rem" }}>
          {nav.warning_es}
        </div>
      ) : null}

      <DeliveryVisitStart
        planId={planId}
        customerKey={customerKey}
        clientName={c.client_name || "Cliente"}
        documentCount={totals.count}
        documentTotalLabel={totals.amount != null ? moneyCLP(totals.amount) : "—"}
        p4Ready={Boolean(data.p4?.tables_ready)}
        p4Note={data.p4?.note_es}
        activeVisit={data.active_visit || null}
        latestVisit={data.latest_visit || data.active_visit || null}
        lastClosedVisit={data.last_closed_visit || null}
        allowCreateAfterClose={
          data.capabilities?.create_new_visit_after_close ??
          !["completed", "partial", "not_delivered"].includes(
            String(data.last_closed_visit?.status || ""),
          )
        }
        blockNote={data.capabilities?.note_es || null}
        onChanged={() => setReloadToken((n) => n + 1)}
      />

      {!adminReview &&
      (data.capabilities?.start_delivery_visit ||
        data.active_visit?.status === "in_progress" ||
        data.capabilities?.create_new_visit_after_close) ? (
        <div className="btn-row">
          <Link
            href={`/despachos/${planId}/clientes/${encodeURIComponent(customerKey)}/entrega/incidencia?reason=closed_store`}
            className="btn btn-danger"
            style={{ textAlign: "center" }}
          >
            Local cerrado
          </Link>
          <Link
            href={`/despachos/${planId}/clientes/${encodeURIComponent(customerKey)}/entrega/incidencia?reason=rejected_order`}
            className="btn btn-secondary"
            style={{ textAlign: "center" }}
          >
            Pedido rechazado
          </Link>
        </div>
      ) : null}

      <DeliveryLocalToolsHost
        planId={planId}
        customerKey={customerKey}
        address={c.address}
        city={c.city}
        latitude={c.latitude}
        longitude={c.longitude}
        coordinatesSource={c.coordinates_source}
        locationRegistration={c.location_registration}
        facade={c.facade}
        facadePhoto={c.facade_photo}
        navigationUrl={nav?.available ? nav.google_maps_url || null : null}
        warnAccuracyM={data.p3?.gps_warn_accuracy_m}
        backHref={backClients}
        onChanged={() => setReloadToken((n) => n + 1)}
      />

      {lightbox ? (
        <div className="lightbox" role="dialog" aria-modal="true">
          <button
            type="button"
            className="drawer-backdrop"
            aria-label="Cerrar imagen"
            onClick={() => setLightbox(null)}
          />
          {/* eslint-disable-next-line @next/next/no-img-element */}
          <img src={lightbox} alt="Fachada ampliada" />
        </div>
      ) : null}
    </div>
  )
}
