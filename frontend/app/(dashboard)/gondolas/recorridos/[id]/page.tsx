"use client"

import { useCallback, useEffect, useRef, useState } from "react"
import Link from "next/link"
import { useParams, useRouter, useSearchParams } from "next/navigation"
import {
  AlertTriangle,
  ArrowLeft,
  Ban,
  CheckCircle2,
  CircleCheck,
  Keyboard,
  ListChecks,
  Loader2,
  PackageCheck,
  PackageSearch,
  ScanBarcode,
} from "lucide-react"

import {
  AnalyticPageHeader,
  AnalyticsLoading,
  EmptyState,
  MetricCard,
  ProfessionalDataTable,
  SectionCard,
  StatusBadge,
} from "@/components/analytics-v2/shared/analytics-ui"
import { CameraScanner } from "@/components/gondolas/camera-scanner"
import {
  ExhibitionBadge,
  REPLENISHMENT_LABELS,
  REPLENISHMENT_TONES,
  SCAN_RESULT_LABELS,
  SCAN_RESULT_TONES,
  formatDate,
  formatDateTime,
  formatQty,
  movementLabel,
} from "@/components/gondolas/gondola-labels"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import {
  GondolasError,
  cancelGondolaSession,
  closeGondolaSession,
  getGondolaOptions,
  getGondolaSession,
  registerGondolaScan,
  updateGondolaReplenishment,
} from "@/lib/gondolas/api"
import type {
  GondolaSessionDetail,
  ReplenishmentStatus,
  ScanResponse,
} from "@/lib/gondolas/types"
import { getStoredCompanyId } from "@/lib/api"
import { cn } from "@/lib/utils"

const REPLENISHMENT_OPTIONS = Object.keys(REPLENISHMENT_LABELS) as ReplenishmentStatus[]

export default function GondolaSessionPage() {
  const params = useParams<{ id: string }>()
  const search = useSearchParams()
  const router = useRouter()
  const sessionId = Number(params.id)
  const [companyId] = useState<number | null>(() => {
    const fromQuery = Number(search.get("company_id"))
    return Number.isFinite(fromQuery) && fromQuery > 0 ? fromQuery : getStoredCompanyId()
  })
  const [session, setSession] = useState<GondolaSessionDetail | null>(null)
  const [canCancel, setCanCancel] = useState(false)
  const [code, setCode] = useState("")
  const [scanning, setScanning] = useState(false)
  const [lastScan, setLastScan] = useState<ScanResponse | null>(null)
  const [filter, setFilter] = useState<"todos" | "pendiente" | "encontrado">("todos")
  const [itemQuery, setItemQuery] = useState("")
  const [busyAction, setBusyAction] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  const inputRef = useRef<HTMLInputElement | null>(null)

  const load = useCallback(async () => {
    if (companyId == null || !sessionId) return
    try {
      const detail = await getGondolaSession(companyId, sessionId)
      setSession(detail)
    } catch (err) {
      setError(err instanceof GondolasError ? err.message : "No se pudo cargar el recorrido.")
    }
  }, [companyId, sessionId])

  useEffect(() => {
    void load()
    if (companyId != null) {
      getGondolaOptions(companyId)
        .then((opts) => setCanCancel(opts.capabilities.cancel))
        .catch(() => setCanCancel(false))
    }
  }, [load, companyId])

  useEffect(() => {
    if (session?.status === "en_curso") inputRef.current?.focus()
  }, [session?.status, session?.found_count])

  async function submitScan(rawCode: string, device: string, variantId?: number) {
    const value = rawCode.trim()
    if (!value || companyId == null || scanning) return
    setScanning(true)
    setError(null)
    try {
      const result = await registerGondolaScan(companyId, sessionId, {
        bar_code: value,
        device,
        variant_id: variantId,
      })
      setLastScan(result)
      setCode("")
      await load()
      inputRef.current?.focus()
    } catch (err) {
      setError(err instanceof GondolasError ? err.message : "No se pudo registrar el escaneo.")
    } finally {
      setScanning(false)
    }
  }

  async function closeSession() {
    if (companyId == null) return
    if (!window.confirm("¿Cerrar el recorrido? Los SKU no detectados pasarán a la lista de reposición.")) return
    setBusyAction("close")
    try {
      const detail = await closeGondolaSession(companyId, sessionId)
      setSession(detail)
    } catch (err) {
      setError(err instanceof GondolasError ? err.message : "No se pudo cerrar el recorrido.")
    } finally {
      setBusyAction(null)
    }
  }

  async function cancelSession() {
    if (companyId == null) return
    if (!window.confirm("¿Cancelar este recorrido? El avance quedará registrado como cancelado.")) return
    setBusyAction("cancel")
    try {
      await cancelGondolaSession(companyId, sessionId)
      router.push("/gondolas")
    } catch (err) {
      setError(err instanceof GondolasError ? err.message : "No se pudo cancelar el recorrido.")
    } finally {
      setBusyAction(null)
    }
  }

  async function changeReplenishment(variantId: number, status: ReplenishmentStatus) {
    if (companyId == null) return
    setBusyAction(`rep-${variantId}`)
    try {
      const updated = await updateGondolaReplenishment(companyId, sessionId, variantId, status)
      setSession((prev) =>
        prev
          ? {
              ...prev,
              replenishment: (prev.replenishment ?? []).map((item) =>
                item.variant_id === variantId ? updated : item,
              ),
            }
          : prev,
      )
    } catch (err) {
      setError(err instanceof GondolasError ? err.message : "No se pudo actualizar la reposición.")
    } finally {
      setBusyAction(null)
    }
  }

  if (!session && !error) return <AnalyticsLoading label="Cargando recorrido de góndola…" />

  const items = (session?.items ?? []).filter((item) => {
    if (filter !== "todos" && item.exhibition_status !== filter) return false
    if (!itemQuery) return true
    const needle = itemQuery.toLowerCase()
    return [item.product_name, item.variant_name, item.sku_code, item.bar_code]
      .some((v) => (v ?? "").toLowerCase().includes(needle))
  })
  const stockChanges = (session?.items ?? []).filter((i) => i.stock_changed)
  const closed = session?.status === "cerrado"

  return (
    <div className="mx-auto w-full max-w-[1600px] space-y-6 px-4 py-6 md:px-6 lg:px-8">
      <AnalyticPageHeader
        eyebrow="Control de Góndolas · Recorrido"
        title={session ? `${session.office_name ?? "Sucursal"} — ${session.product_type_name ?? "Productos"}` : "Recorrido"}
        description={
          session
            ? `Iniciado por ${session.created_by} · ${formatDateTime(session.started_at)} · Stock de referencia congelado al iniciar.`
            : "Recorrido de exhibición."
        }
        actions={
          <>
            <Button asChild variant="outline" className="min-h-11">
              <Link href="/gondolas"><ArrowLeft className="mr-2 h-4 w-4" /> Volver al catálogo</Link>
            </Button>
            {session?.status === "en_curso" ? (
              <>
                <Button type="button" onClick={() => void closeSession()} disabled={busyAction !== null} className="min-h-11">
                  {busyAction === "close" ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <PackageCheck className="mr-2 h-4 w-4" />}
                  Cerrar recorrido
                </Button>
                {canCancel ? (
                  <Button type="button" variant="destructive" onClick={() => void cancelSession()} disabled={busyAction !== null} className="min-h-11">
                    <Ban className="mr-2 h-4 w-4" /> Cancelar
                  </Button>
                ) : null}
              </>
            ) : null}
          </>
        }
      />

      {error ? (
        <Alert variant="destructive">
          <AlertTriangle className="h-4 w-4" />
          <AlertTitle>Ocurrió un problema</AlertTitle>
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      ) : null}

      {session ? (
        <>
          <div className="flex flex-wrap items-center gap-2">
            <StatusBadge tone={session.status === "en_curso" ? "info" : closed ? "success" : "danger"}>
              {session.status === "en_curso" ? "En curso" : closed ? "Cerrado" : "Cancelado"}
            </StatusBadge>
            <span className="text-sm text-slate-500">
              El avance se guarda en el servidor: puede continuar desde otro dispositivo autorizado.
            </span>
          </div>

          <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
            <MetricCard label="SKU del universo inicial" value={session.total_sku} context="SKU distintos con stock positivo al iniciar" icon={ListChecks} />
            <MetricCard label="Encontrados en góndola" value={session.found_count} context="Cada SKU se cuenta una sola vez" tone="success" icon={CircleCheck} />
            <MetricCard label="Pendientes de escaneo" value={session.pending_count} context="Todavía no detectados en sala" tone={session.pending_count ? "warning" : "success"} icon={PackageSearch} />
            <MetricCard
              label="Cobertura"
              value={`${session.coverage_pct.toFixed(1).replace(".", ",")} %`}
              context="Encontrados sobre el universo inicial"
              tone={session.coverage_pct >= 90 ? "success" : session.coverage_pct >= 60 ? "info" : "warning"}
              icon={ScanBarcode}
            />
          </div>

          <div className="h-2.5 overflow-hidden rounded-full bg-slate-200" role="progressbar" aria-valuenow={Math.round(session.coverage_pct)} aria-valuemin={0} aria-valuemax={100} aria-label="Cobertura del recorrido">
            <div className="h-full rounded-full bg-primary transition-all duration-500" style={{ width: `${Math.min(100, session.coverage_pct)}%` }} />
          </div>

          {closed && session.stock_changed_count > 0 ? (
            <Alert>
              <AlertTriangle className="h-4 w-4" />
              <AlertTitle>El stock cambió durante el recorrido</AlertTitle>
              <AlertDescription>
                {session.stock_changed_count} SKU tienen un stock actual distinto al congelado al iniciar
                {stockChanges.length ? <>: {stockChanges.slice(0, 4).map((i) => i.product_name).join(", ")}{stockChanges.length > 4 ? "…" : ""}</> : null}.
                Revise la comparación en la lista de reposición.
              </AlertDescription>
            </Alert>
          ) : null}

          {session.status === "en_curso" ? (
            <div className="grid gap-6 xl:grid-cols-[minmax(0,420px)_minmax(0,1fr)]">
              <SectionCard title="Escanear productos" description="Pistola USB, cámara del celular o ingreso manual.">
                <form
                  onSubmit={(e) => {
                    e.preventDefault()
                    void submitScan(code, "usb_manual")
                  }}
                  className="space-y-3"
                >
                  <label className="space-y-1.5">
                    <span className="flex items-center gap-2 text-xs font-medium text-slate-600">
                      <Keyboard className="h-4 w-4" /> Lector USB o código manual
                    </span>
                    <Input
                      ref={inputRef}
                      value={code}
                      onChange={(e) => setCode(e.target.value)}
                      placeholder="Escanee o escriba el código y presione Enter"
                      className="min-h-12 font-mono text-base"
                      autoFocus
                      autoComplete="off"
                      inputMode="text"
                      aria-label="Código de barras"
                    />
                  </label>
                  <Button type="submit" disabled={scanning || !code.trim()} className="min-h-11 w-full">
                    {scanning ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <ScanBarcode className="mr-2 h-4 w-4" />}
                    Registrar código
                  </Button>
                </form>

                <div className="mt-5 border-t border-slate-100 pt-5">
                  <CameraScanner onCode={(scanned) => void submitScan(scanned, "camara")} />
                </div>

                {lastScan ? (
                  <div className="mt-5 space-y-3" role="status">
                    <StatusBadge tone={SCAN_RESULT_TONES[lastScan.result]}>
                      {SCAN_RESULT_LABELS[lastScan.result]}
                    </StatusBadge>
                    {lastScan.product ? (
                      <p className="text-sm leading-6 text-slate-700">
                        <strong className="text-slate-950">{lastScan.product.product_name}</strong>
                        {lastScan.product.variant_name ? <> · {lastScan.product.variant_name}</> : null}
                        <span className="block text-xs text-slate-500">
                          Código {lastScan.bar_code} · Cobertura {lastScan.progress.coverage_pct.toFixed(1).replace(".", ",")} %
                        </span>
                      </p>
                    ) : null}
                    {lastScan.result === "ambiguo" ? (
                      <div className="space-y-2">
                        <p className="text-xs font-medium text-slate-600">Este código corresponde a más de una variante. Seleccione la que está físicamente en la góndola:</p>
                        {lastScan.candidates.map((candidate) => (
                          <Button
                            key={candidate.variant_id}
                            type="button"
                            variant="outline"
                            className="min-h-11 w-full justify-start"
                            onClick={() => void submitScan(lastScan.bar_code, "seleccion_manual", candidate.variant_id)}
                          >
                            {candidate.product_name}{candidate.variant_name ? ` · ${candidate.variant_name}` : ""}
                          </Button>
                        ))}
                      </div>
                    ) : null}
                  </div>
                ) : null}

                {session.recent_scans.length > 0 ? (
                  <div className="mt-5 border-t border-slate-100 pt-4">
                    <h3 className="text-sm font-semibold text-slate-950">Escaneos recientes</h3>
                    <ul className="mt-2 divide-y divide-slate-100">
                      {session.recent_scans.slice(0, 8).map((scan) => (
                        <li key={scan.id} className="flex items-center justify-between gap-3 py-2 text-xs">
                          <span className="font-mono text-slate-600">{scan.bar_code_raw}</span>
                          <StatusBadge tone={SCAN_RESULT_TONES[scan.result]}>{SCAN_RESULT_LABELS[scan.result]}</StatusBadge>
                        </li>
                      ))}
                    </ul>
                  </div>
                ) : null}
              </SectionCard>

              <SectionCard
                title="Productos del recorrido"
                description="Toque un filtro para concentrarse en lo pendiente. El stock mostrado es el actual en Bsale."
                contentClassName="p-0"
                action={
                  <div className="flex flex-wrap gap-2">
                    {(["todos", "pendiente", "encontrado"] as const).map((value) => (
                      <Button
                        key={value}
                        type="button"
                        size="sm"
                        variant={filter === value ? "default" : "outline"}
                        onClick={() => setFilter(value)}
                        className="min-h-9"
                      >
                        {value === "todos" ? "Todos" : value === "pendiente" ? "Pendientes" : "Encontrados"}
                      </Button>
                    ))}
                  </div>
                }
              >
                <div className="border-b border-slate-100 p-4">
                  <Input value={itemQuery} onChange={(e) => setItemQuery(e.target.value)} placeholder="Buscar dentro del recorrido…" className="min-h-11" aria-label="Buscar dentro del recorrido" />
                </div>
                <SessionItemsTable items={items} />
              </SectionCard>
            </div>
          ) : null}

          {closed ? (
            <SectionCard
              title="Lista de reposición"
              description="SKU no detectados en sala con stock positivo registrado. Esto no afirma que estén en bodega: primero deben buscarse."
              contentClassName="p-0"
            >
              {(session.replenishment ?? []).length === 0 ? (
                <EmptyState
                  title="Sin productos por reponer"
                  description="Todos los SKU del universo inicial fueron detectados en la góndola."
                />
              ) : (
                <>
                  <div className="grid gap-3 p-4 md:hidden">
                    {(session.replenishment ?? []).map((item) => (
                      <article key={item.variant_id} className="rounded-xl border border-slate-200 p-4">
                        <h3 className="text-sm font-semibold text-slate-950">{item.product_name}</h3>
                        <p className="mt-0.5 text-xs text-slate-500">{item.variant_name || "Sin variante"} · {item.bar_code || item.sku_code || ""}</p>
                        <p className="mt-2 text-xs text-slate-600">
                          Stock inicial {formatQty(item.stock_snapshot)} · actual {formatQty(item.stock_current)}
                          {item.stock_changed ? " · cambió durante el recorrido" : ""}
                        </p>
                        <select
                          className="mt-3 min-h-11 w-full rounded-lg border border-slate-300 bg-white px-3 text-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary/35"
                          value={item.status}
                          disabled={busyAction === `rep-${item.variant_id}`}
                          onChange={(e) => void changeReplenishment(item.variant_id, e.target.value as ReplenishmentStatus)}
                          aria-label={`Estado de reposición de ${item.product_name}`}
                        >
                          {REPLENISHMENT_OPTIONS.map((status) => (
                            <option key={status} value={status}>{REPLENISHMENT_LABELS[status]}</option>
                          ))}
                        </select>
                      </article>
                    ))}
                  </div>
                  <ProfessionalDataTable className="hidden rounded-none border-0 shadow-none md:block">
                    <table className="w-full min-w-[920px] text-left text-sm">
                      <thead className="bg-slate-50 text-xs uppercase tracking-wide text-slate-500">
                        <tr>
                          <th className="px-4 py-3 font-medium">Producto no detectado</th>
                          <th className="px-4 py-3 text-right font-medium">Stock inicial</th>
                          <th className="px-4 py-3 text-right font-medium">Stock actual</th>
                          <th className="px-4 py-3 font-medium">Última entrada</th>
                          <th className="px-4 py-3 font-medium">Estado de reposición</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-slate-100">
                        {(session.replenishment ?? []).map((item) => {
                          const full = session.items.find((i) => i.variant_id === item.variant_id)
                          return (
                            <tr key={item.variant_id} className="transition hover:bg-slate-50/70">
                              <td className="px-4 py-3">
                                <span className="block font-medium text-slate-950">{item.product_name}</span>
                                <span className="mt-0.5 block text-xs text-slate-500">
                                  {item.variant_name || "Sin variante"} · {item.bar_code || item.sku_code || "Sin código"}
                                </span>
                              </td>
                              <td className="px-4 py-3 text-right tabular-nums">{formatQty(item.stock_snapshot)}</td>
                              <td className={cn("px-4 py-3 text-right tabular-nums", item.stock_changed && "font-semibold text-amber-700")}>
                                {formatQty(item.stock_current)}
                                {item.stock_changed ? <span className="block text-[11px] font-medium">Cambió</span> : null}
                              </td>
                              <td className="px-4 py-3 text-slate-600">
                                {full?.last_entry_at ? (
                                  <>
                                    <span className="block">{formatDate(full.last_entry_at)}</span>
                                    <span className="mt-0.5 block text-xs text-slate-500">{movementLabel(full.last_entry_movement)}</span>
                                  </>
                                ) : (
                                  <span className="text-slate-400">Sin ingreso registrado</span>
                                )}
                              </td>
                              <td className="px-4 py-3">
                                <div className="flex items-center gap-2">
                                  <StatusBadge tone={REPLENISHMENT_TONES[item.status]}>{REPLENISHMENT_LABELS[item.status]}</StatusBadge>
                                  <select
                                    className="min-h-10 rounded-lg border border-slate-300 bg-white px-2 text-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary/35"
                                    value={item.status}
                                    disabled={busyAction === `rep-${item.variant_id}`}
                                    onChange={(e) => void changeReplenishment(item.variant_id, e.target.value as ReplenishmentStatus)}
                                    aria-label={`Cambiar estado de ${item.product_name}`}
                                  >
                                    {REPLENISHMENT_OPTIONS.map((status) => (
                                      <option key={status} value={status}>{REPLENISHMENT_LABELS[status]}</option>
                                    ))}
                                  </select>
                                </div>
                                {item.updated_by ? (
                                  <span className="mt-1 block text-[11px] text-slate-400">
                                    {item.updated_by} · {formatDateTime(item.updated_at)}
                                  </span>
                                ) : null}
                              </td>
                            </tr>
                          )
                        })}
                      </tbody>
                    </table>
                  </ProfessionalDataTable>
                </>
              )}
              <div className="flex items-center gap-2 border-t border-slate-100 px-4 py-3 text-xs text-slate-500">
                <CheckCircle2 className="h-4 w-4 shrink-0 text-emerald-600" />
                Marcar estados de reposición no modifica las existencias de Bsale.
              </div>
            </SectionCard>
          ) : null}

          {session.status !== "en_curso" && !closed ? (
            <SectionCard title="Productos del recorrido" contentClassName="p-0">
              <SessionItemsTable items={items} />
            </SectionCard>
          ) : null}
        </>
      ) : null}
    </div>
  )
}

function SessionItemsTable({ items }: { items: GondolaSessionDetail["items"] }) {
  if (items.length === 0) {
    return (
      <EmptyState
        title="Sin productos en esta vista"
        description="Cambie el filtro o la búsqueda para ver otros SKU del recorrido."
      />
    )
  }
  return (
    <>
      <div className="grid gap-3 p-4 md:hidden">
        {items.map((item) => (
          <article key={item.variant_id} className="rounded-xl border border-slate-200 p-4">
            <div className="flex items-start justify-between gap-3">
              <div className="min-w-0">
                <h3 className="text-sm font-semibold leading-5 text-slate-950">{item.product_name}</h3>
                <p className="mt-0.5 text-xs text-slate-500">{item.variant_name || "Sin variante"}</p>
              </div>
              <ExhibitionBadge status={item.exhibition_status} />
            </div>
            <dl className="mt-3 grid grid-cols-2 gap-2 text-xs text-slate-600">
              <div><dt className="text-slate-400">Código</dt><dd className="font-mono">{item.bar_code || item.sku_code || "—"}</dd></div>
              <div><dt className="text-slate-400">Stock actual</dt><dd className="font-semibold text-slate-900">{formatQty(item.stock_current)}</dd></div>
              <div><dt className="text-slate-400">Stock al iniciar</dt><dd>{formatQty(item.stock_snapshot)}</dd></div>
              <div><dt className="text-slate-400">Última entrada</dt><dd>{formatDate(item.last_entry_at)}</dd></div>
            </dl>
            {item.found_at ? (
              <p className="mt-2 text-xs text-slate-500">Escaneado {formatDateTime(item.found_at)}{item.found_by ? ` por ${item.found_by}` : ""}</p>
            ) : null}
          </article>
        ))}
      </div>
      <ProfessionalDataTable className="hidden rounded-none border-0 shadow-none md:block">
        <table className="w-full min-w-[860px] text-left text-sm">
          <thead className="bg-slate-50 text-xs uppercase tracking-wide text-slate-500">
            <tr>
              <th className="px-4 py-3 font-medium">Producto</th>
              <th className="px-4 py-3 font-medium">Código</th>
              <th className="px-4 py-3 text-right font-medium">Stock inicial</th>
              <th className="px-4 py-3 text-right font-medium">Stock actual</th>
              <th className="px-4 py-3 font-medium">Última entrada</th>
              <th className="px-4 py-3 font-medium">Exhibición</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-100">
            {items.map((item) => (
              <tr key={item.variant_id} className="transition hover:bg-slate-50/70">
                <td className="px-4 py-3">
                  <span className="block font-medium leading-5 text-slate-950">{item.product_name}</span>
                  <span className="mt-0.5 block text-xs text-slate-500">{item.variant_name || "Sin variante"}</span>
                </td>
                <td className="px-4 py-3 font-mono text-xs text-slate-600">{item.bar_code || item.sku_code || "—"}</td>
                <td className="px-4 py-3 text-right tabular-nums text-slate-600">{formatQty(item.stock_snapshot)}</td>
                <td className={cn("px-4 py-3 text-right font-semibold tabular-nums text-slate-950", item.stock_changed && "text-amber-700")}>
                  {formatQty(item.stock_current)}
                </td>
                <td className="px-4 py-3 text-slate-600">
                  {item.last_entry_at ? formatDate(item.last_entry_at) : <span className="text-slate-400">Sin ingreso registrado</span>}
                </td>
                <td className="px-4 py-3">
                  <ExhibitionBadge status={item.exhibition_status} />
                  {item.found_at ? (
                    <span className="mt-1 block text-[11px] text-slate-400">{formatDateTime(item.found_at)}</span>
                  ) : null}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </ProfessionalDataTable>
    </>
  )
}
