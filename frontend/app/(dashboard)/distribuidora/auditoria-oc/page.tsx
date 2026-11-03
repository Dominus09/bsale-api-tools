"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import {
  ClipboardList,
  GitCompareArrows,
  Loader2,
  Search,
  ShieldAlert,
} from "lucide-react"

import { getStoredCompanyId } from "@/lib/api"
import {
  fetchOcAuditMeta,
  postOcAuditCompare,
  postOcAuditInvestigate,
  postOcAuditLocal,
  postOcRecoverConfirm,
  postOcRecoverSimulate,
  type OcAuditFinding,
  type OcAuditMeta,
  type OcAuditReport,
  type OcAuditStatus,
  type OcCompareReport,
  type OcRecoverSimulation,
} from "@/lib/distribuidora-oc-audit-api"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { StatusBadge } from "@/components/analytics-v2/shared/analytics-ui"

const OFFICE_OPTIONS = [
  { id: 1, label: "Oficina 1" },
  { id: 2, label: "Oficina 2" },
  { id: 3, label: "Oficina 3" },
]

const STATUS_LABEL: Record<string, string> = {
  ok: "OK",
  gap_pending: "Salto pendiente",
  missing_in_pg: "Falta en PG",
  version_diff: "Versión distinta",
  cancelled: "Anulada",
  not_found_in_bsale: "No hallada en Bsale",
  verification_error: "Error verificación",
}

const COMPARE_LABEL: Record<string, string> = {
  equal: "Alineados",
  bsale_ahead: "Bsale adelantado",
  unverified: "No verificado",
  partial: "Parcial",
  no_local_folio: "Sin folio local",
}

function statusTone(
  status: string,
): "success" | "warning" | "danger" | "neutral" | "info" {
  switch (status as OcAuditStatus) {
    case "ok":
      return "success"
    case "gap_pending":
      return "warning"
    case "missing_in_pg":
      return "danger"
    case "version_diff":
      return "info"
    case "cancelled":
      return "neutral"
    case "verification_error":
      return "danger"
    case "not_found_in_bsale":
      return "neutral"
    default:
      return "neutral"
  }
}

function compareTone(
  status: string,
): "success" | "warning" | "danger" | "neutral" | "info" {
  switch (status) {
    case "equal":
      return "success"
    case "bsale_ahead":
      return "warning"
    case "partial":
      return "info"
    case "unverified":
    case "no_local_folio":
      return "danger"
    default:
      return "neutral"
  }
}

function formatWhen(iso?: string | null): string {
  if (!iso) return "—"
  try {
    return new Date(iso).toLocaleString("es-CL")
  } catch {
    return iso
  }
}

export default function AuditoriaOcPage() {
  const [companyId, setCompanyId] = useState(3)
  const [officeId, setOfficeId] = useState(1)
  const [fromFolio, setFromFolio] = useState("")
  const [toFolio, setToFolio] = useState("")
  const [meta, setMeta] = useState<OcAuditMeta | null>(null)
  const [report, setReport] = useState<OcAuditReport | null>(null)
  const [compare, setCompare] = useState<OcCompareReport | null>(null)
  const [selected, setSelected] = useState<Set<number>>(() => new Set())
  const [loadingMeta, setLoadingMeta] = useState(false)
  const [loadingLocal, setLoadingLocal] = useState(false)
  const [loadingInvestigate, setLoadingInvestigate] = useState(false)
  const [loadingCompare, setLoadingCompare] = useState(false)
  const [loadingSimulate, setLoadingSimulate] = useState(false)
  const [loadingConfirm, setLoadingConfirm] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [simulation, setSimulation] = useState<OcRecoverSimulation | null>(null)
  const [recoverOpen, setRecoverOpen] = useState(false)
  const [recoverResult, setRecoverResult] = useState<string | null>(null)

  useEffect(() => {
    const stored = getStoredCompanyId()
    if (stored === 1 || stored === 2 || stored === 3) setCompanyId(stored)
  }, [])

  const loadMeta = useCallback(async () => {
    setLoadingMeta(true)
    setError(null)
    try {
      const data = await fetchOcAuditMeta({ companyId, officeId, window: 100 })
      setMeta(data)
      if (data.suggested_range.from_folio != null) {
        setFromFolio(String(data.suggested_range.from_folio))
      }
      if (data.suggested_range.to_folio != null) {
        setToFolio(String(data.suggested_range.to_folio))
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : "No se pudo cargar meta")
    } finally {
      setLoadingMeta(false)
    }
  }, [companyId, officeId])

  useEffect(() => {
    void loadMeta()
  }, [loadMeta])

  const runLocal = async () => {
    const from = parseInt(fromFolio, 10)
    const to = parseInt(toFolio, 10)
    if (!Number.isFinite(from) || !Number.isFinite(to) || from <= 0 || to < from) {
      setError("Indique un rango de folios válido (from ≤ to, ambos > 0).")
      return
    }
    setLoadingLocal(true)
    setError(null)
    setSelected(new Set())
    try {
      const data = await postOcAuditLocal({
        company_id: companyId,
        office_id: officeId,
        from_folio: from,
        to_folio: to,
      })
      setReport(data)
    } catch (e) {
      setError(e instanceof Error ? e.message : "Error en auditoría local")
    } finally {
      setLoadingLocal(false)
    }
  }

  const runCompare = async () => {
    setLoadingCompare(true)
    setError(null)
    try {
      const data = await postOcAuditCompare({
        company_id: companyId,
        office_id: officeId,
      })
      setCompare(data)
      if (
        data.pending_investigate_folios?.length &&
        data.verification_status !== "unverified"
      ) {
        setSelected(
          new Set(data.pending_investigate_folios.slice(0, 25)),
        )
      }
    } catch (e) {
      setError(
        e instanceof Error
          ? e.message
          : "Error al comparar con Bsale (no se inventa un máximo)",
      )
    } finally {
      setLoadingCompare(false)
    }
  }

  const pendingGaps = useMemo(
    () =>
      (report?.findings || []).filter(
        (f) => f.status === "gap_pending" || f.status === "verification_error",
      ),
    [report],
  )

  const toggleFolio = (folio: number, checked: boolean) => {
    setSelected((prev) => {
      const next = new Set(prev)
      if (checked) next.add(folio)
      else next.delete(folio)
      return next
    })
  }

  const selectPending = () => {
    setSelected(new Set(pendingGaps.map((f) => f.folio).slice(0, 25)))
  }

  const mergeFindings = (incoming: OcAuditFinding[]) => {
    setReport((prev) => {
      if (!prev) {
        const counts: Record<string, number> = {}
        let recoverable = 0
        for (const f of incoming) {
          counts[f.status] = (counts[f.status] || 0) + 1
          if (f.recoverable) recoverable += 1
        }
        return {
          status: "completed",
          mode: "merged",
          wrote: false,
          company_id: companyId,
          office_id: officeId,
          findings: [...incoming].sort((a, b) => a.folio - b.folio),
          counts,
          recoverable_count: recoverable,
        }
      }
      const byFolio = new Map(incoming.map((f) => [f.folio, f]))
      const merged = prev.findings.map((f) => byFolio.get(f.folio) || f)
      for (const f of incoming) {
        if (!merged.some((x) => x.folio === f.folio)) merged.push(f)
      }
      const counts: Record<string, number> = {}
      let recoverable = 0
      for (const f of merged) {
        counts[f.status] = (counts[f.status] || 0) + 1
        if (f.recoverable) recoverable += 1
      }
      return {
        ...prev,
        findings: merged.sort((a, b) => a.folio - b.folio),
        counts,
        recoverable_count: recoverable,
        wrote: false,
      }
    })
  }

  const runInvestigate = async () => {
    if (selected.size === 0) {
      setError("Seleccione al menos un folio para investigar en Bsale.")
      return
    }
    setLoadingInvestigate(true)
    setError(null)
    try {
      const data = await postOcAuditInvestigate({
        company_id: companyId,
        office_id: officeId,
        folios: [...selected].sort((a, b) => a - b),
      })
      setReport((prev) => {
        if (!prev) return data
        const byFolio = new Map(data.findings.map((f) => [f.folio, f]))
        const merged = prev.findings.map((f) => byFolio.get(f.folio) || f)
        for (const f of data.findings) {
          if (!merged.some((x) => x.folio === f.folio)) merged.push(f)
        }
        const counts: Record<string, number> = {}
        let recoverable = 0
        for (const f of merged) {
          counts[f.status] = (counts[f.status] || 0) + 1
          if (f.recoverable) recoverable += 1
        }
        return {
          ...prev,
          ...data,
          mode: prev.mode,
          findings: merged.sort((a, b) => a.folio - b.folio),
          counts,
          recoverable_count: recoverable,
          wrote: false,
        }
      })
    } catch (e) {
      setError(
        e instanceof Error
          ? e.message
          : "Error al consultar Bsale (no se marca como inexistente)",
      )
    } finally {
      setLoadingInvestigate(false)
    }
  }

  const openSimulate = async (folio: number) => {
    setLoadingSimulate(true)
    setError(null)
    setRecoverResult(null)
    try {
      const data = await postOcRecoverSimulate({
        company_id: companyId,
        office_id: officeId,
        folio,
      })
      setSimulation(data)
      setRecoverOpen(true)
    } catch (e) {
      setError(e instanceof Error ? e.message : "No se pudo simular la recuperación")
    } finally {
      setLoadingSimulate(false)
    }
  }

  const confirmRecover = async () => {
    if (!simulation?.simulation_token) return
    setLoadingConfirm(true)
    setError(null)
    setRecoverResult(null)
    try {
      const data = await postOcRecoverConfirm({
        company_id: companyId,
        office_id: officeId,
        folio: simulation.folio,
        simulation_token: simulation.simulation_token,
      })
      setRecoverResult(
        data.wrote
          ? `OC #${data.folio} recuperada (doc local ${data.local_document_id ?? "—"}).`
          : data.detail ||
              `Resultado: ${data.status} (sin escritura duplicada).`,
      )
      if (data.finding) {
        mergeFindings([data.finding])
      } else if (data.wrote || data.status === "already_present") {
        mergeFindings([
          {
            folio: data.folio,
            status: "ok",
            local_document_id: data.local_document_id,
            bsale_source_document_id: data.bsale_source_document_id,
            recoverable: false,
            detail: data.detail || data.status,
          },
        ])
      }
      void loadMeta()
    } catch (e) {
      setError(
        e instanceof Error
          ? e.message
          : "No se pudo confirmar la recuperación",
      )
      setRecoverOpen(false)
      setSimulation(null)
    } finally {
      setLoadingConfirm(false)
    }
  }

  const summaryCards = [
    { label: "Analizados", value: report?.findings?.length ?? 0 },
    { label: "Saltos", value: report?.counts?.gap_pending ?? 0 },
    { label: "Recuperables", value: report?.recoverable_count ?? 0 },
    { label: "Anulados", value: report?.counts?.cancelled ?? 0 },
    {
      label: "Errores verif.",
      value: report?.counts?.verification_error ?? 0,
    },
  ]

  return (
    <div className="space-y-6 p-4 md:p-6">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h1 className="flex items-center gap-2 text-2xl font-semibold tracking-tight text-lq-ink">
            <ClipboardList className="h-6 w-6" />
            Auditoría OC
          </h1>
          <p className="mt-1 max-w-2xl text-sm text-lq-muted">
            Detección de correlativos faltantes (tipo 33). El escaneo local no
            llama a Bsale. La comparación remota y la recuperación son acciones
            manuales: simular primero, confirmar después (una OC por vez).
          </p>
        </div>
      </div>

      {error ? (
        <Alert variant="destructive">
          <ShieldAlert className="h-4 w-4" />
          <AlertTitle>No se completó la operación</AlertTitle>
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      ) : null}

      <section className="space-y-4 rounded-xl border border-lq-line bg-lq-surface p-4 md:p-5">
        <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
          <div className="space-y-1.5">
            <Label>Empresa</Label>
            <Select
              value={String(companyId)}
              onValueChange={(v) => setCompanyId(Number(v))}
            >
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="3">3 — La Quillotana SPA</SelectItem>
                <SelectItem value="1">1 — Minimarkets</SelectItem>
                <SelectItem value="2">2 — Carlos Romero</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-1.5">
            <Label>Oficina</Label>
            <Select
              value={String(officeId)}
              onValueChange={(v) => setOfficeId(Number(v))}
            >
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {OFFICE_OPTIONS.map((o) => (
                  <SelectItem key={o.id} value={String(o.id)}>
                    {o.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-1.5">
            <Label htmlFor="from-folio">Desde folio</Label>
            <Input
              id="from-folio"
              inputMode="numeric"
              value={fromFolio}
              onChange={(e) => setFromFolio(e.target.value)}
            />
          </div>
          <div className="space-y-1.5">
            <Label htmlFor="to-folio">Hasta folio</Label>
            <Input
              id="to-folio"
              inputMode="numeric"
              value={toFolio}
              onChange={(e) => setToFolio(e.target.value)}
            />
          </div>
        </div>

        <div className="flex flex-wrap gap-2">
          <Button type="button" variant="outline" onClick={() => void loadMeta()} disabled={loadingMeta}>
            {loadingMeta ? <Loader2 className="h-4 w-4 animate-spin" /> : null}
            Cargar rango sugerido
          </Button>
          <Button type="button" onClick={() => void runLocal()} disabled={loadingLocal}>
            {loadingLocal ? <Loader2 className="h-4 w-4 animate-spin" /> : <Search className="h-4 w-4" />}
            Auditar local
          </Button>
          <Button
            type="button"
            variant="secondary"
            onClick={() => void runCompare()}
            disabled={loadingCompare}
          >
            {loadingCompare ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <GitCompareArrows className="h-4 w-4" />
            )}
            Comparar con Bsale
          </Button>
        </div>

        {meta?.suggested_range?.tail_guidance ? (
          <p className="text-xs text-lq-muted">{meta.suggested_range.tail_guidance}</p>
        ) : null}
        {meta?.bounds?.max_folio != null ? (
          <p className="text-xs text-lq-faint">
            Máximo local PG: #{meta.bounds.max_folio}
            {meta.bounds.min_folio != null ? ` · Mínimo: #${meta.bounds.min_folio}` : ""}
            {" · "}
            Tipo documental 33 · Recuperación manual con simulación previa
          </p>
        ) : null}
      </section>

      {compare ? (
        <section className="space-y-3 rounded-xl border border-lq-line bg-lq-surface p-4 md:p-5">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <h2 className="text-sm font-semibold uppercase tracking-wide text-lq-faint">
              Comparación Bsale / PostgreSQL
            </h2>
            <StatusBadge tone={compareTone(compare.verification_status)}>
              {COMPARE_LABEL[compare.verification_status] ||
                compare.verification_status}
            </StatusBadge>
          </div>
          <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
            <div className="rounded-lg border border-lq-line/80 px-3 py-2">
              <div className="text-xs text-lq-faint">Máximo PostgreSQL</div>
              <div className="text-xl font-semibold text-lq-ink">
                {compare.pg_max_folio != null ? `#${compare.pg_max_folio}` : "—"}
              </div>
            </div>
            <div className="rounded-lg border border-lq-line/80 px-3 py-2">
              <div className="text-xs text-lq-faint">
                Máximo Bsale verificado
                {compare.bsale_max_is_absolute === false ? " (no absoluto)" : ""}
              </div>
              <div className="text-xl font-semibold text-lq-ink">
                {compare.verification_status === "unverified" ||
                compare.verification_status === "no_local_folio"
                  ? "No concluyente"
                  : compare.bsale_verified_max_folio != null ||
                      compare.bsale_max_folio != null
                    ? `#${compare.bsale_verified_max_folio ?? compare.bsale_max_folio}`
                    : "No concluyente"}
              </div>
            </div>
            <div className="rounded-lg border border-lq-line/80 px-3 py-2">
              <div className="text-xs text-lq-faint">Por investigar</div>
              <div className="text-xl font-semibold text-lq-ink">
                {compare.pending_investigate_count}
              </div>
            </div>
            <div className="rounded-lg border border-lq-line/80 px-3 py-2">
              <div className="text-xs text-lq-faint">Última consulta remota</div>
              <div className="text-sm font-medium text-lq-ink">
                {formatWhen(compare.queried_at || compare.finished_at)}
              </div>
            </div>
          </div>
          {compare.detail ? (
            <p className="text-sm text-lq-muted">{compare.detail}</p>
          ) : null}
          {compare.ceiling_reached ? (
            <Alert>
              <AlertTitle>Techo de sonda alcanzado</AlertTitle>
              <AlertDescription>
                El tip Bsale puede ser mayor al verificado. No se inventa un
                máximo; vuelva a comparar más adelante o investigue el tramo.
              </AlertDescription>
            </Alert>
          ) : null}
        </section>
      ) : null}

      {report ? (
        <>
          <section className="grid gap-3 sm:grid-cols-2 lg:grid-cols-5">
            {summaryCards.map((c) => (
              <div
                key={c.label}
                className="rounded-xl border border-lq-line bg-lq-surface px-4 py-3"
              >
                <div className="text-xs uppercase tracking-wide text-lq-faint">{c.label}</div>
                <div className="mt-1 text-2xl font-semibold text-lq-ink">{c.value}</div>
              </div>
            ))}
          </section>

          {report.to_folio_beyond_local_max ? (
            <Alert>
              <AlertTitle>Rango sobre el máximo local</AlertTitle>
              <AlertDescription>
                Hay {report.gaps_beyond_local_max ?? 0} folio(s) por encima del
                máximo PostgreSQL. Investíguelos en Bsale para confirmar si
                faltan en el ERP; un error HTTP no los marca como inexistentes.
              </AlertDescription>
            </Alert>
          ) : null}

          {report.sequence_probe?.has_overlap ? (
            <Alert>
              <AlertTitle>Posible secuencia compartida entre oficinas</AlertTitle>
              <AlertDescription>
                Se detectaron números también presentes en otras oficinas de la
                misma empresa. La auditoría sigue filtrada por la oficina
                seleccionada.
              </AlertDescription>
            </Alert>
          ) : null}

          <section className="space-y-3 rounded-xl border border-lq-line bg-lq-surface p-4">
            <div className="flex flex-wrap items-center gap-2">
              <Button type="button" variant="outline" size="sm" onClick={selectPending}>
                Seleccionar saltos (máx. 25)
              </Button>
              <Button
                type="button"
                size="sm"
                onClick={() => void runInvestigate()}
                disabled={loadingInvestigate || selected.size === 0}
              >
                {loadingInvestigate ? (
                  <Loader2 className="h-4 w-4 animate-spin" />
                ) : null}
                Investigar seleccionados en Bsale
              </Button>
              <span className="text-xs text-lq-muted">{selected.size} seleccionados</span>
            </div>

            <div className="overflow-x-auto">
              <table className="min-w-full text-left text-sm">
                <thead className="border-b border-lq-line text-xs uppercase text-lq-faint">
                  <tr>
                    <th className="px-2 py-2">Sel.</th>
                    <th className="px-2 py-2">Folio</th>
                    <th className="px-2 py-2">Estado</th>
                    <th className="px-2 py-2">Doc. local</th>
                    <th className="px-2 py-2">source_id</th>
                    <th className="px-2 py-2">Bsale id</th>
                    <th className="px-2 py-2">Detalle</th>
                    <th className="px-2 py-2">Acción</th>
                  </tr>
                </thead>
                <tbody>
                  {(report.findings as OcAuditFinding[]).map((f) => (
                    <tr key={f.folio} className="border-b border-lq-line/70">
                      <td className="px-2 py-2">
                        <Checkbox
                          checked={selected.has(f.folio)}
                          onCheckedChange={(v) => toggleFolio(f.folio, v === true)}
                        />
                      </td>
                      <td className="px-2 py-2 font-medium text-lq-ink">#{f.folio}</td>
                      <td className="px-2 py-2">
                        <div className="flex flex-wrap items-center gap-1">
                          <StatusBadge tone={statusTone(f.status)}>
                            {STATUS_LABEL[f.status] || f.status}
                          </StatusBadge>
                          {f.recoverable ? (
                            <StatusBadge tone="info">Recuperable</StatusBadge>
                          ) : null}
                          {f.beyond_local_max ? (
                            <StatusBadge tone="warning">Cola</StatusBadge>
                          ) : null}
                        </div>
                      </td>
                      <td className="px-2 py-2 text-lq-body">
                        {f.local_document_id ?? "—"}
                      </td>
                      <td className="px-2 py-2 text-lq-body">
                        {f.postgresql_source_document_id ?? "—"}
                      </td>
                      <td className="px-2 py-2 text-lq-body">
                        {f.bsale_source_document_id ?? "—"}
                      </td>
                      <td className="max-w-xs px-2 py-2 text-xs text-lq-muted">
                        {f.error || f.detail || f.emission_date || "—"}
                      </td>
                      <td className="px-2 py-2">
                        {f.recoverable ? (
                          <Button
                            type="button"
                            size="sm"
                            variant="outline"
                            disabled={loadingSimulate}
                            onClick={() => void openSimulate(f.folio)}
                          >
                            {loadingSimulate ? (
                              <Loader2 className="h-3.5 w-3.5 animate-spin" />
                            ) : null}
                            Simular recuperación
                          </Button>
                        ) : (
                          <span className="text-xs text-lq-faint">—</span>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>
        </>
      ) : null}

      <Dialog
        open={recoverOpen}
        onOpenChange={(open) => {
          setRecoverOpen(open)
          if (!open) {
            setSimulation(null)
            setRecoverResult(null)
          }
        }}
      >
        <DialogContent className="sm:max-w-lg">
          <DialogHeader>
            <DialogTitle>
              {recoverResult
                ? "Resultado de recuperación"
                : `Simulación OC #${simulation?.folio ?? ""}`}
            </DialogTitle>
            <DialogDescription>
              {recoverResult
                ? "La operación terminó. El listado se actualiza con el nuevo estado."
                : "Revise el detalle. Confirmar revalida en Bsale y escribe una sola OC."}
            </DialogDescription>
          </DialogHeader>

          {recoverResult ? (
            <p className="text-sm text-lq-body">{recoverResult}</p>
          ) : simulation ? (
            <div className="space-y-2 text-sm text-lq-body">
              <div className="grid grid-cols-2 gap-2">
                <div>
                  <div className="text-xs text-lq-faint">Empresa / oficina</div>
                  <div>
                    {simulation.company_id} / {simulation.office_id}
                  </div>
                </div>
                <div>
                  <div className="text-xs text-lq-faint">Documento Bsale</div>
                  <div>{simulation.bsale_source_document_id ?? "—"}</div>
                </div>
                <div>
                  <div className="text-xs text-lq-faint">Cliente</div>
                  <div>
                    {simulation.client?.client_name ||
                      (simulation.client?.client_id != null
                        ? `#${simulation.client.client_id}`
                        : "—")}
                  </div>
                </div>
                <div>
                  <div className="text-xs text-lq-faint">Total</div>
                  <div>{simulation.total_amount ?? "—"}</div>
                </div>
                <div>
                  <div className="text-xs text-lq-faint">Líneas</div>
                  <div>{simulation.details_count ?? "—"}</div>
                </div>
                <div>
                  <div className="text-xs text-lq-faint">Resultado previsto</div>
                  <div>
                    {String(
                      simulation.predicted_reconciliation?.status ?? "—",
                    )}
                  </div>
                </div>
              </div>
              {simulation.warnings && simulation.warnings.length > 0 ? (
                <Alert>
                  <AlertTitle>Advertencias</AlertTitle>
                  <AlertDescription>
                    <ul className="list-disc pl-4">
                      {simulation.warnings.map((w) => (
                        <li key={w}>{w}</li>
                      ))}
                    </ul>
                  </AlertDescription>
                </Alert>
              ) : null}
              <p className="text-xs text-lq-faint">
                Expira: {formatWhen(simulation.simulation_expires_at)}
              </p>
            </div>
          ) : null}

          <DialogFooter>
            {recoverResult ? (
              <Button
                type="button"
                onClick={() => {
                  setRecoverOpen(false)
                  setSimulation(null)
                  setRecoverResult(null)
                }}
              >
                Cerrar
              </Button>
            ) : (
              <>
                <Button
                  type="button"
                  variant="outline"
                  onClick={() => {
                    setRecoverOpen(false)
                    setSimulation(null)
                  }}
                >
                  Cancelar
                </Button>
                <Button
                  type="button"
                  onClick={() => void confirmRecover()}
                  disabled={loadingConfirm || !simulation?.simulation_token}
                >
                  {loadingConfirm ? (
                    <Loader2 className="h-4 w-4 animate-spin" />
                  ) : null}
                  Confirmar recuperación
                </Button>
              </>
            )}
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}
