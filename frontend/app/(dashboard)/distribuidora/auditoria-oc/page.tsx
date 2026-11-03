"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import { ClipboardList, Loader2, Search, ShieldAlert } from "lucide-react"

import { getStoredCompanyId } from "@/lib/api"
import {
  fetchOcAuditMeta,
  postOcAuditInvestigate,
  postOcAuditLocal,
  type OcAuditFinding,
  type OcAuditMeta,
  type OcAuditReport,
  type OcAuditStatus,
} from "@/lib/distribuidora-oc-audit-api"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
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

export default function AuditoriaOcPage() {
  const [companyId, setCompanyId] = useState(3)
  const [officeId, setOfficeId] = useState(1)
  const [fromFolio, setFromFolio] = useState("")
  const [toFolio, setToFolio] = useState("")
  const [meta, setMeta] = useState<OcAuditMeta | null>(null)
  const [report, setReport] = useState<OcAuditReport | null>(null)
  const [selected, setSelected] = useState<Set<number>>(() => new Set())
  const [loadingMeta, setLoadingMeta] = useState(false)
  const [loadingLocal, setLoadingLocal] = useState(false)
  const [loadingInvestigate, setLoadingInvestigate] = useState(false)
  const [error, setError] = useState<string | null>(null)

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
            Detección de correlativos faltantes (tipo 33). El escaneo inicial es
            local; la consulta a Bsale es manual y acotada. Esta fase no recupera
            documentos.
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
        </div>

        {meta?.suggested_range?.tail_guidance ? (
          <p className="text-xs text-lq-muted">{meta.suggested_range.tail_guidance}</p>
        ) : null}
        {meta?.bounds?.max_folio != null ? (
          <p className="text-xs text-lq-faint">
            Máximo local PG: #{meta.bounds.max_folio}
            {meta.bounds.min_folio != null ? ` · Mínimo: #${meta.bounds.min_folio}` : ""}
            {" · "}
            Tipo documental 33 · Escritura deshabilitada
          </p>
        ) : null}
      </section>

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
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>
        </>
      ) : null}
    </div>
  )
}
