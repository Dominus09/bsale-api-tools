"use client"

import { useEffect, useState } from "react"
import { Loader2 } from "lucide-react"

import {
  assignDispatchPlanDriver,
  getDispatchPlanDriver,
  listAvailableDispatchDrivers,
  updateDispatchPlanPeonetas,
  type DispatchPlanDriverInfo,
  type DispatchPlanDriverOption,
} from "@/lib/api"
import { formatClp } from "@/lib/ors-map-ui"
import { formatPeonetasDisplay } from "@/lib/peonetas-display"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"

/** Solo fallback para planificación simplificada SPA / Bodega Central. */
const SIMPLE_LIST_COMPANY_ID = 3
const SIMPLE_LIST_OFFICE_ID = 1

type Props = {
  planId: number
  canAssign?: boolean
  /** Si el padre conoce el origen (simple_list | ors). */
  planningOrigin?: string | null
  onAssigned?: () => void
}

function resolveScope(info: DispatchPlanDriverInfo | null, planningOrigin?: string | null) {
  if (info?.company_id != null && info?.office_id != null) {
    return { companyId: info.company_id, officeId: info.office_id }
  }
  const origin = (info?.planning_origin || planningOrigin || "").toLowerCase()
  if (origin === "simple_list") {
    return { companyId: SIMPLE_LIST_COMPANY_ID, officeId: SIMPLE_LIST_OFFICE_ID }
  }
  // ORS / desconocido: sin filtro de empresa-oficina (solo PWA+chofer).
  return { companyId: undefined as number | undefined, officeId: undefined as number | undefined }
}

export function DispatchPlanDriverAssign({
  planId,
  canAssign = true,
  planningOrigin = null,
  onAssigned,
}: Props) {
  const [info, setInfo] = useState<DispatchPlanDriverInfo | null>(null)
  const [drivers, setDrivers] = useState<DispatchPlanDriverOption[]>([])
  const [driverUserId, setDriverUserId] = useState<string>("")
  const [reason, setReason] = useState("")
  const [peonetasInput, setPeonetasInput] = useState("")
  const [peonetasReason, setPeonetasReason] = useState("")
  const [loading, setLoading] = useState(true)
  const [busy, setBusy] = useState(false)
  const [listError, setListError] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [msg, setMsg] = useState<string | null>(null)
  const [diagNote, setDiagNote] = useState<string | null>(null)
  const [nearMisses, setNearMisses] = useState<
    { user_id?: number; display_name?: string; email?: string; missing_labels?: string[] }[]
  >([])
  const [scopeLabel, setScopeLabel] = useState<string>("")

  async function reload() {
    setLoading(true)
    setError(null)
    setListError(null)
    try {
      const driverInfo = await getDispatchPlanDriver(planId)
      setInfo(driverInfo)
      if (driverInfo.driver_user_id) {
        setDriverUserId(String(driverInfo.driver_user_id))
      } else {
        setDriverUserId("")
      }
      if (driverInfo.peonetas_recorded === true && driverInfo.assistant_count != null) {
        setPeonetasInput(String(driverInfo.assistant_count))
      } else if (
        driverInfo.assistant_count != null &&
        Number(driverInfo.assistant_count) > 0
      ) {
        setPeonetasInput(String(driverInfo.assistant_count))
      } else {
        setPeonetasInput("")
      }

      if (!canAssign) {
        setDrivers([])
        setNearMisses([])
        setDiagNote(null)
        return
      }

      const scope = resolveScope(driverInfo, planningOrigin)
      setScopeLabel(
        scope.companyId != null && scope.officeId != null
          ? `empresa ${scope.companyId} · oficina ${scope.officeId}`
          : "sin filtro empresa/oficina (ORS / contexto no vinculado)",
      )

      try {
        const available = await listAvailableDispatchDrivers({
          company_id: scope.companyId,
          office_id: scope.officeId,
          diagnostics: true,
        })
        setDrivers(available.items || [])
        setDiagNote(
          available.diagnostics?.note_es ||
            available.diagnostics?.requirements_summary_es ||
            null,
        )
        setNearMisses(available.diagnostics?.ineligible_near_misses || [])
        setListError(null)
      } catch {
        setDrivers([])
        setNearMisses([])
        setDiagNote(null)
        setListError("No se pudo cargar la lista de choferes. Reintentar.")
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : "Error al cargar chofer")
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    if (!Number.isFinite(planId) || planId <= 0) return
    void reload()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [planId, canAssign, planningOrigin])

  async function onSubmit() {
    const uid = Number(driverUserId)
    if (!Number.isFinite(uid) || uid <= 0) {
      setError("Seleccione un chofer")
      return
    }
    if (reason.trim().length < 3) {
      setError("Indique un motivo (mín. 3 caracteres)")
      return
    }
    setBusy(true)
    setError(null)
    setMsg(null)
    const scope = resolveScope(info, planningOrigin)
    try {
      const res = await assignDispatchPlanDriver(planId, {
        driver_user_id: uid,
        reason: reason.trim(),
        company_id: scope.companyId,
        office_id: scope.officeId,
      })
      const conflictN = res.same_day_conflicts?.length ?? 0
      setMsg(
        conflictN > 0
          ? `Chofer ${res.status}. Aviso: ${conflictN} despacho(s) activo(s) el mismo día.`
          : `Chofer ${res.status}.`,
      )
      setReason("")
      await reload()
      onAssigned?.()
    } catch (e) {
      setError(e instanceof Error ? e.message : "No se pudo asignar")
    } finally {
      setBusy(false)
    }
  }

  async function onSavePeonetas() {
    const raw = peonetasInput.trim()
    if (!/^\d+$/.test(raw)) {
      setError("Cant. de peonetas debe ser un entero (no vacío).")
      return
    }
    const n = Number(raw)
    if (!Number.isInteger(n) || n < 0 || n > 10) {
      setError("Cant. de peonetas debe estar entre 0 y 10.")
      return
    }
    if (peonetasReason.trim().length < 3) {
      setError("Indique un motivo para el cambio de peonetas (mín. 3 caracteres)")
      return
    }
    setBusy(true)
    setError(null)
    setMsg(null)
    try {
      const res = await updateDispatchPlanPeonetas(planId, {
        assistant_count: n,
        reason: peonetasReason.trim(),
      })
      const costNote =
        res.peonetas_cost_clp != null
          ? ` · costo ${formatClp(res.peonetas_cost_clp)}`
          : ""
      const deltaNote =
        res.cost_delta_clp != null ? ` · Δ ${formatClp(res.cost_delta_clp)}` : ""
      setMsg(
        `Peonetas ${res.status}: ${n}${costNote}${deltaNote}` +
          (res.cost_note_es ? ` · ${res.cost_note_es}` : ""),
      )
      setPeonetasReason("")
      await reload()
      onAssigned?.()
    } catch (e) {
      setError(e instanceof Error ? e.message : "No se pudo guardar peonetas")
    } finally {
      setBusy(false)
    }
  }

  if (loading) {
    return (
      <div className="flex items-center gap-2 text-sm text-muted-foreground">
        <Loader2 className="size-4 animate-spin" />
        Cargando asignación de chofer…
      </div>
    )
  }

  const hasLinkedDriver = Boolean(
    info?.driver_user_id || info?.driver?.user_id || (info?.driver_name || "").trim(),
  )
  const isPending = info == null || info.driver_pending === true || !hasLinkedDriver

  return (
    <div className="space-y-3 rounded-md border p-3">
      <div className="flex flex-wrap items-center gap-2">
        <p className="text-sm font-medium">Chofer del despacho</p>
        {isPending ? (
          <Badge variant="outline">Pendiente</Badge>
        ) : (
          <Badge>Asignado</Badge>
        )}
      </div>

      <p className="text-sm">
        {info?.driver?.display_name || info?.driver_name || "Sin chofer vinculado"}
        <span className="text-muted-foreground">
          {" "}
          · Peonetas:{" "}
          {formatPeonetasDisplay({
            assistant_count: info?.assistant_count,
            peonetas_recorded: info?.peonetas_recorded,
          })}
        </span>
      </p>

      {canAssign ? (
        <div className="grid gap-3 sm:grid-cols-[1fr_1fr_auto] sm:items-end">
          <div className="space-y-1">
            <Label htmlFor={`driver-${planId}`}>Chofer</Label>
            <select
              id={`driver-${planId}`}
              className="driver-chofer-native-select flex h-9 w-full rounded-md border border-input bg-transparent px-3 text-sm"
              value={driverUserId}
              onChange={(e) => setDriverUserId(e.target.value)}
              disabled={busy || Boolean(listError)}
            >
              <option value="">Seleccionar…</option>
              {drivers.map((d) => (
                <option key={d.user_id} value={d.user_id}>
                  {d.display_name}
                </option>
              ))}
            </select>
            {listError ? (
              <div className="flex flex-wrap items-center gap-2">
                <p className="text-[11px] text-destructive">{listError}</p>
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  className="h-7 text-[11px]"
                  onClick={() => void reload()}
                  disabled={busy}
                >
                  Reintentar
                </Button>
              </div>
            ) : !drivers.length ? (
              <p className="text-[11px] text-amber-800 dark:text-amber-200">
                No hay choferes habilitados para esta empresa y oficina.
              </p>
            ) : null}
            {scopeLabel ? (
              <p className="text-[10px] text-muted-foreground">Filtro: {scopeLabel}</p>
            ) : null}
          </div>
          <div className="space-y-1">
            <Label htmlFor={`reason-${planId}`}>Motivo</Label>
            <Input
              id={`reason-${planId}`}
              value={reason}
              onChange={(e) => setReason(e.target.value)}
              placeholder="Asignación / reasignación…"
              disabled={busy}
            />
          </div>
          <Button type="button" onClick={() => void onSubmit()} disabled={busy || Boolean(listError)}>
            {busy ? <Loader2 className="size-4 animate-spin" /> : "Guardar"}
          </Button>
        </div>
      ) : null}

      <div className="space-y-2 border-t border-border/60 pt-3">
        <p className="text-sm font-medium">Cantidad de peonetas</p>
        <p className="text-xs text-muted-foreground">
          Actual:{" "}
          {formatPeonetasDisplay({
            assistant_count: info?.assistant_count,
            peonetas_recorded: info?.peonetas_recorded,
          })}
          {info?.peonetas_unit_cost_clp != null
            ? ` · tarifa ${formatClp(info.peonetas_unit_cost_clp)} / peoneta`
            : ""}
          {info?.peonetas_cost_clp != null
            ? ` · costo ${formatClp(info.peonetas_cost_clp)}`
            : info?.peonetas_recorded
              ? ""
              : " · costo pendiente hasta informar cantidad"}
        </p>
        {canAssign && info?.peonetas_editable !== false ? (
          <div className="grid gap-3 sm:grid-cols-[120px_1fr_auto] sm:items-end">
            <div className="space-y-1">
              <Label htmlFor={`peonetas-${planId}`}>Cantidad</Label>
              <Input
                id={`peonetas-${planId}`}
                type="number"
                inputMode="numeric"
                min={0}
                max={10}
                step={1}
                className="h-9"
                value={peonetasInput}
                onChange={(e) => setPeonetasInput(e.target.value)}
                placeholder="0–10"
                disabled={busy}
              />
            </div>
            <div className="space-y-1">
              <Label htmlFor={`peonetas-reason-${planId}`}>Motivo</Label>
              <Input
                id={`peonetas-reason-${planId}`}
                value={peonetasReason}
                onChange={(e) => setPeonetasReason(e.target.value)}
                placeholder="Ajuste operativo…"
                disabled={busy}
              />
            </div>
            <Button
              type="button"
              onClick={() => void onSavePeonetas()}
              disabled={busy || !/^\d+$/.test(peonetasInput.trim())}
            >
              {busy ? <Loader2 className="size-4 animate-spin" /> : "Guardar peonetas"}
            </Button>
          </div>
        ) : (
          <p className="text-xs text-muted-foreground">
            {info?.peonetas_editable === false
              ? "El estado del despacho no permite modificar peonetas."
              : "Sin permiso de edición."}
          </p>
        )}
        {info?.peonetas_history?.length ? (
          <ul className="max-h-28 space-y-1 overflow-auto text-xs text-muted-foreground">
            {info.peonetas_history.map((h) => (
              <li key={h.id}>
                {(h.created_at || "").toString().slice(0, 19)} ·{" "}
                {h.previous_assistant_count ?? "—"} → {h.new_assistant_count ?? "—"}
                {h.cost_delta_clp != null
                  ? ` · Δ ${formatClp(h.cost_delta_clp)}`
                  : ""}{" "}
                · {h.actor_name || "?"} · {h.reason || ""}
              </li>
            ))}
          </ul>
        ) : null}
      </div>

      {error ? <p className="text-sm text-destructive">{error}</p> : null}
      {msg ? <p className="text-sm text-muted-foreground">{msg}</p> : null}
      {diagNote ? <p className="text-[11px] text-muted-foreground">{diagNote}</p> : null}
      {nearMisses.length ? (
        <div className="rounded-md border border-dashed px-2 py-1.5 text-[11px] text-muted-foreground">
          <p className="font-medium">Usuarios cercanos no elegibles</p>
          <ul className="mt-1 list-inside list-disc">
            {nearMisses.slice(0, 6).map((n, i) => (
              <li key={i}>
                {n.display_name || n.email || "Usuario"}
                {n.user_id != null ? ` (#${n.user_id})` : ""}:{" "}
                {(n.missing_labels || []).join("; ") || "requisito pendiente"}
              </li>
            ))}
          </ul>
        </div>
      ) : null}

      {info?.history?.length ? (
        <div className="space-y-1">
          <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
            Historial
          </p>
          <ul className="max-h-40 space-y-1 overflow-auto text-xs text-muted-foreground">
            {info.history.map((h) => (
              <li key={h.id}>
                {(h.created_at || "").toString().slice(0, 19)} ·{" "}
                {h.previous_driver_name || "—"} → {h.new_driver_name || "—"} ·{" "}
                {h.actor_name || "?"} · {h.reason || ""}
              </li>
            ))}
          </ul>
        </div>
      ) : null}
    </div>
  )
}
