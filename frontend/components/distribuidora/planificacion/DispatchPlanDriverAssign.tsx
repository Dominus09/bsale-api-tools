"use client"

import { useEffect, useState } from "react"
import { Loader2 } from "lucide-react"

import {
  assignDispatchPlanDriver,
  getDispatchPlanDriver,
  listAvailableDispatchDrivers,
  type DispatchPlanDriverInfo,
  type DispatchPlanDriverOption,
} from "@/lib/api"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"

type Props = {
  planId: number
  canAssign?: boolean
  onAssigned?: () => void
}

export function DispatchPlanDriverAssign({
  planId,
  canAssign = true,
  onAssigned,
}: Props) {
  const [info, setInfo] = useState<DispatchPlanDriverInfo | null>(null)
  const [drivers, setDrivers] = useState<DispatchPlanDriverOption[]>([])
  const [driverUserId, setDriverUserId] = useState<string>("")
  const [reason, setReason] = useState("")
  const [loading, setLoading] = useState(true)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [msg, setMsg] = useState<string | null>(null)
  const [diagNote, setDiagNote] = useState<string | null>(null)
  const [nearMisses, setNearMisses] = useState<
    { user_id?: number; display_name?: string; email?: string; missing_labels?: string[] }[]
  >([])

  async function reload() {
    setLoading(true)
    setError(null)
    try {
      const [driverInfo, available] = await Promise.all([
        getDispatchPlanDriver(planId),
        canAssign
          ? listAvailableDispatchDrivers({ diagnostics: true })
          : Promise.resolve({ items: [] as DispatchPlanDriverOption[] }),
      ])
      setInfo(driverInfo)
      setDrivers(available.items)
      setDiagNote(available.diagnostics?.note_es || available.diagnostics?.requirements_summary_es || null)
      setNearMisses(available.diagnostics?.ineligible_near_misses || [])
      if (driverInfo.driver_user_id) {
        setDriverUserId(String(driverInfo.driver_user_id))
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
  }, [planId])

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
    try {
      const res = await assignDispatchPlanDriver(planId, {
        driver_user_id: uid,
        reason: reason.trim(),
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

  if (loading) {
    return (
      <div className="flex items-center gap-2 text-sm text-muted-foreground">
        <Loader2 className="size-4 animate-spin" />
        Cargando asignación de chofer…
      </div>
    )
  }

  return (
    <div className="space-y-3 rounded-md border p-3">
      <div className="flex flex-wrap items-center gap-2">
        <p className="text-sm font-medium">Chofer del despacho</p>
        {info?.driver_pending ? (
          <Badge variant="outline">Pendiente</Badge>
        ) : (
          <Badge>Asignado</Badge>
        )}
      </div>

      <p className="text-sm">
        {info?.driver?.display_name || info?.driver_name || "Sin chofer vinculado"}
        {info?.assistant_count != null ? (
          <span className="text-muted-foreground">
            {" "}
            · {info.assistant_count} peoneta(s)
          </span>
        ) : null}
      </p>

      {canAssign ? (
        <div className="grid gap-3 sm:grid-cols-[1fr_1fr_auto] sm:items-end">
          <div className="space-y-1">
            <Label htmlFor={`driver-${planId}`}>Chofer</Label>
            <select
              id={`driver-${planId}`}
              className="flex h-9 w-full rounded-md border border-input bg-transparent px-3 text-sm"
              value={driverUserId}
              onChange={(e) => setDriverUserId(e.target.value)}
              disabled={busy}
            >
              <option value="">Seleccionar…</option>
              {drivers.map((d) => (
                <option key={d.user_id} value={d.user_id}>
                  {d.display_name}
                </option>
              ))}
            </select>
            {!drivers.length ? (
              <p className="text-[11px] text-amber-800 dark:text-amber-200">
                Sin choferes elegibles. Se requiere usuario activo + PWA + perfil chofer
                (el rol ERP operativo no basta).
              </p>
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
          <Button type="button" onClick={() => void onSubmit()} disabled={busy}>
            {busy ? <Loader2 className="size-4 animate-spin" /> : "Guardar"}
          </Button>
        </div>
      ) : null}

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
          <p className="mt-1.5 text-[10px]">
            Corregir en Admin → Usuarios y Accesos: activar app PWA + perfil operacional
            «chofer» (+ empresa/oficina del despacho). El rol ERP «operativo» no alcanza.
          </p>
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
