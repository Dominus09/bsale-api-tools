"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import { AlertTriangle, Loader2, Plus, RefreshCw, Save } from "lucide-react"

import {
  createDispatchPlanLoadBatch,
  getDispatchPlanLoadOrganization,
  saveDispatchPlanLoadOrganization,
  updateDispatchPlanLoadBatch,
  type DispatchPlanLoadBatch,
  type DispatchPlanLoadOrganizationResponse,
  type LoadOrganizationClientRow,
} from "@/lib/api"
import { formatClp } from "@/lib/ors-map-ui"
import {
  clientSelectionKey,
  encodeBatchRoleDescription,
  priorityReviewNeeded,
  sortLoadOrgClients,
  stripBatchRoleDescription,
  type LoadOrgSortKey,
} from "@/lib/load-organization-ui"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Badge } from "@/components/ui/badge"
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
import { cn } from "@/lib/utils"

type Props = {
  planId: number
  onMessage: (msg: string) => void
}

function fmtNum(n: number | null | undefined, digits = 1): string {
  if (n == null || Number.isNaN(Number(n))) return "—"
  return Number(n).toLocaleString("es-CL", { maximumFractionDigits: digits })
}

function limitationLabel(code: string): string {
  switch (code) {
    case "cajas_parciales":
      return "cajas parciales"
    case "cajas_no_disponibles":
      return "sin cajas confiables"
    case "cajas_desde_snapshot_plan":
      return "cajas del snapshot del plan"
    case "peso_cobertura_incompleta":
      return "peso incompleto"
    case "peso_no_disponible":
      return "sin peso"
    case "sku_estimado_por_lineas":
      return "SKU no consolidable"
    case "units_missing":
      return "unidades incompletas"
    default:
      return code
  }
}

export function DispatchPlanLoadOrganizationPanel({ planId, onMessage }: Props) {
  const [loading, setLoading] = useState(true)
  const [busy, setBusy] = useState<string | null>(null)
  const [data, setData] = useState<DispatchPlanLoadOrganizationResponse | null>(null)
  const [draftByClient, setDraftByClient] = useState<Record<string, number | null>>({})
  const [selected, setSelected] = useState<Set<string>>(() => new Set())
  const [sortKey, setSortKey] = useState<LoadOrgSortKey>("venta_desc")
  const [bulkBatchId, setBulkBatchId] = useState<string>("")
  const [newName, setNewName] = useState("")
  const [newRole, setNewRole] = useState<"priority" | "consolidated">("priority")
  const [renameId, setRenameId] = useState<string>("")
  const [renameValue, setRenameValue] = useState("")

  const applyResponse = useCallback((res: DispatchPlanLoadOrganizationResponse) => {
    setData(res)
    const draft: Record<string, number | null> = {}
    for (const c of res.clients || []) {
      draft[clientSelectionKey(c)] = c.load_batch_id ?? null
    }
    setDraftByClient(draft)
    setSelected(new Set())
  }, [])

  const load = useCallback(async () => {
    setLoading(true)
    try {
      const res = await getDispatchPlanLoadOrganization(planId)
      applyResponse(res)
    } catch (e) {
      onMessage(e instanceof Error ? e.message : "Error al cargar organización")
    } finally {
      setLoading(false)
    }
  }, [planId, applyResponse, onMessage])

  useEffect(() => {
    void load()
  }, [load])

  const schemaReady = Boolean(data?.schema_ready)
  const clients = data?.clients || []
  const batches = data?.batches || []
  const sortedClients = useMemo(
    () => sortLoadOrgClients(clients, sortKey),
    [clients, sortKey],
  )

  const dirty = useMemo(() => {
    if (!data) return false
    for (const c of data.clients || []) {
      const key = clientSelectionKey(c)
      const draft = draftByClient[key] ?? null
      const saved = c.load_batch_id ?? null
      if (draft !== saved) return true
    }
    return false
  }, [data, draftByClient])

  const groupCards = useMemo(() => {
    return (data?.group_summaries || []).map((g) => {
      // Recalcular conteo de clientes desde el draft para advertencia inmediata.
      const draftMembers = (data?.clients || []).filter((c) => {
        const key = clientSelectionKey(c)
        return (draftByClient[key] ?? null) === g.load_batch_id
      })
      const role =
        g.presentation_role ||
        batches.find((b) => b.id === g.load_batch_id)?.presentation_role ||
        null
      return {
        ...g,
        draft_client_count: draftMembers.length,
        draft_clients: draftMembers,
        draft_priority_warning: priorityReviewNeeded(role, draftMembers.length),
      }
    })
  }, [data, draftByClient, batches])

  const setClientBatch = (c: LoadOrganizationClientRow, batchId: number | null) => {
    const key = clientSelectionKey(c)
    setDraftByClient((prev) => ({ ...prev, [key]: batchId }))
  }

  const toggleSelected = (key: string, on: boolean) => {
    setSelected((prev) => {
      const next = new Set(prev)
      if (on) next.add(key)
      else next.delete(key)
      return next
    })
  }

  const allVisibleSelected =
    sortedClients.length > 0 &&
    sortedClients.every((c) => selected.has(clientSelectionKey(c)))

  const applyBulk = () => {
    if (!bulkBatchId || !selected.size) {
      onMessage("Seleccione clientes y un grupo destino")
      return
    }
    const batchId = Number(bulkBatchId)
    setDraftByClient((prev) => {
      const next = { ...prev }
      for (const key of selected) next[key] = batchId
      return next
    })
    onMessage(`Asignación masiva pendiente de guardar (${selected.size} clientes)`)
  }

  const createGroup = async () => {
    const name = newName.trim()
    if (!name) {
      onMessage("Indique un nombre para el grupo")
      return
    }
    setBusy("create")
    try {
      await createDispatchPlanLoadBatch(planId, {
        name,
        description: encodeBatchRoleDescription(newRole),
      })
      setNewName("")
      await load()
      onMessage(`Grupo «${name}» creado (${newRole === "priority" ? "prioritario" : "consolidado"})`)
    } catch (e) {
      onMessage(e instanceof Error ? e.message : "No se pudo crear el grupo")
    } finally {
      setBusy(null)
    }
  }

  const renameGroup = async () => {
    const id = Number(renameId)
    const name = renameValue.trim()
    if (!id || !name) return
    const batch = batches.find((b) => b.id === id)
    if (!batch) return
    setBusy("rename")
    try {
      await updateDispatchPlanLoadBatch(planId, id, {
        name,
        // Solo nota humana: el backend conserva role=* existente.
        description: stripBatchRoleDescription(batch.description) || undefined,
      })
      setRenameId("")
      setRenameValue("")
      await load()
      onMessage("Nombre de grupo actualizado")
    } catch (e) {
      onMessage(e instanceof Error ? e.message : "No se pudo renombrar")
    } finally {
      setBusy(null)
    }
  }

  const setBatchRole = async (
    batch: DispatchPlanLoadBatch,
    role: "priority" | "consolidated",
  ) => {
    setBusy(`role-${batch.id}`)
    try {
      await updateDispatchPlanLoadBatch(planId, batch.id, {
        name: batch.name,
        description: encodeBatchRoleDescription(
          role,
          stripBatchRoleDescription(batch.description),
        ),
      })
      await load()
    } catch (e) {
      onMessage(e instanceof Error ? e.message : "No se pudo actualizar el tipo de grupo")
    } finally {
      setBusy(null)
    }
  }

  const save = async (markOrganized: boolean) => {
    if (!data || !schemaReady) return
    const assignments: { oc_document_id: number; load_batch_id: number }[] = []
    for (const c of data.clients) {
      const key = clientSelectionKey(c)
      const batchId = draftByClient[key]
      if (batchId == null) continue
      for (const ocId of c.oc_document_ids) {
        assignments.push({ oc_document_id: ocId, load_batch_id: batchId })
      }
    }
    setBusy(markOrganized ? "organize" : "save")
    try {
      const res = await saveDispatchPlanLoadOrganization(planId, {
        assignments,
        organization_version: data.load_organization_version,
        mark_organized: markOrganized,
      })
      applyResponse(res)
      onMessage(
        markOrganized
          ? "Organización marcada como completa"
          : "Distribución guardada (borrador)",
      )
    } catch (e) {
      onMessage(e instanceof Error ? e.message : "Error al guardar")
      if (e instanceof Error && /versión|concurrencia|409/i.test(e.message)) {
        await load()
      }
    } finally {
      setBusy(null)
    }
  }

  if (loading && !data) {
    return (
      <div className="flex items-center gap-2 text-sm text-muted-foreground">
        <Loader2 className="h-4 w-4 animate-spin" /> Cargando organización de carga…
      </div>
    )
  }

  return (
    <div className="space-y-4">
      <div className="flex flex-col gap-2 sm:flex-row sm:items-start sm:justify-between">
        <div className="min-w-0 space-y-1">
          <h3 className="text-sm font-semibold">Organizar carga</h3>
          <p className="max-w-3xl text-xs text-muted-foreground">
            Agrupe clientes para facilitar la descarga física. La decisión es manual: un
            grupo prioritario suele ser 1–2 clientes; el resto de la ruta puede ir en un
            grupo consolidado. No se altera el orden de entrega. El campo{" "}
            <code className="text-[10px]">sort_order</code> ordena la preparación en UI;
            el tipo prioritario/consolidado guía la descarga temprana — no confundir con
            orden de estiba (aún no modelado).
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <Badge variant="outline">
            {data?.load_organization_status || "legacy"}
            {data != null ? ` · v${data.load_organization_version}` : ""}
          </Badge>
          <Button
            type="button"
            size="sm"
            variant="outline"
            onClick={() => void load()}
            disabled={busy != null}
          >
            <RefreshCw className="mr-1.5 h-3.5 w-3.5" /> Actualizar
          </Button>
        </div>
      </div>

      {!schemaReady ? (
        <Alert>
          <AlertTriangle className="h-4 w-4" />
          <AlertTitle>Migración 053 pendiente</AlertTitle>
          <AlertDescription>
            {data?.message ||
              "La organización anticipada no está disponible en este entorno. Los planes legacy siguen operativos."}
          </AlertDescription>
        </Alert>
      ) : null}

      {schemaReady ? (
        <>
          <div className="flex flex-wrap items-end gap-2 rounded-lg border bg-muted/15 p-3">
            <div className="space-y-1">
              <Label className="text-xs">Nuevo grupo</Label>
              <Input
                className="h-8 w-[200px]"
                value={newName}
                onChange={(e) => setNewName(e.target.value)}
                placeholder="Ej. Prioritario Hardy"
                disabled={!schemaReady || busy != null}
              />
            </div>
            <div className="space-y-1">
              <Label className="text-xs">Tipo</Label>
              <Select
                value={newRole}
                onValueChange={(v) => setNewRole(v as "priority" | "consolidated")}
              >
                <SelectTrigger className="h-8 w-[160px]">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="priority">Prioritario (1–2)</SelectItem>
                  <SelectItem value="consolidated">Consolidado (ruta)</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <Button
              size="sm"
              onClick={() => void createGroup()}
              disabled={!schemaReady || busy != null || !newName.trim()}
            >
              {busy === "create" ? (
                <Loader2 className="mr-1.5 h-3.5 w-3.5 animate-spin" />
              ) : (
                <Plus className="mr-1.5 h-3.5 w-3.5" />
              )}
              Crear grupo
            </Button>
          </div>

          <div className="flex flex-wrap items-end gap-3 rounded-lg border p-3">
            <div className="space-y-1">
              <Label className="text-xs">Ordenar clientes</Label>
              <Select
                value={sortKey}
                onValueChange={(v) => setSortKey(v as LoadOrgSortKey)}
              >
                <SelectTrigger className="h-8 w-[200px]">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="venta_desc">Venta (mayor)</SelectItem>
                  <SelectItem value="sku_desc">SKU distintos</SelectItem>
                  <SelectItem value="cajas_desc">Cajas equivalentes</SelectItem>
                  <SelectItem value="peso_desc">Peso conocido</SelectItem>
                  <SelectItem value="nombre_asc">Nombre</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div className="text-xs text-muted-foreground">
              {selected.size} seleccionados · solo visual, no cambia la ruta
            </div>
            <div className="space-y-1">
              <Label className="text-xs">Asignar a grupo</Label>
              <Select value={bulkBatchId || undefined} onValueChange={setBulkBatchId}>
                <SelectTrigger className="h-8 w-[200px]">
                  <SelectValue placeholder="Elegir grupo" />
                </SelectTrigger>
                <SelectContent>
                  {batches.map((b) => (
                    <SelectItem key={b.id} value={String(b.id)}>
                      {b.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <Button
              size="sm"
              variant="secondary"
              onClick={applyBulk}
              disabled={!selected.size || !bulkBatchId}
            >
              Asignar seleccionados
            </Button>
            <Button
              size="sm"
              variant="outline"
              onClick={() => setSelected(new Set())}
              disabled={!selected.size}
            >
              Limpiar selección
            </Button>
          </div>

          <div className="overflow-x-auto rounded-lg border">
            <table className="w-full min-w-[980px] text-left text-sm">
              <thead className="border-b bg-muted/40 text-xs uppercase text-muted-foreground">
                <tr>
                  <th className="w-10 px-2 py-2">
                    <Checkbox
                      checked={allVisibleSelected}
                      onCheckedChange={(v) => {
                        if (v === true) {
                          setSelected(
                            new Set(sortedClients.map((c) => clientSelectionKey(c))),
                          )
                        } else {
                          setSelected(new Set())
                        }
                      }}
                      aria-label="Seleccionar todos"
                    />
                  </th>
                  <th className="px-2 py-2">Cliente</th>
                  <th className="px-2 py-2">OC</th>
                  <th className="px-2 py-2">Venta</th>
                  <th className="px-2 py-2">SKU</th>
                  <th className="px-2 py-2">Unidades</th>
                  <th className="px-2 py-2">Cajas</th>
                  <th className="px-2 py-2">Peso / cob.</th>
                  <th className="px-2 py-2">Grupo</th>
                </tr>
              </thead>
              <tbody>
                {sortedClients.map((c) => {
                  const key = clientSelectionKey(c)
                  const draft = draftByClient[key] ?? null
                  const limits = (c.limitations || []).map(limitationLabel)
                  return (
                    <tr key={key} className="border-b align-middle">
                      <td className="px-2 py-1.5">
                        <Checkbox
                          checked={selected.has(key)}
                          onCheckedChange={(v) => toggleSelected(key, v === true)}
                          aria-label={`Seleccionar ${c.client_name}`}
                        />
                      </td>
                      <td className="px-2 py-1.5">
                        <div
                          className="max-w-[220px] truncate font-medium"
                          title={c.client_name}
                        >
                          {c.client_name}
                        </div>
                        {limits.length ? (
                          <div className="text-[11px] text-amber-800 dark:text-amber-200">
                            {limits.join(" · ")}
                          </div>
                        ) : null}
                      </td>
                      <td className="px-2 py-1.5 text-xs">
                        <div className="font-medium">{c.oc_count}</div>
                        <div className="max-w-[140px] truncate text-muted-foreground">
                          {(c.folios || []).join(", ") || "—"}
                        </div>
                      </td>
                      <td className="px-2 py-1.5 tabular-nums text-xs">
                        {formatClp(c.venta_oc_clp)}
                      </td>
                      <td className="px-2 py-1.5 tabular-nums">{c.sku_distinct}</td>
                      <td className="px-2 py-1.5 tabular-nums">{fmtNum(c.units, 0)}</td>
                      <td className="px-2 py-1.5 text-xs">
                        {c.cajas_equivalentes != null ? (
                          <>
                            <span className="tabular-nums">
                              {fmtNum(c.cajas_equivalentes, 2)}
                            </span>
                            {c.cajas_partial ? (
                              <span className="ml-1 text-amber-800 dark:text-amber-200">
                                parcial
                              </span>
                            ) : null}
                          </>
                        ) : (
                          <span className="text-muted-foreground">—</span>
                        )}
                      </td>
                      <td className="px-2 py-1.5 text-xs">
                        <div className="tabular-nums">
                          {fmtNum(c.peso_conocido_kg, 1)} kg
                        </div>
                        <div className="text-muted-foreground">
                          {c.peso_coverage_percent != null
                            ? `${fmtNum(c.peso_coverage_percent, 1)}%`
                            : "—"}
                        </div>
                      </td>
                      <td className="px-2 py-1.5">
                        <Select
                          value={draft != null ? String(draft) : "__none__"}
                          onValueChange={(v) =>
                            setClientBatch(c, v === "__none__" ? null : Number(v))
                          }
                          disabled={!schemaReady}
                        >
                          <SelectTrigger className="h-8 w-[180px]">
                            <SelectValue placeholder="Sin grupo" />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="__none__">Sin grupo</SelectItem>
                            {batches.map((b) => (
                              <SelectItem key={b.id} value={String(b.id)}>
                                {b.name}
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                      </td>
                    </tr>
                  )
                })}
                {!sortedClients.length ? (
                  <tr>
                    <td
                      colSpan={9}
                      className="px-3 py-6 text-center text-sm text-muted-foreground"
                    >
                      Este plan aún no tiene OC para organizar.
                    </td>
                  </tr>
                ) : null}
              </tbody>
            </table>
          </div>

          <div className="flex flex-wrap gap-2">
            <Button
              onClick={() => void save(false)}
              disabled={!schemaReady || busy != null || !dirty}
            >
              {busy === "save" ? (
                <Loader2 className="mr-1.5 h-4 w-4 animate-spin" />
              ) : (
                <Save className="mr-1.5 h-4 w-4" />
              )}
              Guardar distribución
            </Button>
            <Button
              variant="secondary"
              onClick={() => void save(true)}
              disabled={
                !schemaReady ||
                busy != null ||
                !data?.clients?.length ||
                (data.clients || []).some(
                  (c) => (draftByClient[clientSelectionKey(c)] ?? null) == null,
                )
              }
            >
              {busy === "organize" ? (
                <Loader2 className="mr-1.5 h-4 w-4 animate-spin" />
              ) : null}
              Marcar organización completa
            </Button>
            <p className="self-center text-xs text-muted-foreground">
              Completas: {data?.completeness.assigned_orders ?? 0}/
              {data?.completeness.total_orders ?? 0} OC
              {dirty ? " · hay cambios sin guardar" : ""}
            </p>
          </div>

          <div className="space-y-3">
            <div className="flex flex-wrap items-end gap-2">
              <h4 className="text-sm font-semibold">Resumen por grupo</h4>
              <div className="ml-auto flex flex-wrap items-end gap-2">
                <Select value={renameId || undefined} onValueChange={setRenameId}>
                  <SelectTrigger className="h-8 w-[180px]">
                    <SelectValue placeholder="Renombrar grupo…" />
                  </SelectTrigger>
                  <SelectContent>
                    {batches.map((b) => (
                      <SelectItem key={b.id} value={String(b.id)}>
                        {b.name}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                <Input
                  className="h-8 w-[180px]"
                  value={renameValue}
                  onChange={(e) => setRenameValue(e.target.value)}
                  placeholder="Nuevo nombre"
                  disabled={!renameId}
                />
                <Button
                  size="sm"
                  variant="outline"
                  disabled={!renameId || !renameValue.trim() || busy != null}
                  onClick={() => void renameGroup()}
                >
                  Aplicar nombre
                </Button>
              </div>
            </div>

            <div className="grid gap-3 lg:grid-cols-2">
              {groupCards.map((g) => {
                const batch = batches.find((b) => b.id === g.load_batch_id)
                const role = batch?.presentation_role || g.presentation_role
                return (
                  <div
                    key={g.load_batch_id}
                    className={cn(
                      "space-y-2 rounded-lg border p-3",
                      role === "priority" && "border-sky-500/40 bg-sky-500/5",
                      role === "consolidated" && "border-border bg-muted/10",
                      g.draft_priority_warning && "border-amber-500/50",
                    )}
                  >
                    <div className="flex flex-wrap items-start justify-between gap-2">
                      <div>
                        <div className="font-medium">{g.name || `Grupo #${g.load_batch_id}`}</div>
                        <div className="text-xs text-muted-foreground">
                          {g.draft_client_count} clientes · {g.oc_count} OC
                        </div>
                      </div>
                      <div className="flex flex-wrap gap-1">
                        <Badge variant={role === "priority" ? "default" : "outline"}>
                          {role === "priority"
                            ? "Prioritario"
                            : role === "consolidated"
                              ? "Consolidado"
                              : "Sin tipo"}
                        </Badge>
                        {batch ? (
                          <>
                            <Button
                              type="button"
                              size="sm"
                              variant="ghost"
                              className="h-7 px-2 text-[11px]"
                              disabled={busy != null}
                              onClick={() => void setBatchRole(batch, "priority")}
                            >
                              Prioritario
                            </Button>
                            <Button
                              type="button"
                              size="sm"
                              variant="ghost"
                              className="h-7 px-2 text-[11px]"
                              disabled={busy != null}
                              onClick={() => void setBatchRole(batch, "consolidated")}
                            >
                              Consolidado
                            </Button>
                          </>
                        ) : null}
                      </div>
                    </div>
                    {g.draft_priority_warning ? (
                      <p className="text-xs text-amber-800 dark:text-amber-200">
                        Revisión operativa: este grupo prioritario tiene más de 2 clientes.
                        No se bloquea el guardado.
                      </p>
                    ) : null}
                    <dl className="grid grid-cols-2 gap-x-3 gap-y-1 text-xs sm:grid-cols-3">
                      <div>
                        <dt className="text-muted-foreground">Venta</dt>
                        <dd className="font-medium">{formatClp(g.venta_oc_clp)}</dd>
                      </div>
                      <div>
                        <dt className="text-muted-foreground">SKU distintos</dt>
                        <dd className="font-medium tabular-nums">{g.sku_distinct}</dd>
                      </div>
                      <div>
                        <dt className="text-muted-foreground">Unidades</dt>
                        <dd className="tabular-nums">{fmtNum(g.units, 0)}</dd>
                      </div>
                      <div>
                        <dt className="text-muted-foreground">Cajas eq.</dt>
                        <dd className="tabular-nums">
                          {g.cajas_equivalentes != null
                            ? fmtNum(g.cajas_equivalentes, 2)
                            : "—"}
                        </dd>
                      </div>
                      <div>
                        <dt className="text-muted-foreground">Peso conocido</dt>
                        <dd className="tabular-nums">{fmtNum(g.peso_conocido_kg, 1)} kg</dd>
                      </div>
                      <div>
                        <dt className="text-muted-foreground">Cobertura</dt>
                        <dd>
                          {g.peso_coverage_incomplete ? "incompleta / parcial" : "ok / N/D"}
                        </dd>
                      </div>
                    </dl>
                    <ul className="max-h-28 list-disc overflow-y-auto pl-4 text-xs">
                      {(g.draft_clients.length ? g.draft_clients : g.clients).map((c) => (
                        <li key={`${g.load_batch_id}-${c.client_id ?? c.client_name}`}>
                          {c.client_name}
                          {c.oc_count != null ? ` · ${c.oc_count} OC` : ""}
                          {c.sku_distinct != null ? ` · ${c.sku_distinct} SKU` : ""}
                        </li>
                      ))}
                      {!g.draft_clients.length && !g.clients.length ? (
                        <li className="list-none text-muted-foreground">Sin clientes</li>
                      ) : null}
                    </ul>
                  </div>
                )
              })}
              {!groupCards.length ? (
                <p className="text-xs text-muted-foreground">
                  Cree grupos para comenzar. No hay asignación automática por monto.
                </p>
              ) : null}
            </div>
          </div>
        </>
      ) : null}
    </div>
  )
}
