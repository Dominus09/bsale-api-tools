"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import Link from "next/link"
import { Loader2, Truck } from "lucide-react"
import { useRouter } from "next/navigation"

import { useDistribuidoraPlanning } from "@/context/distribuidora-planning-selection"
import {
  getDistribuidoraPurchaseByDocumentIds,
  postDistribuidoraRoutePlanningBatch,
  type DistribuidoraPlanningPreviewItem,
} from "@/lib/api"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { cn } from "@/lib/utils"

const TRUCKS = ["HINO 2", "HINO 3", "HINO 4", "HYUNDAI"] as const
const DEFAULT_TRUCK = TRUCKS[0]

function localIsoDate(d = new Date()): string {
  const y = d.getFullYear()
  const m = String(d.getMonth() + 1).padStart(2, "0")
  const day = String(d.getDate()).padStart(2, "0")
  return `${y}-${m}-${day}`
}

function formatCLP(n: number): string {
  return n.toLocaleString("es-CL", {
    style: "currency",
    currency: "CLP",
    maximumFractionDigits: 0,
  })
}

function comunaLabel(r: DistribuidoraPlanningPreviewItem): string {
  const t = (r.municipality ?? "").trim()
  return t || "—"
}

function aggregateRows(rows: DistribuidoraPlanningPreviewItem[]) {
  const clients = new Set<number>()
  for (const r of rows) {
    const c = r.client_id
    if (c != null && Number.isFinite(Number(c))) clients.add(Number(c))
  }
  const amount = rows.reduce((s, r) => s + Number(r.total_amount ?? 0), 0)
  return { clientCount: clients.size, amount }
}

export default function DistribuidoraPlanningPage() {
  const router = useRouter()
  const {
    planningDocumentIdsArray,
    clearPlanningDocuments,
  } = useDistribuidoraPlanning()

  const [planningDate, setPlanningDate] = useState(() => localIsoDate())
  const [preview, setPreview] = useState<DistribuidoraPlanningPreviewItem[]>([])
  const [truckByDoc, setTruckByDoc] = useState<Record<number, string>>({})
  const [loading, setLoading] = useState(false)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [success, setSuccess] = useState<string | null>(null)

  const loadPreview = useCallback(async () => {
    const ids = planningDocumentIdsArray
    if (ids.length === 0) {
      setPreview([])
      setTruckByDoc({})
      return
    }
    setLoading(true)
    setError(null)
    try {
      const res = await getDistribuidoraPurchaseByDocumentIds({ documentIds: ids })
      setPreview(res.items)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Error al cargar órdenes")
      setPreview([])
    } finally {
      setLoading(false)
    }
  }, [planningDocumentIdsArray])

  useEffect(() => {
    void loadPreview()
  }, [loadPreview])

  useEffect(() => {
    setTruckByDoc((prev) => {
      const next: Record<number, string> = { ...prev }
      for (const r of preview) {
        if (next[r.document_id] === undefined) next[r.document_id] = DEFAULT_TRUCK
      }
      for (const k of Object.keys(next)) {
        const id = Number(k)
        if (!preview.some((r) => r.document_id === id)) delete next[id]
      }
      return next
    })
  }, [preview])

  const missingDocumentIds = useMemo(() => {
    const loaded = new Set(preview.map((r) => r.document_id))
    return planningDocumentIdsArray.filter((id) => !loaded.has(id))
  }, [preview, planningDocumentIdsArray])

  const trucksWithRows = useMemo(() => {
    const used = new Set<string>()
    for (const r of preview) {
      used.add(truckByDoc[r.document_id] ?? DEFAULT_TRUCK)
    }
    return TRUCKS.filter((t) => used.has(t))
  }, [preview, truckByDoc])

  const rowsByTruck = useCallback(
    (truck: string) =>
      preview
        .filter((r) => (truckByDoc[r.document_id] ?? DEFAULT_TRUCK) === truck)
        .sort(
          (a, b) =>
            (Number(a.oc_number) || 0) - (Number(b.oc_number) || 0),
        ),
    [preview, truckByDoc],
  )

  const setTruck = (documentId: number, truck: string) => {
    setTruckByDoc((prev) => ({ ...prev, [documentId]: truck }))
  }

  const onConfirm = async () => {
    if (preview.length === 0) return
    setSaving(true)
    setError(null)
    setSuccess(null)
    try {
      const assignments = preview.map((r) => ({
        document_id: r.document_id,
        truck: truckByDoc[r.document_id] ?? DEFAULT_TRUCK,
      }))
      const res = await postDistribuidoraRoutePlanningBatch({
        planning_date: planningDate,
        assignments,
      })
      setSuccess(
        `Se planificaron ${res.inserted} OC para el ${planningDate}. Total: ${formatCLP(res.total_amount)} · ${res.total_clients} clientes.`,
      )
      clearPlanningDocuments()
      setPreview([])
      setTruckByDoc({})
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Error al guardar")
    } finally {
      setSaving(false)
    }
  }

  if (planningDocumentIdsArray.length === 0 && preview.length === 0 && !loading) {
    return (
      <div className="mx-auto flex max-w-lg flex-col gap-6 py-8">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">Planificación de rutas</h1>
          <p className="mt-2 text-sm text-muted-foreground">
            No hay órdenes en la cola. Selecciónelas en{" "}
            <Link href="/distribuidora/orders" className="font-medium text-primary underline">
              Órdenes de compra
            </Link>{" "}
            y añádalas a la cola de planificación.
          </p>
        </div>
        <Button asChild variant="outline">
          <Link href="/distribuidora/orders">Ir a órdenes</Link>
        </Button>
      </div>
    )
  }

  return (
    <div className="mx-auto flex max-w-[1200px] flex-col gap-6 pb-10">
      <div className="flex flex-col gap-4 border-b pb-6 sm:flex-row sm:items-end sm:justify-between">
        <div>
          <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
            Distribuidora
          </p>
          <h1 className="text-2xl font-semibold tracking-tight">Planificación de rutas</h1>
          <p className="mt-1 max-w-xl text-sm text-muted-foreground">
            Asigne cada OC a un camión. Los totales se agrupan por camión antes de
            confirmar.
          </p>
        </div>
        <div className="flex flex-wrap gap-2">
          <Button asChild variant="outline" size="sm">
            <Link href="/distribuidora/orders">Volver a órdenes</Link>
          </Button>
          <Button asChild variant="outline" size="sm">
            <Link href="/distribuidora/rutero">Ver rutero</Link>
          </Button>
        </div>
      </div>

      {error ? (
        <Alert variant="destructive">
          <AlertTitle>No se pudo guardar</AlertTitle>
          <AlertDescription className="whitespace-pre-wrap text-xs">
            {error}
          </AlertDescription>
        </Alert>
      ) : null}

      {missingDocumentIds.length > 0 ? (
        <Alert>
          <AlertTitle>Algunas OC no se cargaron</AlertTitle>
          <AlertDescription className="text-xs">
            IDs en cola sin fila en vista: {missingDocumentIds.join(", ")}
          </AlertDescription>
        </Alert>
      ) : null}

      {success ? (
        <Alert>
          <AlertTitle>Planificación guardada</AlertTitle>
          <AlertDescription className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
            <span>{success}</span>
            <Button size="sm" onClick={() => router.push("/distribuidora/rutero")}>
              Ir al rutero
            </Button>
          </AlertDescription>
        </Alert>
      ) : null}

      <div className="flex flex-col gap-4 rounded-xl border bg-card p-4 shadow-sm sm:flex-row sm:items-end sm:justify-between">
        <div className="grid gap-4 sm:grid-cols-2">
          <div className="space-y-2">
            <Label htmlFor="plan-date">Día de reparto / planificación</Label>
            <Input
              id="plan-date"
              type="date"
              value={planningDate}
              onChange={(e) => setPlanningDate(e.target.value)}
              disabled={loading || saving}
            />
          </div>
        </div>
        <div className="flex flex-wrap gap-2">
          <Button
            type="button"
            variant="secondary"
            onClick={() => void loadPreview()}
            disabled={loading || saving || planningDocumentIdsArray.length === 0}
          >
            {loading ? (
              <>
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                Cargando…
              </>
            ) : (
              "Recargar cola"
            )}
          </Button>
          <Button
            type="button"
            size="lg"
            className="gap-2"
            disabled={
              saving ||
              loading ||
              preview.length === 0 ||
              planningDocumentIdsArray.length === 0
            }
            onClick={() => void onConfirm()}
          >
            {saving ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <Truck className="h-4 w-4" />
            )}
            Confirmar planificación
          </Button>
        </div>
      </div>

      {loading && preview.length === 0 ? (
        <div className="flex items-center gap-2 text-sm text-muted-foreground">
          <Loader2 className="h-4 w-4 animate-spin" />
          Cargando órdenes de la cola…
        </div>
      ) : null}

      {preview.length > 0 ? (
        <div className="flex flex-col gap-10">
          {trucksWithRows.map((truck) => {
            const rows = rowsByTruck(truck)
            const { clientCount, amount } = aggregateRows(rows)
            return (
              <section key={truck} className="space-y-3">
                <div
                  className={cn(
                    "flex flex-col gap-2 rounded-lg border bg-muted/30 px-4 py-3 sm:flex-row sm:items-center sm:justify-between",
                  )}
                >
                  <div className="flex items-center gap-2">
                    <Truck className="h-5 w-5 text-muted-foreground" />
                    <h2 className="text-lg font-semibold tracking-tight">{truck}</h2>
                    <span className="text-sm text-muted-foreground">
                      {rows.length} OC
                    </span>
                  </div>
                  <div className="text-sm text-muted-foreground">
                    <span className="font-medium text-foreground">{clientCount}</span>{" "}
                    clientes ·{" "}
                    <span className="font-medium text-foreground">{formatCLP(amount)}</span>
                  </div>
                </div>

                <div className="overflow-x-auto rounded-lg border bg-card shadow-sm">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead className="w-[100px]">OC</TableHead>
                        <TableHead>Nombre fantasía</TableHead>
                        <TableHead>Comuna</TableHead>
                        <TableHead className="text-right">Total</TableHead>
                        <TableHead>Vendedor</TableHead>
                        <TableHead className="min-w-[140px]">Camión</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {rows.map((r) => (
                        <TableRow key={r.document_id}>
                          <TableCell className="font-mono text-sm">
                            {r.oc_number ?? "—"}
                          </TableCell>
                          <TableCell className="max-w-[220px] truncate">
                            {r.client_name ?? "—"}
                          </TableCell>
                          <TableCell>{comunaLabel(r)}</TableCell>
                          <TableCell className="text-right tabular-nums">
                            {formatCLP(Number(r.total_amount ?? 0))}
                          </TableCell>
                          <TableCell className="max-w-[180px] truncate text-sm">
                            {r.seller ?? "—"}
                          </TableCell>
                          <TableCell>
                            <Select
                              value={truckByDoc[r.document_id] ?? DEFAULT_TRUCK}
                              onValueChange={(v) => setTruck(r.document_id, v)}
                              disabled={saving}
                            >
                              <SelectTrigger className="w-full max-w-[160px]">
                                <SelectValue />
                              </SelectTrigger>
                              <SelectContent>
                                {TRUCKS.map((t) => (
                                  <SelectItem key={t} value={t}>
                                    {t}
                                  </SelectItem>
                                ))}
                              </SelectContent>
                            </Select>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>
              </section>
            )
          })}
        </div>
      ) : null}

      {preview.length === 0 && !loading && planningDocumentIdsArray.length > 0 ? (
        <Alert variant="destructive">
          <AlertTitle>Sin datos</AlertTitle>
          <AlertDescription>
            No se encontraron órdenes para los document_id de la cola. Compruebe que las OC
            existan en el sistema.
          </AlertDescription>
        </Alert>
      ) : null}
    </div>
  )
}
