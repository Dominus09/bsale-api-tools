"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import {
  AlertTriangle,
  CheckCircle2,
  Loader2,
  PackageCheck,
  RefreshCw,
  Scale,
} from "lucide-react"

import {
  getWeightAudit,
  getWeightAuditOrderDetail,
  type WeightAuditOrderDetail,
  type WeightAuditResponse,
} from "@/lib/api"
import { Alert, AlertDescription } from "@/components/ui/alert"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { cn } from "@/lib/utils"

function formatCount(n: number): string {
  return n.toLocaleString("es-CL")
}

function formatKg(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(Number(value))) return "—"
  return `${Number(value).toLocaleString("es-CL", {
    minimumFractionDigits: 1,
    maximumFractionDigits: 1,
  })} kg`
}

function formatPct(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(Number(value))) return "—"
  return `${Number(value).toFixed(1)}%`
}

const JOIN_STATUS_LABEL: Record<string, string> = {
  ok_variant_join: "Join variant_id OK",
  ok_barcode_join: "Peso vía barcode (variant_id no coincide)",
  sin_variant_id: "Línea sin variant_id",
  sin_match_pm: "Sin fila en products_master",
  pm_sin_peso: "PM sin peso calculable",
  sin_peso: "Sin peso",
  sin_cantidad: "Cantidad cero",
}

function OrderEstadoBadge({ estado }: { estado: string }) {
  if (estado === "completo") {
    return (
      <Badge className="bg-emerald-600 hover:bg-emerald-600">
        <CheckCircle2 className="mr-1 size-3" />
        Completo
      </Badge>
    )
  }
  if (estado === "parcial") {
    return (
      <Badge variant="secondary" className="bg-amber-100 text-amber-900">
        <AlertTriangle className="mr-1 size-3" />
        Parcial
      </Badge>
    )
  }
  return (
    <Badge variant="destructive">
      <AlertTriangle className="mr-1 size-3" />
      Sin peso
    </Badge>
  )
}

function KpiCard({
  title,
  value,
  subtitle,
  className,
}: {
  title: string
  value: string
  subtitle?: string
  className?: string
}) {
  return (
    <Card className={className}>
      <CardHeader className="pb-2">
        <CardTitle className="text-sm font-medium text-muted-foreground">{title}</CardTitle>
      </CardHeader>
      <CardContent>
        <p className="text-2xl font-semibold tabular-nums">{value}</p>
        {subtitle ? (
          <p className="mt-1 text-xs text-muted-foreground">{subtitle}</p>
        ) : null}
      </CardContent>
    </Card>
  )
}

export default function AuditoriaPesosPage() {
  const [data, setData] = useState<WeightAuditResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [detailOpen, setDetailOpen] = useState(false)
  const [detailLoading, setDetailLoading] = useState(false)
  const [detail, setDetail] = useState<WeightAuditOrderDetail | null>(null)
  const [detailError, setDetailError] = useState<string | null>(null)

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const res = await getWeightAudit()
      setData(res)
    } catch (e) {
      setError(e instanceof Error ? e.message : "Error al cargar auditoría")
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    void load()
  }, [load])

  const openDetail = useCallback(async (documentId: number) => {
    setDetailOpen(true)
    setDetailLoading(true)
    setDetailError(null)
    setDetail(null)
    try {
      const res = await getWeightAuditOrderDetail(documentId)
      setDetail(res)
    } catch (e) {
      setDetailError(e instanceof Error ? e.message : "Error al cargar detalle")
    } finally {
      setDetailLoading(false)
    }
  }, [])

  const master = data?.master
  const summary = data?.orders_summary
  const orders = data?.orders ?? []

  const sortedOrders = useMemo(
    () =>
      [...orders].sort((a, b) => {
        const estadoOrder = { sin_peso: 0, parcial: 1, completo: 2 }
        const ea = estadoOrder[a.estado] ?? 9
        const eb = estadoOrder[b.estado] ?? 9
        if (ea !== eb) return ea - eb
        return (b.productos_sin_peso ?? 0) - (a.productos_sin_peso ?? 0)
      }),
    [orders],
  )

  return (
    <div className="flex flex-col gap-6 p-6">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
            Analítica → Logística
          </p>
          <h1 className="flex items-center gap-2 text-2xl font-semibold">
            <Scale className="size-7 text-primary" />
            Auditoría de pesos
          </h1>
          <p className="mt-1 max-w-2xl text-sm text-muted-foreground">
            Diagnóstico de cobertura de peso en maestro y órdenes de compra abiertas. Use el
            detalle para ver dónde falla el join variant_id → products_master.
          </p>
        </div>
        <Button variant="outline" size="sm" onClick={() => void load()} disabled={loading}>
          {loading ? (
            <Loader2 className="mr-2 size-4 animate-spin" />
          ) : (
            <RefreshCw className="mr-2 size-4" />
          )}
          Actualizar
        </Button>
      </div>

      {error ? (
        <Alert variant="destructive">
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      ) : null}

      {loading && !data ? (
        <div className="flex items-center justify-center py-16 text-muted-foreground">
          <Loader2 className="mr-2 size-5 animate-spin" />
          Cargando auditoría…
        </div>
      ) : null}

      {master ? (
        <section>
          <h2 className="mb-3 text-sm font-semibold uppercase tracking-wide text-muted-foreground">
            Maestro de productos
          </h2>
          <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
            <KpiCard title="Productos con peso" value={formatCount(master.productos_con_peso)} />
            <KpiCard title="Productos sin peso" value={formatCount(master.productos_sin_peso)} />
            <KpiCard
              title="Cobertura variantes"
              value={formatPct(master.porcentaje_cobertura)}
              subtitle={`${formatCount(master.variantes_con_peso)} / ${formatCount(master.variantes_total)} variantes`}
            />
            <KpiCard
              title="Productos ERP"
              value={formatCount(master.productos_erp)}
              subtitle={`${formatCount(master.variantes_con_peso)} var. con peso · ${formatCount(master.variantes_sin_peso)} sin`}
            />
          </div>
        </section>
      ) : null}

      {summary ? (
        <section>
          <h2 className="mb-3 text-sm font-semibold uppercase tracking-wide text-muted-foreground">
            Órdenes abiertas
          </h2>
          <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
            <KpiCard
              title="Órdenes con peso"
              value={formatCount(summary.ordenes_completo)}
              subtitle={`de ${formatCount(summary.ordenes_total)} abiertas`}
            />
            <KpiCard title="Órdenes parciales" value={formatCount(summary.ordenes_parcial)} />
            <KpiCard title="Órdenes sin peso" value={formatCount(summary.ordenes_sin_peso)} />
            <KpiCard
              title="Total auditadas"
              value={formatCount(summary.ordenes_total)}
              subtitle="OC tipo 33 sin facturar"
            />
          </div>
        </section>
      ) : null}

      {data ? (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2 text-base">
              <PackageCheck className="size-5" />
              Pedidos abiertos
            </CardTitle>
          </CardHeader>
          <CardContent className="overflow-x-auto">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>OC</TableHead>
                  <TableHead>Cliente</TableHead>
                  <TableHead className="text-right">Peso calculado</TableHead>
                  <TableHead className="text-right">Cobertura %</TableHead>
                  <TableHead className="text-right">Sin peso</TableHead>
                  <TableHead>Estado</TableHead>
                  <TableHead />
                </TableRow>
              </TableHeader>
              <TableBody>
                {sortedOrders.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={7} className="text-center text-muted-foreground">
                      No hay órdenes abiertas para auditar.
                    </TableCell>
                  </TableRow>
                ) : (
                  sortedOrders.map((row) => (
                    <TableRow key={row.document_id}>
                      <TableCell className="font-mono font-medium">{row.oc}</TableCell>
                      <TableCell className="max-w-[200px] truncate">
                        {row.cliente || "—"}
                      </TableCell>
                      <TableCell className="text-right tabular-nums">
                        {formatKg(row.peso_total_kg)}
                      </TableCell>
                      <TableCell className="text-right tabular-nums">
                        {formatPct(row.porcentaje_cobertura)}
                      </TableCell>
                      <TableCell className="text-right tabular-nums">
                        {row.productos_sin_peso}
                      </TableCell>
                      <TableCell>
                        <OrderEstadoBadge estado={row.estado} />
                      </TableCell>
                      <TableCell className="text-right">
                        <Button
                          variant="outline"
                          size="sm"
                          onClick={() => void openDetail(row.document_id)}
                        >
                          Ver detalle
                        </Button>
                      </TableCell>
                    </TableRow>
                  ))
                )}
              </TableBody>
            </Table>
          </CardContent>
        </Card>
      ) : null}

      <Dialog open={detailOpen} onOpenChange={setDetailOpen}>
        <DialogContent className="max-h-[90vh] max-w-4xl overflow-y-auto">
          <DialogHeader>
            <DialogTitle>
              {detail ? `OC ${detail.oc} — detalle de peso` : "Detalle de peso"}
            </DialogTitle>
          </DialogHeader>

          {detailLoading ? (
            <div className="flex items-center justify-center py-10 text-muted-foreground">
              <Loader2 className="mr-2 size-5 animate-spin" />
              Cargando líneas…
            </div>
          ) : null}

          {detailError ? (
            <Alert variant="destructive">
              <AlertDescription>{detailError}</AlertDescription>
            </Alert>
          ) : null}

          {detail ? (
            <div className="space-y-4">
              <div className="flex flex-wrap items-center gap-3 text-sm">
                <span className="text-muted-foreground">Cliente:</span>
                <span className="font-medium">{detail.cliente || "—"}</span>
                <OrderEstadoBadge estado={detail.estado} />
                <span className="text-muted-foreground">
                  Peso total: <strong>{formatKg(detail.peso_total_kg)}</strong>
                </span>
                <span className="text-muted-foreground">
                  Cobertura: <strong>{formatPct(detail.porcentaje_cobertura)}</strong>
                </span>
              </div>

              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Código</TableHead>
                    <TableHead>Producto</TableHead>
                    <TableHead className="text-right">Cant.</TableHead>
                    <TableHead className="text-right">Peso unit.</TableHead>
                    <TableHead className="text-right">Peso total</TableHead>
                    <TableHead>Estado</TableHead>
                    <TableHead>Join debug</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {detail.lines.map((line) => {
                    const jd = line.join_debug
                    const ok = line.estado === "tiene_peso"
                    return (
                      <TableRow
                        key={line.detail_id}
                        className={cn(!ok && "bg-destructive/5")}
                      >
                        <TableCell className="font-mono text-xs">
                          {line.codigo || "—"}
                        </TableCell>
                        <TableCell className="max-w-[220px] text-sm">
                          {line.producto || "—"}
                        </TableCell>
                        <TableCell className="text-right tabular-nums">
                          {line.cantidad}
                        </TableCell>
                        <TableCell className="text-right tabular-nums">
                          {formatKg(line.peso_unitario_kg)}
                        </TableCell>
                        <TableCell className="text-right tabular-nums">
                          {formatKg(line.peso_total_kg)}
                        </TableCell>
                        <TableCell>
                          {ok ? (
                            <span className="inline-flex items-center text-emerald-700">
                              <CheckCircle2 className="mr-1 size-4" />
                              Tiene peso
                            </span>
                          ) : (
                            <span className="inline-flex items-center text-amber-700">
                              <AlertTriangle className="mr-1 size-4" />
                              Sin peso
                            </span>
                          )}
                        </TableCell>
                        <TableCell className="text-xs text-muted-foreground">
                          <div>{JOIN_STATUS_LABEL[jd.join_status] ?? jd.join_status}</div>
                          <div className="font-mono">
                            v={jd.variant_id ?? "—"} · bc={jd.barcode ?? "—"}
                          </div>
                        </TableCell>
                      </TableRow>
                    )
                  })}
                </TableBody>
              </Table>
            </div>
          ) : null}
        </DialogContent>
      </Dialog>
    </div>
  )
}
