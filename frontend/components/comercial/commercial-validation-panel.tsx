"use client"

import {
  AlertTriangle,
  Calendar,
  CheckCircle2,
  Clock,
  FileText,
  Loader2,
  Package,
  Scale,
  ShieldCheck,
  TrendingDown,
  TrendingUp,
  Users,
  Wallet,
  Zap,
} from "lucide-react"

import type { CommercialValidationResponse } from "@/lib/api"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Badge } from "@/components/ui/badge"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { cn } from "@/lib/utils"

function formatCLP(n: number): string {
  return n.toLocaleString("es-CL", { style: "currency", currency: "CLP", maximumFractionDigits: 0 })
}

function formatNum(n: number): string {
  return n.toLocaleString("es-CL")
}

const METHOD_LABELS: Record<string, string> = {
  calendar_month: "Mismo rango mes anterior",
  rolling: "Período rolling equivalente",
  manual: "Comparación manual",
}

function MetricCard({
  icon,
  label,
  value,
  sub,
  className,
}: {
  icon: React.ReactNode
  label: string
  value: string
  sub?: string
  className?: string
}) {
  return (
    <Card className={cn("overflow-hidden border-0 bg-gradient-to-br from-card to-muted/40 shadow-sm", className)}>
      <CardContent className="p-6">
        <div className="mb-3 flex items-center gap-2 text-muted-foreground">
          {icon}
          <span className="text-sm font-medium">{label}</span>
        </div>
        <p className="text-3xl font-bold tracking-tight">{value}</p>
        {sub && <p className="mt-1 text-xs text-muted-foreground">{sub}</p>}
      </CardContent>
    </Card>
  )
}

function DeltaCell({ delta, format = "currency" }: { delta: { delta_abs: number; delta_pct: number }; format?: "currency" | "number" }) {
  const up = delta.delta_abs > 0
  const flat = Math.abs(delta.delta_abs) < 0.01
  const Icon = flat ? Scale : up ? TrendingUp : TrendingDown
  const color = flat ? "text-muted-foreground" : up ? "text-emerald-600" : "text-red-600"
  const val = format === "currency" ? formatCLP(delta.delta_abs) : formatNum(delta.delta_abs)
  return (
    <span className={cn("inline-flex items-center gap-1 text-xs tabular-nums", color)}>
      <Icon className="h-3.5 w-3.5" />
      {up && !flat ? "+" : ""}
      {val} ({delta.delta_pct > 0 ? "+" : ""}
      {delta.delta_pct.toFixed(1)}%)
    </span>
  )
}

function statusBadgeVariant(status: string): "default" | "destructive" | "secondary" | "outline" {
  if (status === "OK") return "default"
  if (status === "WARNING") return "secondary"
  return "destructive"
}

export function CommercialValidationPanel({
  data,
  loading,
  error,
}: {
  data: CommercialValidationResponse | null
  loading: boolean
  error: string | null
}) {
  if (loading && !data) {
    return (
      <div className="flex items-center justify-center py-24">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    )
  }

  if (error) {
    return (
      <Card className="border-destructive/40 bg-destructive/5">
        <CardContent className="p-6 text-sm text-destructive">{error}</CardContent>
      </Card>
    )
  }

  if (!data) {
    return (
      <p className="py-12 text-center text-muted-foreground">
        Etapa 1 — Selecciona un período y abre esta pestaña para cuadrar contra Bsale.
      </p>
    )
  }

  const formulaOk = Math.abs(data.ventas_netas.ventas_netas - data.ventas_netas.formula_check) < 1
  const issues = data.audit_checks.filter((c) => c.severity !== "ok")

  return (
    <div className="space-y-6">
      <Card className="border-primary/20 bg-primary/5">
        <CardContent className="flex flex-wrap items-center justify-between gap-4 p-5">
          <div className="flex items-center gap-3">
            <ShieldCheck className="h-6 w-6 text-primary" />
            <div>
              <p className="font-semibold">Etapa 1 — Cuadratura ERP vs Bsale</p>
              <p className="text-sm text-muted-foreground">
                {data.scope.company_name} · Período {data.period.from} → {data.period.to}
              </p>
              <p className="text-xs text-muted-foreground">
                Comparación: {data.compare_period.previous.from} → {data.compare_period.previous.to} (
                {data.compare_period.previous.days} días ·{" "}
                {METHOD_LABELS[data.compare_period.method] ?? data.compare_period.method})
              </p>
            </div>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <Badge variant={statusBadgeVariant(data.validation.status)}>{data.validation.status}</Badge>
            <Badge variant="outline">{data.validation.engine_version}</Badge>
            <span className="text-xs text-muted-foreground">
              {new Date(data.validation.generated_at).toLocaleString("es-CL")}
            </span>
          </div>
        </CardContent>
      </Card>

      {issues.length > 0 && (
        <div className="space-y-2">
          {issues.map((check, i) => (
            <Alert
              key={`${check.metric}-${i}`}
              variant={check.severity === "error" ? "destructive" : "default"}
              className={check.severity === "warning" ? "border-amber-500/50 bg-amber-500/5" : undefined}
            >
              {check.severity === "error" ? (
                <AlertTriangle className="h-4 w-4" />
              ) : check.severity === "warning" ? (
                <AlertTriangle className="h-4 w-4 text-amber-600" />
              ) : (
                <CheckCircle2 className="h-4 w-4" />
              )}
              <AlertTitle className="text-sm">{check.message}</AlertTitle>
              {(check.possible_cause || check.seller) && (
                <AlertDescription className="text-xs">
                  {check.possible_cause}
                  {check.seller ? ` · Vendedor: ${check.seller}` : ""}
                  {check.document_type ? ` · Doc: ${check.document_type}` : ""}
                  {check.delta_abs != null && check.delta_pct != null
                    ? ` · Δ ${formatCLP(check.delta_abs)} (${check.delta_pct.toFixed(1)}%)`
                    : null}
                </AlertDescription>
              )}
            </Alert>
          ))}
        </div>
      )}

      <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
        <MetricCard
          icon={<Calendar className="h-5 w-5" />}
          label="Primer documento"
          value={data.temporal_coverage.first_document_date ?? "—"}
          sub={`${formatNum(data.temporal_coverage.days_covered)} días en período`}
        />
        <MetricCard
          icon={<Calendar className="h-5 w-5" />}
          label="Último documento"
          value={data.temporal_coverage.last_document_date ?? "—"}
        />
        <MetricCard
          icon={<FileText className="h-5 w-5" />}
          label="Documentos únicos"
          value={formatNum(data.documents.total)}
          sub={`F ${formatNum(data.documents.facturas)} · B ${formatNum(data.documents.boletas)} · NC ${formatNum(data.documents.notas_credito)}`}
        />
        <MetricCard
          icon={<Users className="h-5 w-5" />}
          label="Clientes únicos"
          value={formatNum(data.clients.unique_clients)}
        />
        <MetricCard
          icon={<Package className="h-5 w-5" />}
          label="Productos únicos"
          value={formatNum(data.products.unique_products)}
          sub={`${formatNum(data.products.total_lines)} líneas`}
        />
        <MetricCard
          icon={<Package className="h-5 w-5" />}
          label="Unidades netas"
          value={formatNum(data.products.unidades_netas)}
        />
        <MetricCard
          icon={<Wallet className="h-5 w-5" />}
          label="Ventas netas"
          value={formatCLP(data.ventas_netas.ventas_netas)}
          sub={
            formulaOk
              ? `F + B − NC validado`
              : "Fórmula F + B − NC no cuadra"
          }
        />
        <MetricCard
          icon={<Scale className="h-5 w-5" />}
          label="Ticket promedio"
          value={formatCLP(data.ventas_netas.ticket_promedio)}
        />
        <MetricCard
          icon={<Zap className="h-5 w-5" />}
          label="Tiempo de generación"
          value={`${data.validation.execution_ms.toFixed(0)} ms`}
          sub="sales_base del bundle"
          className="sm:col-span-2 xl:col-span-2"
        />
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="text-base">Actual vs anterior (auditable)</CardTitle>
        </CardHeader>
        <CardContent className="overflow-x-auto">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Métrica</TableHead>
                <TableHead className="text-right">Actual</TableHead>
                <TableHead className="text-right">Anterior</TableHead>
                <TableHead className="text-right">Variación</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {(
                [
                  ["Venta neta", data.comparison.deltas.venta_neta, "currency"],
                  ["Clientes únicos", data.comparison.deltas.clientes_unicos, "number"],
                  ["Documentos únicos", data.comparison.deltas.documentos_total, "number"],
                  ["Unidades netas", data.comparison.deltas.unidades_netas, "number"],
                  ["Ticket promedio", data.comparison.deltas.ticket_promedio, "currency"],
                ] as const
              ).map(([label, delta, fmt]) => (
                <TableRow key={label}>
                  <TableCell className="font-medium">{label}</TableCell>
                  <TableCell className="text-right tabular-nums">
                    {fmt === "currency" ? formatCLP(delta.current) : formatNum(delta.current)}
                  </TableCell>
                  <TableCell className="text-right tabular-nums text-muted-foreground">
                    {fmt === "currency" ? formatCLP(delta.previous) : formatNum(delta.previous)}
                  </TableCell>
                  <TableCell className="text-right">
                    <DeltaCell delta={delta} format={fmt} />
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="flex flex-row items-center justify-between">
          <CardTitle className="text-base">Por vendedor — comparar con Bsale</CardTitle>
          <span className="flex items-center gap-1 text-xs text-muted-foreground">
            <Clock className="h-3.5 w-3.5" />
            4 vendedores operativos
          </span>
        </CardHeader>
        <CardContent className="overflow-x-auto">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Vendedor</TableHead>
                <TableHead className="text-right">Docs</TableHead>
                <TableHead className="text-right">Clientes</TableHead>
                <TableHead className="text-right">NC</TableHead>
                <TableHead className="text-right">Ticket</TableHead>
                <TableHead className="text-right">Venta neta</TableHead>
                <TableHead className="text-right">vs anterior</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {data.seller_distribution.map((row) => (
                <TableRow key={row.seller_id} className={row.out_of_scope ? "bg-amber-500/5" : undefined}>
                  <TableCell className="font-medium">
                    {row.seller}
                    {row.out_of_scope && (
                      <Badge variant="outline" className="ml-2 text-[10px]">
                        fuera scope
                      </Badge>
                    )}
                  </TableCell>
                  <TableCell className="text-right tabular-nums">{formatNum(row.documents)}</TableCell>
                  <TableCell className="text-right tabular-nums">{formatNum(row.clients)}</TableCell>
                  <TableCell className="text-right tabular-nums">{formatNum(row.notas_credito)}</TableCell>
                  <TableCell className="text-right tabular-nums">{formatCLP(row.ticket_promedio)}</TableCell>
                  <TableCell className="text-right tabular-nums">{formatCLP(row.ventas_netas.current)}</TableCell>
                  <TableCell className="text-right">
                    <DeltaCell delta={row.ventas_netas} />
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      <p className="text-center text-xs text-muted-foreground">
        Coteja en Bsale el período {data.period.from} al {data.period.to}. Documentos = COUNT(DISTINCT
        document_id). Venta neta = Facturas + Boletas − Notas de crédito.
      </p>
    </div>
  )
}
