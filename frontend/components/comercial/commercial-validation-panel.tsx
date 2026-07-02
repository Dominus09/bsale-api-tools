"use client"

import { Calendar, Clock, FileText, Loader2, Package, ShieldCheck, Users, Wallet, Zap } from "lucide-react"

import type { CommercialValidationResponse } from "@/lib/api"
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
        Selecciona un período y abre esta pestaña para auditar el motor.
      </p>
    )
  }

  const formulaOk = Math.abs(data.ventas_netas.ventas_netas - data.ventas_netas.formula_check) < 1

  return (
    <div className="space-y-6">
      <Card className="border-primary/20 bg-primary/5">
        <CardContent className="flex flex-wrap items-center justify-between gap-4 p-5">
          <div className="flex items-center gap-3">
            <ShieldCheck className="h-6 w-6 text-primary" />
            <div>
              <p className="font-semibold">Validación pre-deploy</p>
              <p className="text-sm text-muted-foreground">
                {data.scope.company_name} · {data.scope.office_name} · {data.period.from} → {data.period.to}
              </p>
            </div>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <Badge variant={data.validation.status === "OK" ? "default" : "destructive"}>
              {data.validation.status}
            </Badge>
            <Badge variant="outline">{data.validation.engine_version}</Badge>
            <Badge variant="outline">{data.validation.sales_scope_version}</Badge>
            <span className="text-xs text-muted-foreground">
              {new Date(data.validation.generated_at).toLocaleString("es-CL")}
            </span>
          </div>
        </CardContent>
      </Card>

      <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
        <MetricCard
          icon={<Calendar className="h-5 w-5" />}
          label="Primer documento"
          value={data.temporal_coverage.first_document_date ?? "—"}
          sub={`${formatNum(data.temporal_coverage.days_covered)} días cubiertos`}
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
          sub={`Activos ${formatNum(data.clients.active_clients)} · Inactivos ${formatNum(data.clients.inactive_clients)}`}
        />
        <MetricCard
          icon={<Package className="h-5 w-5" />}
          label="Productos únicos"
          value={formatNum(data.products.unique_products)}
          sub={`${formatNum(data.products.total_lines)} líneas de detalle`}
        />
        <MetricCard
          icon={<Wallet className="h-5 w-5" />}
          label="Ventas netas"
          value={formatCLP(data.ventas_netas.ventas_netas)}
          sub={
            formulaOk
              ? `F ${formatCLP(data.ventas_netas.facturas)} + B ${formatCLP(data.ventas_netas.boletas)} − NC ${formatCLP(data.ventas_netas.notas_credito)}`
              : "Revisar fórmula F + B − NC"
          }
        />
        <MetricCard
          icon={<Zap className="h-5 w-5" />}
          label="Tiempo de generación"
          value={`${data.validation.execution_ms.toFixed(0)} ms`}
          sub="Mismo sales_base del bundle"
          className="sm:col-span-2 xl:col-span-1"
        />
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="text-base">Alcance analizado</CardTitle>
        </CardHeader>
        <CardContent className="flex flex-wrap gap-2">
          {data.scope.active_sellers.map((s) => (
            <Badge key={s.id} variant="secondary">
              {s.name} <span className="ml-1 text-muted-foreground">#{s.id}</span>
            </Badge>
          ))}
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="flex flex-row items-center justify-between">
          <CardTitle className="text-base">Distribución por vendedor</CardTitle>
          <span className="flex items-center gap-1 text-xs text-muted-foreground">
            <Clock className="h-3.5 w-3.5" />
            Comparar con Bsale
          </span>
        </CardHeader>
        <CardContent className="overflow-x-auto">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Vendedor</TableHead>
                <TableHead className="text-right">Documentos</TableHead>
                <TableHead className="text-right">Clientes</TableHead>
                <TableHead className="text-right">Ventas netas</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {data.seller_distribution.map((row) => (
                <TableRow key={row.seller_id ?? row.seller}>
                  <TableCell className="font-medium">{row.seller}</TableCell>
                  <TableCell className="text-right tabular-nums">{formatNum(row.documents)}</TableCell>
                  <TableCell className="text-right tabular-nums">{formatNum(row.clients)}</TableCell>
                  <TableCell className="text-right tabular-nums">{formatCLP(row.ventas_netas)}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      <p className="text-center text-xs text-muted-foreground">
        Usa estos totales para cotejar con Bsale en el mismo período ({data.period.from} al {data.period.to}).
        Los documentos son únicos (sin contar líneas).
      </p>
    </div>
  )
}
