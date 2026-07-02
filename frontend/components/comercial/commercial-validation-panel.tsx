"use client"

import { useMemo, useState } from "react"
import {
  AlertTriangle,
  ArrowDown,
  CheckCircle2,
  ChevronRight,
  ExternalLink,
  Loader2,
  ShieldCheck,
  XCircle,
} from "lucide-react"

import type {
  CommercialValidationDocumentRow,
  CommercialValidationProductRow,
  CommercialValidationResponse,
  CommercialValidationSellerReconcile,
} from "@/lib/api"
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import {
  Drawer,
  DrawerClose,
  DrawerContent,
  DrawerHeader,
  DrawerTitle,
} from "@/components/ui/drawer"
import { Progress } from "@/components/ui/progress"
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

function formatPct(n: number): string {
  return n.toLocaleString("es-CL", { minimumFractionDigits: 2, maximumFractionDigits: 2 })
}

function formatDateCL(iso: string): string {
  const [y, m, d] = iso.split("-")
  if (!y || !m || !d) return iso
  return `${d}-${m}-${y}`
}

function formatDateTimeCL(iso: string): string {
  return new Date(iso).toLocaleString("es-CL", {
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  })
}

function StatusDot({ status }: { status: "ok" | "error" | "warning" }) {
  if (status === "ok") return <span className="text-emerald-600">✔</span>
  if (status === "warning") return <span className="text-amber-500">⚠</span>
  return <span className="text-red-600">❌</span>
}

function ReconcileBadge({ status }: { status: "ok" | "error" }) {
  return (
    <Badge
      variant={status === "ok" ? "default" : "destructive"}
      className={cn(status === "ok" && "bg-emerald-600 hover:bg-emerald-600")}
    >
      {status === "ok" ? "OK" : "Diferencia"}
    </Badge>
  )
}

function scrollToAnchor(id: string) {
  document.getElementById(id)?.scrollIntoView({ behavior: "smooth", block: "start" })
}

type SortKey = keyof Pick<
  CommercialValidationProductRow,
  "product" | "qty_erp" | "qty_bsale" | "amount_erp" | "amount_bsale" | "delta"
>

export function CommercialValidationPanel({
  data,
  loading,
  error,
}: {
  data: CommercialValidationResponse | null
  loading: boolean
  error: string | null
}) {
  const [drawerDay, setDrawerDay] = useState<string | null>(null)
  const [drawerDocs, setDrawerDocs] = useState<CommercialValidationDocumentRow[]>([])
  const [sellerDetail, setSellerDetail] = useState<CommercialValidationSellerReconcile | null>(null)
  const [productSort, setProductSort] = useState<{ key: SortKey; asc: boolean }>({
    key: "delta",
    asc: false,
  })

  const sortedProducts = useMemo(() => {
    const rows = [...(data?.product_reconciliation ?? [])]
    if (!rows.length) return []
    const { key, asc } = productSort
    rows.sort((a, b) => {
      const av = a[key]
      const bv = b[key]
      if (typeof av === "string" && typeof bv === "string") {
        return asc ? av.localeCompare(bv) : bv.localeCompare(av)
      }
      return asc ? Number(av) - Number(bv) : Number(bv) - Number(av)
    })
    return rows
  }, [data?.product_reconciliation, productSort])

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
        Etapa 1 — Selecciona un período y abre esta pestaña para cuadrar ERP con Bsale.
      </p>
    )
  }

  const audit = data.audit_status ?? {
    state: "MAJOR_DIFFERENCES" as const,
    label: "SIN DATOS",
    emoji: "🔴",
    precision_percent: 0,
    progress_percent: 0,
    progress_label: "Auditoría ERP",
    validated: false,
  }
  const dataCoverage = data.data_coverage ?? []
  const commercialReconciliation = data.commercial_reconciliation ?? []
  const sellerReconciliation = data.seller_reconciliation ?? []
  const dailyReconciliation = data.daily_reconciliation ?? []
  const clientReconciliation = data.client_reconciliation ?? []
  const autoAuditRules = data.auto_audit_rules ?? []
  const differenceItems = data.difference_items ?? []
  const documentsByDay = data.documents_by_day ?? {}
  const engineLabel = data.validation.audit_engine_version ?? "2.1"

  const openDayDrawer = (day: string) => {
    setDrawerDay(day)
    setDrawerDocs(documentsByDay[day] ?? [])
  }

  const toggleProductSort = (key: SortKey) => {
    setProductSort((prev) =>
      prev.key === key ? { key, asc: !prev.asc } : { key, asc: false },
    )
  }

  return (
    <div className="space-y-6">
      {/* 11. Barra de progreso */}
      <Card className="border-primary/30">
        <CardContent className="space-y-3 p-5">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <p className="text-sm font-medium">{audit.progress_label}</p>
            <span className="text-sm tabular-nums text-muted-foreground">
              {formatPct(audit.progress_percent)} %
            </span>
          </div>
          <Progress value={audit.progress_percent} className="h-3" />
          {audit.validated && (
            <div className="rounded-lg border border-emerald-500/40 bg-emerald-500/10 p-4 text-center">
              <p className="text-lg font-bold text-emerald-700">AUDITORÍA VALIDADA</p>
              <p className="text-sm text-emerald-800">ERP CUADRADO CON BSALE</p>
            </div>
          )}
        </CardContent>
      </Card>

      {/* 1. Panel estado de auditoría */}
      <Card
        className={cn(
          "border-2",
          audit.state === "VALIDATED" && "border-emerald-500/50 bg-emerald-500/5",
          audit.state === "MINOR_DIFFERENCES" && "border-amber-500/50 bg-amber-500/5",
          audit.state === "MAJOR_DIFFERENCES" && "border-red-500/50 bg-red-500/5",
        )}
      >
        <CardContent className="grid gap-6 p-6 lg:grid-cols-[1fr_auto]">
          <div className="space-y-4">
            <div className="flex items-center gap-3">
              <ShieldCheck className="h-8 w-8 shrink-0 text-primary" />
              <div>
                <p className="text-xs uppercase tracking-wide text-muted-foreground">Estado</p>
                <p className="text-2xl font-bold">
                  {audit.emoji} {audit.label}
                </p>
              </div>
            </div>
            <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
              <div>
                <p className="text-xs text-muted-foreground">Precisión</p>
                <p className="text-xl font-semibold tabular-nums">{formatPct(audit.precision_percent)} %</p>
              </div>
              <div>
                <p className="text-xs text-muted-foreground">Período auditado</p>
                <p className="font-medium">{formatDateCL(data.period.from)}</p>
                <p className="text-center text-muted-foreground">↓</p>
                <p className="font-medium">{formatDateCL(data.period.to)}</p>
              </div>
              <div>
                <p className="text-xs text-muted-foreground">Generado</p>
                <p className="font-medium">{formatDateTimeCL(data.validation.generated_at)}</p>
              </div>
              <div>
                <p className="text-xs text-muted-foreground">Engine</p>
                <p className="font-medium">v{engineLabel}</p>
              </div>
              <div>
                <p className="text-xs text-muted-foreground">Scope</p>
                <p className="text-sm font-medium">{data.validation.sales_scope_version}</p>
              </div>
              <div>
                <p className="text-xs text-muted-foreground">Método comparación</p>
                <p className="text-sm">{data.validation.comparison_method ?? "document_id · venta neta"}</p>
              </div>
            </div>
          </div>
          <div className="flex flex-col items-start gap-2 lg:items-end">
            <Badge variant="outline">{data.scope.company_name}</Badge>
            <Badge variant="outline">{data.scope.office_name}</Badge>
            <span className="text-xs text-muted-foreground">
              {data.validation.execution_ms.toFixed(0)} ms
            </span>
          </div>
        </CardContent>
      </Card>

      {/* 10. Panel de diferencias */}
      {differenceItems.length > 0 && (
        <Card id="panel-diferencias" className="border-amber-500/30">
          <CardHeader>
            <CardTitle className="text-base">Panel de diferencias</CardTitle>
          </CardHeader>
          <CardContent className="overflow-x-auto">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Prioridad</TableHead>
                  <TableHead>Tipo</TableHead>
                  <TableHead>Descripción</TableHead>
                  <TableHead className="text-right">Impacto</TableHead>
                  <TableHead />
                </TableRow>
              </TableHeader>
              <TableBody>
                {differenceItems.map((item, i) => (
                  <TableRow key={`${item.anchor}-${i}`}>
                    <TableCell>
                      {item.priority === "high" ? "🔴" : item.priority === "medium" ? "🟡" : "🟢"}
                    </TableCell>
                    <TableCell className="font-medium">{item.type}</TableCell>
                    <TableCell className="max-w-md text-sm">{item.description}</TableCell>
                    <TableCell className="text-right tabular-nums">
                      {item.type === "Ventas" || item.type === "Diario" || item.type === "Vendedores"
                        ? formatCLP(item.impact)
                        : formatNum(item.impact)}
                    </TableCell>
                    <TableCell>
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() => scrollToAnchor(item.anchor)}
                      >
                        Ir <ChevronRight className="ml-1 h-4 w-4" />
                      </Button>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </CardContent>
        </Card>
      )}

      {/* 2. Cobertura de datos */}
      <Card id="cobertura-datos">
        <CardHeader>
          <CardTitle className="text-base">Cobertura de datos</CardTitle>
          <p className="text-sm text-muted-foreground">ERP vs Bsale (sales_base)</p>
        </CardHeader>
        <CardContent className="overflow-x-auto">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Métrica</TableHead>
                <TableHead className="text-right">ERP</TableHead>
                <TableHead className="text-right">Bsale</TableHead>
                <TableHead className="text-center">Match</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {dataCoverage.map((row) => (
                <TableRow key={row.metric}>
                  <TableCell className="font-medium">{row.metric}</TableCell>
                  <TableCell className="text-right tabular-nums">
                    {typeof row.erp === "number" ? formatNum(row.erp) : row.erp ?? "—"}
                  </TableCell>
                  <TableCell className="text-right tabular-nums">
                    {typeof row.bsale === "number" ? formatNum(row.bsale) : row.bsale ?? "—"}
                  </TableCell>
                  <TableCell className="text-center text-lg">
                    {row.match ? <span className="text-emerald-600">✔</span> : <span className="text-red-600">❌</span>}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      {/* 3. Cuadratura comercial */}
      <Card id="cuadratura-comercial">
        <CardHeader>
          <CardTitle className="text-base">Cuadratura comercial</CardTitle>
        </CardHeader>
        <CardContent className="overflow-x-auto">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Concepto</TableHead>
                <TableHead className="text-right">ERP</TableHead>
                <TableHead className="text-right">Bsale</TableHead>
                <TableHead className="text-right">Diferencia</TableHead>
                <TableHead>Estado</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {commercialReconciliation.map((row) => (
                <TableRow
                  key={row.concept}
                  className={cn(row.status === "ok" ? "hover:bg-emerald-500/5" : "bg-red-500/5")}
                >
                  <TableCell className="font-medium">{row.concept}</TableCell>
                  <TableCell className="text-right tabular-nums">
                    {row.concept.includes("únicos") || row.concept === "Unidades"
                      ? formatNum(row.erp)
                      : formatCLP(row.erp)}
                  </TableCell>
                  <TableCell className="text-right tabular-nums">
                    {row.concept.includes("únicos") || row.concept === "Unidades"
                      ? formatNum(row.bsale)
                      : formatCLP(row.bsale)}
                  </TableCell>
                  <TableCell
                    className={cn(
                      "text-right tabular-nums font-medium",
                      row.status === "ok" ? "text-emerald-600" : "text-red-600",
                    )}
                  >
                    {row.concept.includes("únicos") || row.concept === "Unidades"
                      ? formatNum(row.delta)
                      : formatCLP(row.delta)}
                  </TableCell>
                  <TableCell>
                    <ReconcileBadge status={row.status} />
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      {/* 4. Cuadratura por vendedor */}
      <Card id="cuadratura-vendedor">
        <CardHeader>
          <CardTitle className="text-base">Cuadratura por vendedor</CardTitle>
          <p className="text-sm text-muted-foreground">Scope: 89 · 80 · 85 · 59</p>
        </CardHeader>
        <CardContent className="overflow-x-auto">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Vendedor</TableHead>
                <TableHead className="text-right">Facturas</TableHead>
                <TableHead className="text-right">Boletas</TableHead>
                <TableHead className="text-right">NC</TableHead>
                <TableHead className="text-right">Venta ERP</TableHead>
                <TableHead className="text-right">Venta Bsale</TableHead>
                <TableHead className="text-right">Δ</TableHead>
                <TableHead className="text-right">Clientes</TableHead>
                <TableHead className="text-right">Ticket</TableHead>
                <TableHead>Estado</TableHead>
                <TableHead />
              </TableRow>
            </TableHeader>
            <TableBody>
              {sellerReconciliation.map((row) => (
                <TableRow key={row.seller_id}>
                  <TableCell className="font-medium">{row.seller}</TableCell>
                  <TableCell className="text-right tabular-nums">{formatNum(row.facturas)}</TableCell>
                  <TableCell className="text-right tabular-nums">{formatNum(row.boletas)}</TableCell>
                  <TableCell className="text-right tabular-nums">{formatNum(row.notas_credito)}</TableCell>
                  <TableCell className="text-right tabular-nums">{formatCLP(row.venta_erp)}</TableCell>
                  <TableCell className="text-right tabular-nums">{formatCLP(row.venta_bsale)}</TableCell>
                  <TableCell
                    className={cn(
                      "text-right tabular-nums font-medium",
                      row.status === "ok" ? "text-emerald-600" : "text-red-600",
                    )}
                  >
                    {formatCLP(row.delta)}
                  </TableCell>
                  <TableCell className="text-right tabular-nums">{formatNum(row.clientes)}</TableCell>
                  <TableCell className="text-right tabular-nums">{formatCLP(row.ticket)}</TableCell>
                  <TableCell>
                    <ReconcileBadge status={row.status} />
                  </TableCell>
                  <TableCell>
                    <Button variant="outline" size="sm" onClick={() => setSellerDetail(row)}>
                      Ver detalle
                    </Button>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      {/* 5. Cuadratura diaria */}
      <Card id="cuadratura-diaria">
        <CardHeader>
          <CardTitle className="text-base">Cuadratura diaria</CardTitle>
          <p className="text-sm text-muted-foreground">Click en un día con diferencia para ver documentos</p>
        </CardHeader>
        <CardContent className="max-h-80 overflow-auto">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Fecha</TableHead>
                <TableHead className="text-right">ERP</TableHead>
                <TableHead className="text-right">Bsale</TableHead>
                <TableHead className="text-right">Δ</TableHead>
                <TableHead>Estado</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {dailyReconciliation.map((row) => (
                <TableRow
                  key={row.date}
                  className={cn(
                    row.status !== "ok" && "cursor-pointer hover:bg-muted/60",
                    row.status === "ok" && "text-muted-foreground",
                  )}
                  onClick={() => row.status !== "ok" && openDayDrawer(row.date)}
                >
                  <TableCell className="font-medium">{formatDateCL(row.date)}</TableCell>
                  <TableCell className="text-right tabular-nums">{formatCLP(row.erp)}</TableCell>
                  <TableCell className="text-right tabular-nums">{formatCLP(row.bsale)}</TableCell>
                  <TableCell
                    className={cn(
                      "text-right tabular-nums",
                      row.status === "ok" ? "text-emerald-600" : "text-red-600",
                    )}
                  >
                    {formatCLP(row.delta)}
                  </TableCell>
                  <TableCell>
                    <ReconcileBadge status={row.status} />
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      {/* 7. Cuadratura por cliente */}
      {clientReconciliation.length > 0 && (
        <Card id="cuadratura-cliente">
          <CardHeader>
            <CardTitle className="text-base">Cuadratura por cliente</CardTitle>
            <p className="text-sm text-muted-foreground">Solo clientes con diferencia</p>
          </CardHeader>
          <CardContent className="max-h-80 overflow-auto">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Cliente</TableHead>
                  <TableHead className="text-right">ERP</TableHead>
                  <TableHead className="text-right">Bsale</TableHead>
                  <TableHead className="text-right">Δ</TableHead>
                  <TableHead className="text-right">Documentos</TableHead>
                  <TableHead className="text-right">Ticket</TableHead>
                  <TableHead>Última compra</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {clientReconciliation.map((row) => (
                  <TableRow key={row.client_id}>
                    <TableCell className="max-w-[200px] truncate font-medium" title={row.client}>
                      {row.client}
                    </TableCell>
                    <TableCell className="text-right tabular-nums">{formatCLP(row.erp)}</TableCell>
                    <TableCell className="text-right tabular-nums">{formatCLP(row.bsale)}</TableCell>
                    <TableCell className="text-right tabular-nums text-red-600">{formatCLP(row.delta)}</TableCell>
                    <TableCell className="text-right tabular-nums">{formatNum(row.documentos)}</TableCell>
                    <TableCell className="text-right tabular-nums">{formatCLP(row.ticket)}</TableCell>
                    <TableCell>{row.ultima_compra ? formatDateCL(String(row.ultima_compra)) : "—"}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </CardContent>
        </Card>
      )}

      {/* 8. Cuadratura por producto */}
      {sortedProducts.length > 0 && (
        <Card id="cuadratura-producto">
          <CardHeader>
            <CardTitle className="text-base">Cuadratura por producto</CardTitle>
            <p className="text-sm text-muted-foreground">Solo productos con diferencia · ordenable</p>
          </CardHeader>
          <CardContent className="max-h-96 overflow-auto">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>
                    <button type="button" className="hover:underline" onClick={() => toggleProductSort("product")}>
                      Producto
                    </button>
                  </TableHead>
                  <TableHead className="text-right">
                    <button type="button" className="hover:underline" onClick={() => toggleProductSort("qty_erp")}>
                      Cant. ERP
                    </button>
                  </TableHead>
                  <TableHead className="text-right">
                    <button type="button" className="hover:underline" onClick={() => toggleProductSort("qty_bsale")}>
                      Cant. Bsale
                    </button>
                  </TableHead>
                  <TableHead className="text-right">
                    <button type="button" className="hover:underline" onClick={() => toggleProductSort("amount_erp")}>
                      Monto ERP
                    </button>
                  </TableHead>
                  <TableHead className="text-right">
                    <button type="button" className="hover:underline" onClick={() => toggleProductSort("amount_bsale")}>
                      Monto Bsale
                    </button>
                  </TableHead>
                  <TableHead className="text-right">
                    <button type="button" className="hover:underline" onClick={() => toggleProductSort("delta")}>
                      Δ
                    </button>
                  </TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {sortedProducts.map((row) => (
                  <TableRow key={row.variant_id}>
                    <TableCell className="max-w-[220px] truncate font-medium" title={row.product}>
                      {row.product}
                    </TableCell>
                    <TableCell className="text-right tabular-nums">{formatNum(row.qty_erp)}</TableCell>
                    <TableCell className="text-right tabular-nums">{formatNum(row.qty_bsale)}</TableCell>
                    <TableCell className="text-right tabular-nums">{formatCLP(row.amount_erp)}</TableCell>
                    <TableCell className="text-right tabular-nums">{formatCLP(row.amount_bsale)}</TableCell>
                    <TableCell className="text-right tabular-nums text-red-600">{formatCLP(row.delta)}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </CardContent>
        </Card>
      )}

      {/* 9. Auditoría automática */}
      <Card id="revision-automatica">
        <CardHeader>
          <CardTitle className="text-base">Revisión automática</CardTitle>
        </CardHeader>
        <CardContent>
          <Accordion type="multiple" className="w-full">
            {autoAuditRules.map((rule) => (
              <AccordionItem key={rule.rule_id} value={rule.rule_id}>
                <AccordionTrigger className="hover:no-underline">
                  <div className="flex flex-1 items-center gap-3 pr-4 text-left">
                    <StatusDot
                      status={rule.severity === "ok" ? "ok" : rule.severity === "warning" ? "warning" : "error"}
                    />
                    <span className="font-medium">{rule.label}</span>
                    <Badge
                      variant={
                        rule.severity === "ok"
                          ? "default"
                          : rule.severity === "warning"
                            ? "secondary"
                            : "destructive"
                      }
                      className={cn(rule.severity === "ok" && "bg-emerald-600")}
                    >
                      {rule.severity.toUpperCase()}
                    </Badge>
                    {rule.count > 0 && (
                      <span className="text-xs text-muted-foreground">({formatNum(rule.count)})</span>
                    )}
                  </div>
                </AccordionTrigger>
                <AccordionContent className="flex items-start gap-2 text-sm text-muted-foreground">
                  {rule.severity === "ok" ? (
                    <CheckCircle2 className="mt-0.5 h-4 w-4 shrink-0 text-emerald-600" />
                  ) : rule.severity === "warning" ? (
                    <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-amber-600" />
                  ) : (
                    <XCircle className="mt-0.5 h-4 w-4 shrink-0 text-red-600" />
                  )}
                  {rule.message}
                </AccordionContent>
              </AccordionItem>
            ))}
          </Accordion>
        </CardContent>
      </Card>

      {/* 6. Drawer detalle documentos */}
      <Drawer open={drawerDay !== null} onOpenChange={(open) => !open && setDrawerDay(null)}>
        <DrawerContent className="max-h-[85vh]">
          <DrawerHeader>
            <DrawerTitle>
              Documentos — {drawerDay ? formatDateCL(drawerDay) : ""}
            </DrawerTitle>
          </DrawerHeader>
          <div className="overflow-auto px-4 pb-6">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Número</TableHead>
                  <TableHead>Tipo</TableHead>
                  <TableHead>Cliente</TableHead>
                  <TableHead>Vendedor</TableHead>
                  <TableHead className="text-right">ERP</TableHead>
                  <TableHead className="text-right">Bsale</TableHead>
                  <TableHead className="text-right">Δ</TableHead>
                  <TableHead>Estado</TableHead>
                  <TableHead />
                </TableRow>
              </TableHeader>
              <TableBody>
                {drawerDocs.map((doc) => (
                  <TableRow key={doc.document_id}>
                    <TableCell className="tabular-nums">{doc.number ?? doc.document_id}</TableCell>
                    <TableCell>{doc.document_type}</TableCell>
                    <TableCell className="max-w-[140px] truncate" title={doc.client}>
                      {doc.client}
                    </TableCell>
                    <TableCell className="max-w-[120px] truncate">{doc.seller}</TableCell>
                    <TableCell className="text-right tabular-nums">{formatCLP(doc.erp)}</TableCell>
                    <TableCell className="text-right tabular-nums">{formatCLP(doc.bsale)}</TableCell>
                    <TableCell
                      className={cn(
                        "text-right tabular-nums",
                        doc.status === "ok" ? "text-emerald-600" : "text-red-600",
                      )}
                    >
                      {formatCLP(doc.delta)}
                    </TableCell>
                    <TableCell>
                      <ReconcileBadge status={doc.status} />
                    </TableCell>
                    <TableCell>
                      {doc.url ? (
                        <Button variant="ghost" size="sm" asChild>
                          <a href={doc.url} target="_blank" rel="noopener noreferrer">
                            Abrir <ExternalLink className="ml-1 h-3.5 w-3.5" />
                          </a>
                        </Button>
                      ) : (
                        <span className="text-xs text-muted-foreground">—</span>
                      )}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
            <div className="mt-4 flex justify-end">
              <DrawerClose asChild>
                <Button variant="outline">Cerrar</Button>
              </DrawerClose>
            </div>
          </div>
        </DrawerContent>
      </Drawer>

      {/* Drawer detalle vendedor */}
      <Drawer open={sellerDetail !== null} onOpenChange={(open) => !open && setSellerDetail(null)}>
        <DrawerContent>
          <DrawerHeader>
            <DrawerTitle>{sellerDetail?.seller}</DrawerTitle>
          </DrawerHeader>
          {sellerDetail && (
            <div className="grid gap-4 px-4 pb-6 sm:grid-cols-2">
              <Card>
                <CardContent className="p-4">
                  <p className="text-xs text-muted-foreground">Venta ERP</p>
                  <p className="text-xl font-bold">{formatCLP(sellerDetail.venta_erp)}</p>
                </CardContent>
              </Card>
              <Card>
                <CardContent className="p-4">
                  <p className="text-xs text-muted-foreground">Venta Bsale</p>
                  <p className="text-xl font-bold">{formatCLP(sellerDetail.venta_bsale)}</p>
                </CardContent>
              </Card>
              <Card>
                <CardContent className="p-4">
                  <p className="text-xs text-muted-foreground">Documentos</p>
                  <p className="text-lg">
                    F {formatNum(sellerDetail.facturas)} · B {formatNum(sellerDetail.boletas)} · NC{" "}
                    {formatNum(sellerDetail.notas_credito)}
                  </p>
                </CardContent>
              </Card>
              <Card>
                <CardContent className="p-4">
                  <p className="text-xs text-muted-foreground">Clientes / Ticket</p>
                  <p className="text-lg">
                    {formatNum(sellerDetail.clientes)} · {formatCLP(sellerDetail.ticket)}
                  </p>
                </CardContent>
              </Card>
              <div className="sm:col-span-2 flex items-center justify-between rounded-lg border p-4">
                <span className="text-sm">Diferencia cuadratura</span>
                <span
                  className={cn(
                    "text-lg font-bold tabular-nums",
                    sellerDetail.status === "ok" ? "text-emerald-600" : "text-red-600",
                  )}
                >
                  {formatCLP(sellerDetail.delta)}
                </span>
              </div>
            </div>
          )}
        </DrawerContent>
      </Drawer>

      <p className="flex items-center justify-center gap-1 text-center text-xs text-muted-foreground">
        <ArrowDown className="h-3 w-3" />
        Venta neta = Facturas + Boletas − Notas de crédito · mismo sales_base del bundle comercial
      </p>
    </div>
  )
}
