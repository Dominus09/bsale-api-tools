"use client"

import { Fragment, useCallback, useEffect, useMemo, useState } from "react"
import { ChevronLeft, ChevronRight, Download, FileText, Loader2, RefreshCw } from "lucide-react"

import {
  downloadDispatchPlanPickingClienteExcel,
  downloadDispatchPlanPickingProductoExcel,
  type DispatchPlanPickingClientResponse,
  type DispatchPlanPickingHeader,
  type DispatchPlanPickingProductResponse,
} from "@/lib/api"
import {
  exportDispatchPlanPickingClientePdf,
  exportDispatchPlanPickingProductoPdf,
} from "@/lib/dispatch-plan-picking-pdf"
import {
  effectiveBoxes,
  groupRowsByCity,
  normalizePickingCategory,
} from "@/lib/picking-display"
import {
  filterPickingProductRows,
  paginatePickingProductRows,
  PICKING_PRODUCT_PAGE_SIZE,
  summarizePickingProductRows,
  uniquePickingProductCategories,
} from "@/lib/picking-product-table"
import { formatClp } from "@/lib/ors-map-ui"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
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
import { formatPeonetasDisplay } from "@/lib/peonetas-display"
import { cn } from "@/lib/utils"

function PickingHeaderBlock({ header }: { header: DispatchPlanPickingHeader }) {
  const chofer = header.driver_name || header.driver_label || "—"
  const peonetas = header.assistant_names?.length
    ? header.assistant_names.join(", ")
    : header.assistant_label && header.assistant_label !== "0 peonetas"
      ? header.assistant_label
      : formatPeonetasDisplay({
          assistant_count: header.assistant_count,
          peonetas_recorded: header.peonetas_recorded,
        })
  return (
    <div className="grid gap-2 rounded-lg border border-border/70 bg-muted/20 p-3 text-xs sm:grid-cols-2 lg:grid-cols-4">
      <div>
        <span className="text-muted-foreground">Planificación</span>
        <p className="font-mono font-semibold">{header.planning_number}</p>
      </div>
      <div>
        <span className="text-muted-foreground">Fecha entrega</span>
        <p className="font-medium">{header.delivery_date}</p>
      </div>
      <div>
        <span className="text-muted-foreground">Vehículo</span>
        <p className="font-medium">{header.truck_name}</p>
      </div>
      <div>
        <span className="text-muted-foreground">Chofer</span>
        <p className="font-medium">{chofer}</p>
      </div>
      <div>
        <span className="text-muted-foreground">Peonetas</span>
        <p className="font-medium">{peonetas || "No informado"}</p>
      </div>
      <div className="sm:col-span-2">
        <span className="text-muted-foreground">Ruta / comunas</span>
        <p className="font-medium">
          {header.route_name}
          {header.communes ? ` · ${header.communes}` : ""}
        </p>
      </div>
      <div className="sm:col-span-2">
        <span className="text-muted-foreground">Sello</span>
        <p className="whitespace-pre-wrap">{header.sello || "—"}</p>
      </div>
    </div>
  )
}

type ClientPanelProps = {
  planId: number
  pickingReady: boolean
  blockedMessage: string
  data: DispatchPlanPickingClientResponse | null
  loading: boolean
  onGenerate: (opts: { includeProbable: boolean }) => Promise<void>
  onRefresh?: () => Promise<void>
  onMessage: (msg: string) => void
}

export function DispatchPlanPickingClientePanel({
  planId,
  pickingReady,
  blockedMessage,
  data,
  loading,
  onGenerate,
  onRefresh,
  onMessage,
}: ClientPanelProps) {
  const [includeProbable, setIncludeProbable] = useState(false)
  const [busy, setBusy] = useState<string | null>(null)

  const runExport = useCallback(
    async (kind: "xlsx" | "pdf") => {
      if (!data?.header || !data.clients?.length) {
        onMessage("Cargue el picking antes de exportar.")
        return
      }
      setBusy(kind)
      try {
        if (kind === "xlsx") {
          await downloadDispatchPlanPickingClienteExcel(planId, {
            version: data.version,
            pickingId: data.picking_id,
          })
          onMessage("Excel de picking cliente descargado.")
        } else {
          await exportDispatchPlanPickingClientePdf({
            header: data.header,
            clients: data.clients,
            warnings: data.warnings,
            version: data.version,
            generatedAt: data.generated_at,
          })
          onMessage("PDF de picking cliente generado.")
        }
      } catch (e: unknown) {
        onMessage(e instanceof Error ? e.message : "Error al exportar")
      } finally {
        setBusy(null)
      }
    },
    [data, planId, includeProbable, onMessage],
  )

  const cityGroups = useMemo(
    () => (data?.clients?.length ? groupRowsByCity(data.clients) : []),
    [data?.clients],
  )

  return (
    <div className="space-y-3">
      {!pickingReady ? (
        <Alert>
          <AlertTitle>Picking limitado</AlertTitle>
          <AlertDescription className="text-sm">{blockedMessage}</AlertDescription>
        </Alert>
      ) : null}

      <div className="flex flex-wrap items-center gap-3">
        <div className="flex items-center gap-2">
          <Checkbox
            id="incl-prob-cliente"
            checked={includeProbable}
            onCheckedChange={(v) => setIncludeProbable(v === true)}
          />
          <Label htmlFor="incl-prob-cliente" className="text-xs font-normal">
            Incluir coincidencias probables (60–74) al generar
          </Label>
        </div>
        <Button
          type="button"
          size="sm"
          variant="default"
          disabled={loading}
          onClick={() => void onGenerate({ includeProbable })}
        >
          {loading ? (
            <Loader2 className="mr-1 size-3.5 animate-spin" />
          ) : null}
          {loading ? "Generando…" : "Generar picking (nueva versión)"}
        </Button>
        {onRefresh ? (
          <Button
            type="button"
            size="sm"
            variant="outline"
            disabled={loading}
            onClick={() => void onRefresh()}
          >
            <RefreshCw className="mr-1 size-3.5" />
            Recargar
          </Button>
        ) : null}
        <Button
          type="button"
          size="sm"
          variant="secondary"
          disabled={!!busy || !data?.clients?.length}
          onClick={() => void runExport("xlsx")}
        >
          {busy === "xlsx" ? (
            <Loader2 className="mr-1 size-3.5 animate-spin" />
          ) : (
            <Download className="mr-1 size-3.5" />
          )}
          Excel
        </Button>
        <Button
          type="button"
          size="sm"
          variant="secondary"
          disabled={!!busy || !data?.clients?.length}
          onClick={() => void runExport("pdf")}
        >
          {busy === "pdf" ? (
            <Loader2 className="mr-1 size-3.5 animate-spin" />
          ) : (
            <FileText className="mr-1 size-3.5" />
          )}
          PDF
        </Button>
      </div>

      {data?.ready === false ? (
        <Alert variant="destructive">
          <AlertDescription>{data.reason ?? blockedMessage}</AlertDescription>
        </Alert>
      ) : null}

      {data?.warnings?.length ? (
        <Alert className="border-amber-300 bg-amber-50/80 dark:border-amber-900 dark:bg-amber-950/30">
          <AlertTitle className="text-sm">Advertencias</AlertTitle>
          <AlertDescription>
            <ul className="mt-1 list-inside list-disc text-xs">
              {data.warnings.map((w, i) => (
                <li key={i}>{w}</li>
              ))}
            </ul>
          </AlertDescription>
        </Alert>
      ) : null}

      {data?.header ? <PickingHeaderBlock header={data.header} /> : null}

      {data?.version != null ? (
        <p className="text-xs text-muted-foreground">
          Versión persistida <strong>v{data.version}</strong>
          {data.picking_id ? ` (id ${data.picking_id})` : ""}
          {data.source === "persisted" ? " · fuente oficial SQL" : ""}
        </p>
      ) : null}

      {data?.clients?.length ? (
        <>
          <p className="text-xs text-muted-foreground">
            {data.totals?.stops ?? data.clients.length} paradas ·{" "}
            {formatClp(data.totals?.document_total_clp ?? 0)} total documentos
          </p>
          <div className="overflow-x-auto rounded-md border">
            <table className="w-full min-w-[960px] text-left text-xs">
              <thead className="bg-muted/50 text-[11px] uppercase text-muted-foreground">
                <tr>
                  <th className="px-2 py-2">Ord</th>
                  <th className="px-2 py-2">Ciudad</th>
                  <th className="px-2 py-2">Cliente</th>
                  <th className="px-2 py-2">Fantasía</th>
                  <th className="px-2 py-2">Dirección</th>
                  <th className="px-2 py-2">Celular</th>
                  <th className="px-2 py-2">N° doc</th>
                  <th className="px-2 py-2">Tipo</th>
                  <th className="px-2 py-2">Pago</th>
                  <th className="px-2 py-2">Vendedor</th>
                  <th className="px-2 py-2">Obs. entrega</th>
                  <th className="px-2 py-2 text-right">Peso pedido</th>
                  <th className="px-2 py-2 text-right">Total</th>
                </tr>
              </thead>
              <tbody>
                {cityGroups.map((group) => (
                  <Fragment key={group.cityKey}>
                    <tr className="bg-muted/40">
                      <td
                        colSpan={13}
                        className="px-2 py-1.5 text-[11px] font-semibold uppercase tracking-wide text-muted-foreground"
                      >
                        {group.cityLabel}
                      </td>
                    </tr>
                    {group.rows.map((row, idx) => (
                      <tr
                        key={`${row.related_document_id}-${group.cityKey}-${idx}`}
                        className={cn(
                          "border-t border-border/50",
                          row.is_probable_included && "bg-amber-50/50 dark:bg-amber-950/20",
                        )}
                      >
                        <td className="px-2 py-1.5 tabular-nums">{row.route_order}</td>
                        <td className="px-2 py-1.5">{row.city}</td>
                        <td className="px-2 py-1.5 font-medium">{row.client_name}</td>
                        <td className="px-2 py-1.5">{row.fantasy_name}</td>
                        <td className="px-2 py-1.5 max-w-[140px] truncate">{row.address}</td>
                        <td className="px-2 py-1.5">{row.phone || "—"}</td>
                        <td className="px-2 py-1.5 tabular-nums">{row.document_number}</td>
                        <td className="px-2 py-1.5">{row.document_type}</td>
                        <td className="px-2 py-1.5">{row.payment_method || "—"}</td>
                        <td className="px-2 py-1.5">{row.seller_name}</td>
                        <td className="px-2 py-1.5 max-w-[140px] truncate">
                          {[row.delivery_notes, row.observations]
                            .filter((x) => (x || "").trim())
                            .join(" · ") || "—"}
                        </td>
                        <td className="px-2 py-1.5 text-right tabular-nums text-muted-foreground">
                          {row.peso_total_kg != null && row.peso_total_kg > 0
                            ? `${row.peso_total_kg.toLocaleString("es-CL", { maximumFractionDigits: 0 })} kg`
                            : "—"}
                        </td>
                        <td className="px-2 py-1.5 text-right tabular-nums">
                          {formatClp(Number(row.document_total) || 0)}
                          {row.is_probable_included ? (
                            <Badge variant="outline" className="ml-1 text-[11px]">
                              probable
                            </Badge>
                          ) : row.inclusion === "auto_match" ? (
                            <Badge variant="outline" className="ml-1 border-sky-300 text-[11px]">
                              auto
                            </Badge>
                          ) : null}
                        </td>
                      </tr>
                    ))}
                  </Fragment>
                ))}
              </tbody>
            </table>
          </div>
        </>
      ) : data && data.ready !== false ? (
        <p className="text-sm text-muted-foreground">Sin paradas para los filtros actuales.</p>
      ) : null}
    </div>
  )
}

type ProductPanelProps = {
  planId: number
  pickingReady: boolean
  blockedMessage: string
  data: DispatchPlanPickingProductResponse | null
  loading: boolean
  onRefresh?: () => Promise<void>
  onMessage: (msg: string) => void
}

export function DispatchPlanPickingProductoPanel({
  planId,
  pickingReady,
  blockedMessage,
  data,
  loading,
  onRefresh,
  onMessage,
}: ProductPanelProps) {
  const [busy, setBusy] = useState<string | null>(null)
  const [query, setQuery] = useState("")
  const [category, setCategory] = useState<string>("__all__")
  const [page, setPage] = useState(1)

  const allItems = data?.items || []
  const categories = useMemo(
    () => uniquePickingProductCategories(allItems),
    [allItems],
  )
  const filtered = useMemo(
    () => filterPickingProductRows(allItems, { query, category }),
    [allItems, query, category],
  )
  const pageData = useMemo(
    () => paginatePickingProductRows(filtered, page, PICKING_PRODUCT_PAGE_SIZE),
    [filtered, page],
  )
  const summary = useMemo(() => summarizePickingProductRows(allItems), [allItems])

  useEffect(() => {
    setPage(1)
  }, [query, category, data?.version, data?.picking_id, allItems.length])

  useEffect(() => {
    if (page !== pageData.page) setPage(pageData.page)
  }, [page, pageData.page])

  const runExport = useCallback(
    async (kind: "xlsx" | "pdf") => {
      // Exportación SIEMPRE del picking completo (no la página filtrada).
      if (!data?.header || !data.items?.length) {
        onMessage("Cargue el picking antes de exportar.")
        return
      }
      setBusy(kind)
      try {
        if (kind === "xlsx") {
          await downloadDispatchPlanPickingProductoExcel(planId, {
            version: data.version,
            pickingId: data.picking_id,
          })
          onMessage("Excel de picking producto descargado (completo).")
        } else {
          await exportDispatchPlanPickingProductoPdf({
            header: data.header,
            items: data.items,
            version: data.version,
            generatedAt: data.generated_at,
          })
          onMessage("PDF de picking producto generado (completo).")
        }
      } catch (e: unknown) {
        onMessage(e instanceof Error ? e.message : "Error al exportar")
      } finally {
        setBusy(null)
      }
    },
    [data, planId, onMessage],
  )

  return (
    <div className="space-y-3">
      {!pickingReady ? (
        <Alert>
          <AlertTitle>Picking limitado</AlertTitle>
          <AlertDescription className="text-sm">{blockedMessage}</AlertDescription>
        </Alert>
      ) : null}

      <div className="flex flex-wrap items-center gap-3">
        <p className="text-xs text-muted-foreground">
          Use la pestaña Picking cliente para generar una nueva versión persistida.
        </p>
        {onRefresh ? (
          <Button
            type="button"
            size="sm"
            variant="outline"
            disabled={loading}
            onClick={() => void onRefresh()}
          >
            <RefreshCw className="mr-1 size-3.5" />
            Recargar
          </Button>
        ) : null}
        <Button
          type="button"
          size="sm"
          variant="secondary"
          disabled={!!busy || !data?.items?.length}
          onClick={() => void runExport("xlsx")}
        >
          {busy === "xlsx" ? (
            <Loader2 className="mr-1 size-3.5 animate-spin" />
          ) : (
            <Download className="mr-1 size-3.5" />
          )}
          Excel completo
        </Button>
        <Button
          type="button"
          size="sm"
          variant="secondary"
          disabled={!!busy || !data?.items?.length}
          onClick={() => void runExport("pdf")}
        >
          {busy === "pdf" ? (
            <Loader2 className="mr-1 size-3.5 animate-spin" />
          ) : (
            <FileText className="mr-1 size-3.5" />
          )}
          PDF completo
        </Button>
      </div>

      {data?.ready === false ? (
        <Alert variant="destructive">
          <AlertDescription>{data.reason ?? blockedMessage}</AlertDescription>
        </Alert>
      ) : null}

      {data?.warnings?.length ? (
        <Alert className="border-amber-300 bg-amber-50/80 dark:border-amber-900 dark:bg-amber-950/30">
          <AlertTitle className="text-sm">Advertencias</AlertTitle>
          <AlertDescription>
            <ul className="mt-1 list-inside list-disc text-xs">
              {data.warnings.map((w, i) => (
                <li key={i}>{w}</li>
              ))}
            </ul>
          </AlertDescription>
        </Alert>
      ) : null}

      {data?.header ? <PickingHeaderBlock header={data.header} /> : null}

      {data?.header?.peso_total_picking_kg != null &&
      data.header.peso_total_picking_kg > 0 ? (
        <p className="rounded-md border border-border/70 bg-muted/20 px-3 py-2 text-xs">
          <span className="text-muted-foreground">Peso total del picking: </span>
          <strong className="tabular-nums">
            {data.header.peso_total_picking_kg.toLocaleString("es-CL", {
              maximumFractionDigits: 1,
            })}{" "}
            kg
          </strong>
        </p>
      ) : null}

      {data?.version != null ? (
        <p className="text-xs text-muted-foreground">
          Versión persistida <strong>v{data.version}</strong>
        </p>
      ) : null}

      {data?.items?.length ? (
        <>
          <div className="rounded-md border bg-muted/15 px-3 py-2 text-xs">
            <p className="font-medium">
              Resumen · {summary.skus} SKU · {summary.units} u · {summary.boxes} cajas eq. ·{" "}
              {formatClp(data.totals?.total_monto_clp ?? summary.monto)}
            </p>
            <p className="mt-0.5 text-muted-foreground">
              Vista paginada ({PICKING_PRODUCT_PAGE_SIZE}/página). PDF/Excel exportan el
              listado completo, no solo la página visible.
            </p>
          </div>

          <div className="flex flex-wrap items-end gap-2">
            <div className="min-w-[180px] flex-1 space-y-1">
              <Label className="text-[11px]">Buscar producto / código</Label>
              <Input
                className="h-8"
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder="Nombre o código de barras"
              />
            </div>
            <div className="space-y-1">
              <Label className="text-[11px]">Categoría / tipo</Label>
              <Select value={category} onValueChange={setCategory}>
                <SelectTrigger className="h-8 w-[180px]">
                  <SelectValue placeholder="Todas" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="__all__">Todas</SelectItem>
                  {categories.map((c) => (
                    <SelectItem key={c} value={c}>
                      {c}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>

          <div className="overflow-x-auto rounded-md border">
            <table className="w-full min-w-[720px] text-left text-xs">
              <thead className="bg-muted/50 text-[11px] uppercase text-muted-foreground">
                <tr>
                  <th className="px-2 py-2">Bodega</th>
                  <th className="px-2 py-2">Tipo</th>
                  <th className="px-2 py-2">Producto</th>
                  <th className="px-2 py-2">Código</th>
                  <th className="px-2 py-2 text-right">U</th>
                  <th className="px-2 py-2 text-right">Cajas</th>
                  <th className="px-2 py-2 text-right">Monto</th>
                </tr>
              </thead>
              <tbody>
                {pageData.rows.map((row, idx) => (
                  <tr
                    key={`${row.codigo_barras || row.producto_variante || "r"}-${pageData.from + idx}`}
                    className="border-t border-border/50"
                  >
                    <td className="px-2 py-1.5">{row.sucursal_bodega}</td>
                    <td className="px-2 py-1.5">
                      {normalizePickingCategory(row.tipo_producto)}
                    </td>
                    <td className="px-2 py-1.5 font-medium">
                      {row.display_name || row.producto_variante}
                    </td>
                    <td className="px-2 py-1.5 font-mono text-[11px]">
                      {row.codigo_barras || "—"}
                    </td>
                    <td className="px-2 py-1.5 text-right tabular-nums">{row.unidades}</td>
                    <td className="px-2 py-1.5 text-right tabular-nums">
                      {row.sin_unidad_caja ? (
                        <Badge variant="outline" className="text-[11px]">
                          sin unidad caja
                        </Badge>
                      ) : (
                        effectiveBoxes(row)
                      )}
                    </td>
                    <td className="px-2 py-1.5 text-right tabular-nums">
                      {formatClp(Number(row.total_monto) || 0)}
                    </td>
                  </tr>
                ))}
                {!pageData.rows.length ? (
                  <tr>
                    <td
                      colSpan={7}
                      className="px-3 py-6 text-center text-muted-foreground"
                    >
                      Sin productos para el filtro actual.
                    </td>
                  </tr>
                ) : null}
              </tbody>
            </table>
          </div>

          <div className="flex flex-wrap items-center justify-between gap-2 text-xs">
            <p className="text-muted-foreground">
              Mostrando {pageData.from}–{pageData.to} de {pageData.total} productos
              {filtered.length !== allItems.length
                ? ` (filtrados de ${allItems.length})`
                : ""}
            </p>
            <div className="flex items-center gap-2">
              <Button
                type="button"
                size="sm"
                variant="outline"
                className="h-8"
                disabled={pageData.page <= 1}
                onClick={() => setPage((p) => Math.max(1, p - 1))}
              >
                <ChevronLeft className="mr-1 size-3.5" />
                Anterior
              </Button>
              <span className="tabular-nums text-muted-foreground">
                {pageData.page}/{pageData.totalPages}
              </span>
              <Button
                type="button"
                size="sm"
                variant="outline"
                className="h-8"
                disabled={pageData.page >= pageData.totalPages}
                onClick={() =>
                  setPage((p) => Math.min(pageData.totalPages, p + 1))
                }
              >
                Siguiente
                <ChevronRight className="ml-1 size-3.5" />
              </Button>
            </div>
          </div>
        </>
      ) : data && data.ready !== false ? (
        <p className="text-sm text-muted-foreground">Sin líneas consolidadas.</p>
      ) : null}
    </div>
  )
}
