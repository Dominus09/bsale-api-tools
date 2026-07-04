"use client"

import { Loader2 } from "lucide-react"
import { useEffect, useState } from "react"

import { Badge } from "@/components/ui/badge"
import { ScrollArea } from "@/components/ui/scroll-area"
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { formatCLP, formatDate } from "@/components/notas-credito/format"
import { getReturnsDetail, type ReturnsDetailResponse } from "@/lib/returns-analytics-api"

type Props = {
  returnId: number | null
  open: boolean
  onOpenChange: (open: boolean) => void
}

export function ReturnsDetailSheet({ returnId, open, onOpenChange }: Props) {
  const [data, setData] = useState<ReturnsDetailResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!open || returnId == null) {
      setData(null)
      setError(null)
      return
    }
    const ac = new AbortController()
    setLoading(true)
    setError(null)
    getReturnsDetail(returnId)
      .then(setData)
      .catch((e: Error) => setError(e.message || "Error al cargar ficha"))
      .finally(() => setLoading(false))
    return () => ac.abort()
  }, [open, returnId])

  const h = data?.header

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent className="w-full sm:max-w-2xl border-red-950/20 bg-gradient-to-b from-slate-950/5 to-background">
        <SheetHeader>
          <SheetTitle className="flex items-center gap-2 text-red-900 dark:text-red-300">
            Ficha NC
            {h?.number != null ? (
              <Badge variant="outline" className="font-mono">
                #{h.number}
              </Badge>
            ) : null}
          </SheetTitle>
          <SheetDescription>
            Auditoría de pérdida — documento origen, nota de crédito y margen estimado.
          </SheetDescription>
        </SheetHeader>

        {loading ? (
          <div className="flex items-center justify-center py-16 text-muted-foreground">
            <Loader2 className="mr-2 h-5 w-5 animate-spin" />
            Cargando ficha…
          </div>
        ) : error ? (
          <p className="mt-6 text-sm text-red-600">{error}</p>
        ) : data ? (
          <ScrollArea className="mt-4 h-[calc(100vh-8rem)] pr-4">
            <div className="space-y-6">
              <section className="grid gap-3 rounded-lg border border-red-900/15 bg-red-950/5 p-4 text-sm sm:grid-cols-2">
                <div>
                  <p className="text-xs uppercase tracking-wide text-muted-foreground">Fecha</p>
                  <p className="font-medium">{formatDate(h?.return_date)}</p>
                </div>
                <div>
                  <p className="text-xs uppercase tracking-wide text-muted-foreground">Monto NC</p>
                  <p className="font-semibold text-red-700 dark:text-red-400">{formatCLP(h?.amount)}</p>
                </div>
                <div>
                  <p className="text-xs uppercase tracking-wide text-muted-foreground">Cliente</p>
                  <p>{h?.client || "—"}</p>
                </div>
                <div>
                  <p className="text-xs uppercase tracking-wide text-muted-foreground">Vendedor</p>
                  <p>{h?.seller || "—"}</p>
                </div>
                <div className="sm:col-span-2">
                  <p className="text-xs uppercase tracking-wide text-muted-foreground">Motivo</p>
                  <p>{h?.motive || "Sin motivo"}</p>
                </div>
                <div>
                  <p className="text-xs uppercase tracking-wide text-muted-foreground">Comuna</p>
                  <p>{h?.municipality || "—"}</p>
                </div>
                <div>
                  <p className="text-xs uppercase tracking-wide text-muted-foreground">Margen est. total</p>
                  <p className="font-medium">{formatCLP(data.margin_estimated_total)}</p>
                </div>
              </section>

              <section className="grid gap-4 sm:grid-cols-2">
                <div className="rounded-lg border p-4">
                  <h3 className="mb-2 text-sm font-semibold">Documento origen</h3>
                  <dl className="space-y-1 text-sm">
                    <div className="flex justify-between gap-2">
                      <dt className="text-muted-foreground">Nº</dt>
                      <dd>{h?.reference_document?.number ?? "—"}</dd>
                    </div>
                    <div className="flex justify-between gap-2">
                      <dt className="text-muted-foreground">Emisión</dt>
                      <dd>{formatDate(h?.reference_document?.emission)}</dd>
                    </div>
                    <div className="flex justify-between gap-2">
                      <dt className="text-muted-foreground">Tipo</dt>
                      <dd>{h?.reference_document?.type_id ?? "—"}</dd>
                    </div>
                  </dl>
                </div>
                <div className="rounded-lg border p-4">
                  <h3 className="mb-2 text-sm font-semibold">Nota de crédito</h3>
                  <dl className="space-y-1 text-sm">
                    <div className="flex justify-between gap-2">
                      <dt className="text-muted-foreground">Nº</dt>
                      <dd>{h?.credit_note?.number ?? "—"}</dd>
                    </div>
                    <div className="flex justify-between gap-2">
                      <dt className="text-muted-foreground">Emisión</dt>
                      <dd>{formatDate(h?.credit_note?.emission)}</dd>
                    </div>
                    {h?.credit_note?.url ? (
                      <div className="pt-2">
                        <a
                          href={h.credit_note.url}
                          target="_blank"
                          rel="noreferrer"
                          className="text-xs text-red-700 underline dark:text-red-400"
                        >
                          Ver en Bsale
                        </a>
                      </div>
                    ) : null}
                  </dl>
                </div>
              </section>

              <section>
                <h3 className="mb-2 text-sm font-semibold">Productos devueltos</h3>
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Producto</TableHead>
                      <TableHead className="text-right">Cant.</TableHead>
                      <TableHead className="text-right">Costo u.</TableHead>
                      <TableHead className="text-right">Total</TableHead>
                      <TableHead className="text-right">Margen est.</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {data.lines.map((line) => (
                      <TableRow key={line.variant_id}>
                        <TableCell className="max-w-[200px] truncate" title={line.product || ""}>
                          {line.product || `Variante ${line.variant_id}`}
                        </TableCell>
                        <TableCell className="text-right tabular-nums">{line.quantity}</TableCell>
                        <TableCell className="text-right tabular-nums">{formatCLP(line.unit_cost)}</TableCell>
                        <TableCell className="text-right tabular-nums">{formatCLP(line.total_amount)}</TableCell>
                        <TableCell className="text-right tabular-nums text-red-700 dark:text-red-400">
                          {formatCLP(line.margin_estimated)}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </section>
            </div>
          </ScrollArea>
        ) : null}
      </SheetContent>
    </Sheet>
  )
}
