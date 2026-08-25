"use client"

import { useCallback, useEffect, useState } from "react"
import Link from "next/link"
import { Loader2, Plus, Truck } from "lucide-react"

import { listCargas, type LoadListRow } from "@/lib/cargas-api"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { cn } from "@/lib/utils"

const STATUS_LABEL: Record<string, string> = {
  draft: "Borrador",
  pending: "Pendiente",
  in_progress: "En proceso",
  completed: "Completa",
  certified: "Certificada",
  cancelled: "Anulada",
}

function formatDate(v?: string | null) {
  if (!v) return "—"
  const d = new Date(v.includes("T") ? v : `${v}T12:00:00`)
  if (Number.isNaN(d.getTime())) return v
  return d.toLocaleDateString("es-CL")
}

export default function CargasPage() {
  const [rows, setRows] = useState<LoadListRow[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      setRows(await listCargas({ limit: 80 }))
    } catch (e) {
      setError(e instanceof Error ? e.message : "Error al cargar")
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    void load()
  }, [load])

  return (
    <div className="mx-auto max-w-3xl space-y-4 pb-10">
      <div className="flex items-center justify-between gap-3">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Cargas</h1>
          <p className="text-sm text-muted-foreground">
            Certificación física de picking en camión
          </p>
        </div>
        <Button asChild className="h-11 px-4">
          <Link href="/logistica/cargas/nueva">
            <Plus className="mr-1.5 size-4" />
            Nueva
          </Link>
        </Button>
      </div>

      {error ? (
        <p className="rounded-md border border-destructive/40 bg-destructive/5 px-3 py-2 text-sm text-destructive">
          {error}
        </p>
      ) : null}

      {loading ? (
        <div className="flex items-center justify-center gap-2 py-20 text-muted-foreground">
          <Loader2 className="size-5 animate-spin" />
          Cargando…
        </div>
      ) : rows.length === 0 ? (
        <div className="rounded-xl border border-dashed px-6 py-16 text-center">
          <Truck className="mx-auto mb-3 size-10 text-muted-foreground/60" />
          <p className="font-medium">Sin cargas aún</p>
          <p className="mt-1 text-sm text-muted-foreground">
            Importe un Excel o PDF de picking para comenzar.
          </p>
          <Button asChild className="mt-4">
            <Link href="/logistica/cargas/nueva">Importar picking</Link>
          </Button>
        </div>
      ) : (
        <div className="space-y-3">
          {rows.map((row) => {
            const done = row.items_complete ?? 0
            const total = row.total_items ?? 0
            const pct = row.progress_pct ?? 0
            return (
              <Link
                key={row.id}
                href={
                  row.status === "certified"
                    ? `/logistica/cargas/${row.id}`
                    : `/logistica/cargas/${row.id}/certificar`
                }
                className="block rounded-xl border bg-card p-4 shadow-sm transition hover:border-primary/40"
              >
                <div className="flex items-start justify-between gap-2">
                  <div>
                    <p className="text-lg font-bold">CARGA #{row.picking_number}</p>
                    <p className="text-sm text-muted-foreground">
                      {(row.destination || "—").toUpperCase()}
                      {row.truck ? ` · ${row.truck}` : ""}
                    </p>
                    <p className="mt-0.5 text-xs text-muted-foreground">
                      {formatDate(row.picking_date || row.created_at)}
                    </p>
                  </div>
                  <Badge
                    variant="outline"
                    className={cn(
                      row.status === "certified" && "border-emerald-500 text-emerald-700",
                      row.status === "in_progress" && "border-amber-500 text-amber-800",
                    )}
                  >
                    {STATUS_LABEL[row.status] || row.status}
                  </Badge>
                </div>
                <div className="mt-3">
                  <div className="mb-1 flex justify-between text-xs text-muted-foreground">
                    <span>
                      {done} / {total} SKU
                    </span>
                    <span>{pct}%</span>
                  </div>
                  <div className="h-2 overflow-hidden rounded-full bg-muted">
                    <div
                      className="h-full rounded-full bg-primary transition-all"
                      style={{ width: `${Math.min(100, pct)}%` }}
                    />
                  </div>
                </div>
              </Link>
            )
          })}
        </div>
      )}
    </div>
  )
}
