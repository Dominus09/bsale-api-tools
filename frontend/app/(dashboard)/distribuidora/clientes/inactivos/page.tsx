"use client"

import { useCallback, useEffect, useState } from "react"
import Link from "next/link"
import { Loader2, Users } from "lucide-react"

import { getDistribuidoraClientsInactive, type DistribuidoraClientInactive } from "@/lib/api"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
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

function formatDateTime(iso: string | null | undefined): string {
  if (!iso) return "—"
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return iso
  return d.toLocaleString("es-CL", { dateStyle: "short", timeStyle: "short" })
}

const DAY_PRESETS = [7, 14, 30] as const

export default function DistribuidoraClientesInactivosPage() {
  const [days, setDays] = useState<number>(14)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [rows, setRows] = useState<DistribuidoraClientInactive[]>([])

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const res = await getDistribuidoraClientsInactive({ days, limit: 2000 })
      setRows(res.items)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Error al cargar")
      setRows([])
    } finally {
      setLoading(false)
    }
  }, [days])

  useEffect(() => {
    void load()
  }, [load])

  return (
    <div className="mx-auto flex max-w-[1200px] flex-col gap-8 pb-12">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
            Distribuidora · Clientes
          </p>
          <h1 className="mt-1 text-2xl font-semibold tracking-tight">Clientes inactivos</h1>
          <p className="mt-2 max-w-2xl text-sm text-muted-foreground">
            Clientes con última compra (boleta/factura) hace al menos el umbral indicado.
          </p>
        </div>
        <Button variant="outline" size="sm" asChild>
          <Link href="/distribuidora/clientes">Volver a cartera</Link>
        </Button>
      </div>

      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-base">Umbral rápido</CardTitle>
        </CardHeader>
        <CardContent className="flex flex-wrap gap-2">
          {DAY_PRESETS.map((d) => (
            <Button
              key={d}
              type="button"
              variant={days === d ? "default" : "outline"}
              size="sm"
              onClick={() => setDays(d)}
            >
              {d} días
            </Button>
          ))}
          <Button type="button" variant="ghost" size="sm" onClick={() => void load()} disabled={loading}>
            <Loader2 className={cn("mr-1 h-4 w-4", loading && "animate-spin")} />
            Recargar
          </Button>
        </CardContent>
      </Card>

      {error ? (
        <Alert variant="destructive">
          <AlertTitle>Error</AlertTitle>
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      ) : null}

      {loading ? (
        <div className="flex min-h-[200px] items-center justify-center gap-2 text-muted-foreground">
          <Loader2 className="h-8 w-8 animate-spin" />
          <span>Cargando…</span>
        </div>
      ) : (
        <Card>
          <CardHeader>
            <CardTitle className="text-base flex items-center gap-2">
              <Users className="h-4 w-4" />
              Inactivos ≥ {days} días
            </CardTitle>
            <p className="text-sm text-muted-foreground">
              Rojo: más de 30 días sin comprar. Orden: más días primero.
            </p>
          </CardHeader>
          <CardContent>
            <div className="max-h-[560px] overflow-auto rounded-md border">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Cliente</TableHead>
                    <TableHead>Vendedor (última venta)</TableHead>
                    <TableHead>Última compra</TableHead>
                    <TableHead className="text-right">Días sin comprar</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {rows.map((r) => {
                    const d = Number(r.dias_sin_comprar ?? 0)
                    const risk = d >= 30
                    return (
                      <TableRow
                        key={r.client_id}
                        className={cn(
                          risk && "bg-red-50 text-red-950 dark:bg-red-950/35 dark:text-red-50",
                          !risk && d >= 14 && "bg-amber-50/80 dark:bg-amber-950/20",
                        )}
                      >
                        <TableCell className="font-medium">
                          {(r.client_name ?? "").trim() || `Cliente ${r.client_id}`}
                        </TableCell>
                        <TableCell className="text-muted-foreground">
                          {(r.vendedor ?? "").trim() || "—"}
                        </TableCell>
                        <TableCell className="tabular-nums text-muted-foreground">
                          {formatDateTime(r.ultima_compra)}
                        </TableCell>
                        <TableCell className="text-right font-semibold tabular-nums">{d}</TableCell>
                      </TableRow>
                    )
                  })}
                </TableBody>
              </Table>
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  )
}
