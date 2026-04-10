"use client"

import { useEffect, useMemo, useState } from "react"
import { Loader2 } from "lucide-react"

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import type { DistribuidoraRecord } from "@/lib/api"

type DistribuidoraRecordsViewProps = {
  title: string
  description?: string
  /** Referencia estable (p. ej. `getDistribuidoraRutero` importada). */
  loadRows: () => Promise<DistribuidoraRecord[]>
}

function formatCell(value: unknown): string {
  if (value === null || value === undefined) return "—"
  if (typeof value === "object") return JSON.stringify(value)
  return String(value)
}

export function DistribuidoraRecordsView({
  title,
  description,
  loadRows,
}: DistribuidoraRecordsViewProps) {
  const [rows, setRows] = useState<DistribuidoraRecord[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState("")

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError("")
    loadRows()
      .then((data) => {
        if (!cancelled) setRows(data)
      })
      .catch((e: unknown) => {
        if (!cancelled) setError(e instanceof Error ? e.message : "Error al cargar datos")
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [loadRows])

  const columns = useMemo(() => {
    if (rows.length === 0) return [] as string[]
    return Object.keys(rows[0])
  }, [rows])

  return (
    <div className="space-y-6 p-4 md:p-6">
      <Card>
        <CardHeader>
          <CardTitle>{title}</CardTitle>
          {description ? <CardDescription>{description}</CardDescription> : null}
        </CardHeader>
        <CardContent>
          {loading ? (
            <div className="flex items-center gap-2 text-muted-foreground">
              <Loader2 className="h-5 w-5 animate-spin" />
              Cargando…
            </div>
          ) : error ? (
            <p className="text-sm text-destructive">{error}</p>
          ) : rows.length === 0 ? (
            <p className="text-sm text-muted-foreground">No hay registros.</p>
          ) : (
            <div className="max-h-[min(70vh,720px)] overflow-auto rounded-md border">
              <table className="w-full min-w-max text-left text-sm">
                <thead className="sticky top-0 z-10 border-b bg-muted/80 backdrop-blur">
                  <tr>
                    {columns.map((col) => (
                      <th key={col} className="whitespace-nowrap px-3 py-2 font-medium">
                        {col}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {rows.map((row, i) => (
                    <tr key={i} className="border-b border-border/60 last:border-0">
                      {columns.map((col) => (
                        <td key={col} className="max-w-[280px] truncate px-3 py-2 align-top">
                          {formatCell(row[col])}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
