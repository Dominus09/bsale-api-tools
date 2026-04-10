"use client"

import { useEffect, useState } from "react"
import { Loader2 } from "lucide-react"

import { getDistribuidoraSinGeoref, type DistribuidoraRecord } from "@/lib/api"

function str(v: unknown): string {
  if (v == null || v === "") return "—"
  return String(v)
}

function nombreCliente(row: DistribuidoraRecord): string {
  const nf = row.nombre_fantasia
  const fan = typeof nf === "string" ? nf.trim() : ""
  if (fan) return fan
  const fn = typeof row.first_name === "string" ? row.first_name.trim() : ""
  const ln = typeof row.last_name === "string" ? row.last_name.trim() : ""
  const full = `${fn} ${ln}`.trim()
  if (full) return full
  const id = row.bsale_id
  if (typeof id === "number" || typeof id === "string") return `Cliente #${id}`
  return "—"
}

export default function SinGeorefPage() {
  const [rows, setRows] = useState<DistribuidoraRecord[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState("")

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError("")
    getDistribuidoraSinGeoref()
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
  }, [])

  return (
    <div className="space-y-4 p-6">
      <div>
        <h1 className="text-xl font-semibold tracking-tight">Sin georeferencia</h1>
        <p className="mt-1 max-w-2xl text-sm text-muted-foreground">
          Clientes del rutero activos sin latitud o longitud. Actualiza coordenadas en origen para que
          aparezcan en el mapa rutero.
        </p>
        {!loading && !error ? (
          <p className="mt-2 text-sm text-muted-foreground">
            Registros: <span className="font-medium tabular-nums text-foreground">{rows.length}</span>
          </p>
        ) : null}
      </div>

      {loading ? (
        <div className="flex items-center gap-2 text-sm text-muted-foreground">
          <Loader2 className="h-5 w-5 animate-spin shrink-0" />
          Cargando…
        </div>
      ) : error ? (
        <p className="text-sm text-destructive">{error}</p>
      ) : rows.length === 0 ? (
        <p className="text-sm text-muted-foreground">No hay clientes sin georeferencia.</p>
      ) : (
        <div className="overflow-x-auto rounded-lg border border-border">
          <table className="w-full min-w-[640px] border-collapse text-left text-sm">
            <thead>
              <tr className="border-b border-border bg-muted/60">
                <th className="whitespace-nowrap px-4 py-3 font-semibold text-foreground">Nombre</th>
                <th className="whitespace-nowrap px-4 py-3 font-semibold text-foreground">Vendedor</th>
                <th className="whitespace-nowrap px-4 py-3 font-semibold text-foreground">Municipio</th>
                <th className="whitespace-nowrap px-4 py-3 font-semibold text-foreground">Dirección</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row, i) => {
                const key =
                  typeof row.bsale_id === "number" || typeof row.bsale_id === "string"
                    ? String(row.bsale_id)
                    : `row-${i}`
                return (
                  <tr
                    key={key}
                    className="border-b border-border/80 last:border-0 odd:bg-background even:bg-muted/20"
                  >
                    <td className="max-w-[220px] px-4 py-2.5 align-top font-medium text-foreground">
                      <span className="line-clamp-2">{nombreCliente(row)}</span>
                    </td>
                    <td className="whitespace-nowrap px-4 py-2.5 align-top text-muted-foreground">
                      {str(row.vendedor)}
                    </td>
                    <td className="max-w-[180px] px-4 py-2.5 align-top text-muted-foreground">
                      <span className="line-clamp-2">{str(row.municipality)}</span>
                    </td>
                    <td className="max-w-[280px] px-4 py-2.5 align-top text-muted-foreground">
                      <span className="line-clamp-3">{str(row.address)}</span>
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
