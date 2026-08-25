"use client"

import { useEffect, useState } from "react"
import Link from "next/link"
import { useParams } from "next/navigation"
import { Loader2 } from "lucide-react"

import { getCarga, type LoadDetail } from "@/lib/cargas-api"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"

export default function CargaDetallePage() {
  const params = useParams()
  const loadId = Number(params.id)
  const [load, setLoad] = useState<LoadDetail | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    getCarga(loadId)
      .then(setLoad)
      .catch((e) => setError(e instanceof Error ? e.message : "Error"))
  }, [loadId])

  if (error) return <p className="p-6 text-destructive">{error}</p>
  if (!load) {
    return (
      <div className="flex justify-center py-20 text-muted-foreground">
        <Loader2 className="size-5 animate-spin" />
      </div>
    )
  }

  const s = load.summary
  return (
    <div className="mx-auto max-w-lg space-y-4 p-4 pb-16">
      <div className="flex items-start justify-between gap-2">
        <div>
          <h1 className="text-2xl font-bold">CARGA #{load.picking_number}</h1>
          <p className="text-sm text-muted-foreground">
            {load.destination || "—"} · {load.truck || "—"}
          </p>
        </div>
        <Badge variant="outline">{load.status}</Badge>
      </div>

      <div className="grid grid-cols-2 gap-3 rounded-xl border p-4 text-sm">
        <div>
          <p className="text-muted-foreground">SKU totales</p>
          <p className="text-xl font-bold">{s.total_items}</p>
        </div>
        <div>
          <p className="text-muted-foreground">Completos</p>
          <p className="text-xl font-bold">{s.items_complete}</p>
        </div>
        <div>
          <p className="text-muted-foreground">Parciales</p>
          <p className="text-xl font-bold">{s.items_partial}</p>
        </div>
        <div>
          <p className="text-muted-foreground">Pendientes</p>
          <p className="text-xl font-bold">{s.items_pending}</p>
        </div>
        <div>
          <p className="text-muted-foreground">Unidades sol.</p>
          <p className="text-xl font-bold">{s.requested_units}</p>
        </div>
        <div>
          <p className="text-muted-foreground">Unidades cert.</p>
          <p className="text-xl font-bold">{s.certified_units}</p>
        </div>
        <div>
          <p className="text-muted-foreground">Incidencias</p>
          <p className="text-xl font-bold">{s.open_issues || 0}</p>
        </div>
        <div>
          <p className="text-muted-foreground">Progreso</p>
          <p className="text-xl font-bold">{s.progress_pct}%</p>
        </div>
      </div>

      {load.certified_by ? (
        <p className="text-sm text-muted-foreground">
          Certificada por {load.certified_by}
          {load.certified_at ? ` · ${new Date(load.certified_at).toLocaleString("es-CL")}` : ""}
        </p>
      ) : null}

      {load.status !== "certified" ? (
        <Button asChild className="h-12 w-full">
          <Link href={`/logistica/cargas/${load.id}/certificar`}>Continuar certificación</Link>
        </Button>
      ) : null}
      <Button asChild variant="outline" className="h-11 w-full">
        <Link href="/logistica/cargas">Volver al listado</Link>
      </Button>
    </div>
  )
}
