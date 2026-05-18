"use client"

import { useCallback, useState } from "react"
import { Loader2 } from "lucide-react"

import { VendedoresOperacionesTable } from "@/components/operaciones/vendedores-table"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { useOperacionesPoll } from "@/hooks/use-operaciones-poll"
import { getOperacionesVendedores, localIsoDate } from "@/services/operaciones"

export default function OperacionesVendedoresPage() {
  const [fecha, setFecha] = useState(localIsoDate())
  const loader = useCallback(() => getOperacionesVendedores(fecha), [fecha])
  const { data, loading, error } = useOperacionesPoll(loader, [fecha])

  return (
    <div className="space-y-6">
      <div className="flex flex-col gap-4 sm:flex-row sm:items-end sm:justify-between">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Vendedores</h1>
          <p className="text-sm text-muted-foreground">Estado operacional del día</p>
        </div>
        <Input type="date" value={fecha} onChange={(e) => setFecha(e.target.value)} className="w-[160px]" />
      </div>
      {error ? <p className="text-sm text-destructive">{error}</p> : null}
      <Card>
        <CardHeader>
          <CardTitle>Listado</CardTitle>
        </CardHeader>
        <CardContent>
          {loading && !data ? (
            <div className="flex justify-center py-12">
              <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
            </div>
          ) : (
            <VendedoresOperacionesTable items={data?.items ?? []} />
          )}
        </CardContent>
      </Card>
    </div>
  )
}

