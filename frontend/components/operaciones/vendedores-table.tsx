"use client"

import Link from "next/link"
import { MapPin } from "lucide-react"

import { EstadoConexionBadge } from "@/components/operaciones/estado-badge"
import { Button } from "@/components/ui/button"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import type { VendedorOperacionesRow } from "@/services/operaciones"

function formatSync(iso: string | null): string {
  if (!iso) return "—"
  return new Date(iso).toLocaleTimeString("es-CL", { hour: "2-digit", minute: "2-digit" })
}

export function VendedoresOperacionesTable({ items }: { items: VendedorOperacionesRow[] }) {
  if (!items.length) {
    return (
      <p className="py-8 text-center text-sm text-muted-foreground">No hay vendedores activos para esta fecha.</p>
    )
  }

  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Vendedor</TableHead>
          <TableHead>Estado</TableHead>
          <TableHead className="text-right">Visitas</TableHead>
          <TableHead className="text-right">Pend.</TableHead>
          <TableHead className="text-right">Inc.</TableHead>
          <TableHead className="text-right">Avance</TableHead>
          <TableHead>Última sync</TableHead>
          <TableHead className="text-right">Km</TableHead>
          <TableHead className="text-right">GPS</TableHead>
          <TableHead />
        </TableRow>
      </TableHeader>
      <TableBody>
        {items.map((v) => (
          <TableRow key={v.codigo}>
            <TableCell>
              <div className="font-medium">{v.nombre}</div>
              <div className="text-xs text-muted-foreground">{v.codigo}</div>
            </TableCell>
            <TableCell>
              <EstadoConexionBadge estado={v.estado_conexion} />
            </TableCell>
            <TableCell className="text-right tabular-nums">{v.visitas_realizadas}</TableCell>
            <TableCell className="text-right tabular-nums">{v.visitas_pendientes}</TableCell>
            <TableCell className="text-right tabular-nums">{v.incidencias}</TableCell>
            <TableCell className="text-right tabular-nums">{v.porcentaje_avance.toFixed(0)}%</TableCell>
            <TableCell className="text-sm">{formatSync(v.ultima_sync)}</TableCell>
            <TableCell className="text-right tabular-nums">{v.kilometros_recorridos.toFixed(1)}</TableCell>
            <TableCell className="text-right text-xs text-muted-foreground">
              {v.gps?.lat != null ? `${v.gps.lat.toFixed(4)}, ${v.gps.lon?.toFixed(4)}` : "—"}
            </TableCell>
            <TableCell className="text-right">
              <Button variant="ghost" size="sm" asChild>
                <Link href={`/operaciones/vendedor/${encodeURIComponent(v.codigo)}`}>Detalle</Link>
              </Button>
              {v.ruta_id ? (
                <Button variant="ghost" size="icon" asChild>
                  <Link href={`/operaciones/mapa?ruta=${v.ruta_id}`} title="Ver mapa">
                    <MapPin className="h-4 w-4" />
                  </Link>
                </Button>
              ) : null}
            </TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  )
}
