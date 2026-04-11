"use client"

import { useCallback, useEffect, useState } from "react"
import { Loader2 } from "lucide-react"

import { Button } from "@/components/ui/button"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import {
  getDistribuidoraMapa,
  getDistribuidoraPendientes,
  postDistribuidoraPendientesAsignarDia,
  type DistribuidoraRecord,
} from "@/lib/api"

const SELECT_CLASS =
  "h-9 w-full max-w-xs rounded-md border border-input bg-background px-3 text-sm shadow-sm ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"

const DIAS_FALLBACK = [
  "Lunes",
  "Martes",
  "Miércoles",
  "Jueves",
  "Viernes",
  "Sábado",
  "Domingo",
]

function uniqueSorted(vals: (string | null | undefined)[]): string[] {
  return [...new Set(vals.map((v) => (v ?? "").trim()).filter(Boolean))].sort((a, b) =>
    a.localeCompare(b, "es"),
  )
}

function nombreCliente(c: DistribuidoraRecord): string {
  const fan = String(c.nombre_fantasia ?? "").trim()
  if (fan) return fan
  const fn = String(c.first_name ?? "").trim()
  const ln = String(c.last_name ?? "").trim()
  const full = `${fn} ${ln}`.trim()
  const id = Number(c.bsale_id)
  return full || (Number.isFinite(id) ? `Cliente #${id}` : "Cliente")
}

function bsaleIdDeFila(c: DistribuidoraRecord): number | null {
  const n = Number(c.bsale_id)
  return Number.isFinite(n) && n > 0 ? n : null
}

export default function PendientesVistaClient() {
  const [filas, setFilas] = useState<DistribuidoraRecord[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState("")
  const [diaOptions, setDiaOptions] = useState<string[]>([])

  const [dialogOpen, setDialogOpen] = useState(false)
  const [filaActiva, setFilaActiva] = useState<DistribuidoraRecord | null>(null)
  const [diaElegido, setDiaElegido] = useState("")
  const [guardando, setGuardando] = useState(false)
  const [dialogError, setDialogError] = useState("")

  const cargar = useCallback(async () => {
    setLoading(true)
    setError("")
    try {
      const [pend, mapa] = await Promise.all([getDistribuidoraPendientes(), getDistribuidoraMapa()])
      setFilas(pend)
      const clientes = Array.isArray(mapa.clientes) ? mapa.clientes : []
      const dias = uniqueSorted(clientes.map((c) => c.dia_atencion))
      setDiaOptions(dias.length > 0 ? dias : DIAS_FALLBACK)
    } catch (e: unknown) {
      setFilas([])
      setError(e instanceof Error ? e.message : "Error al cargar pendientes")
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    void cargar()
  }, [cargar])

  const abrirAsignar = useCallback((row: DistribuidoraRecord) => {
    setFilaActiva(row)
    setDiaElegido(diaOptions[0] ?? DIAS_FALLBACK[0] ?? "")
    setDialogError("")
    setDialogOpen(true)
  }, [diaOptions])

  const cerrarDialog = useCallback(() => {
    setDialogOpen(false)
    setFilaActiva(null)
    setDialogError("")
  }, [])

  const confirmarAsignar = useCallback(async () => {
    if (!filaActiva) return
    const bid = bsaleIdDeFila(filaActiva)
    if (bid == null) {
      setDialogError("Fila sin bsale_id válido.")
      return
    }
    const d = diaElegido.trim()
    if (!d) {
      setDialogError("Elige un día.")
      return
    }
    setGuardando(true)
    setDialogError("")
    try {
      await postDistribuidoraPendientesAsignarDia({ bsale_id: bid, dia_atencion: d })
      cerrarDialog()
      await cargar()
    } catch (e: unknown) {
      setDialogError(e instanceof Error ? e.message : "Error al guardar")
    } finally {
      setGuardando(false)
    }
  }, [filaActiva, diaElegido, cerrarDialog, cargar])

  return (
    <div className="space-y-4 p-4 md:p-6">
      <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
        <div>
          <h1 className="text-xl font-semibold tracking-tight text-foreground">Pendientes</h1>
          <p className="mt-1 max-w-2xl text-sm text-muted-foreground">
            Clientes de vendedores de ruta sin día de atención asignado. Al asignar día quedan listos para
            sincronizar al rutero según tu proceso en base de datos.
          </p>
        </div>
        <Button type="button" variant="outline" size="sm" disabled={loading} onClick={() => void cargar()}>
          Actualizar
        </Button>
      </div>

      {error ? (
        <p className="text-sm text-destructive" role="alert">
          {error}
        </p>
      ) : null}

      {loading ? (
        <div className="flex items-center gap-2 text-muted-foreground">
          <Loader2 className="h-5 w-5 animate-spin" aria-hidden />
          Cargando…
        </div>
      ) : filas.length === 0 ? (
        <p className="text-sm text-muted-foreground">No hay clientes pendientes de asignar día.</p>
      ) : (
        <div className="max-h-[min(75vh,800px)] overflow-auto rounded-md border border-border">
          <table className="w-full min-w-[640px] border-collapse text-left text-sm">
            <thead className="sticky top-0 z-10 border-b border-border bg-muted/90 backdrop-blur">
              <tr>
                <th className="min-w-[200px] px-3 py-2 font-medium">Cliente</th>
                <th className="px-3 py-2 font-medium">Municipio</th>
                <th className="whitespace-nowrap px-3 py-2 font-medium">Vendedor</th>
                <th className="whitespace-nowrap px-3 py-2 font-medium">Acción</th>
              </tr>
            </thead>
            <tbody>
              {filas.map((row, i) => {
                const bid = bsaleIdDeFila(row)
                const key = bid != null ? `b-${bid}` : `i-${i}`
                return (
                  <tr key={key} className="border-b border-border/70 last:border-0">
                    <td className="px-3 py-2 font-medium text-foreground">{nombreCliente(row)}</td>
                    <td className="px-3 py-2 text-muted-foreground">
                      {String(row.municipality ?? "").trim() || "—"}
                    </td>
                    <td className="whitespace-nowrap px-3 py-2">{String(row.vendedor ?? "").trim() || "—"}</td>
                    <td className="px-3 py-2">
                      <Button
                        type="button"
                        variant="secondary"
                        size="sm"
                        disabled={bid == null}
                        onClick={() => abrirAsignar(row)}
                      >
                        Asignar día
                      </Button>
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      )}

      <Dialog open={dialogOpen} onOpenChange={(open) => !open && cerrarDialog()}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle>Asignar día de atención</DialogTitle>
            <DialogDescription>
              {filaActiva ? (
                <>
                  Cliente: <span className="font-medium text-foreground">{nombreCliente(filaActiva)}</span>
                </>
              ) : null}
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-2 py-2">
            <label htmlFor="pendiente-dia" className="text-sm font-medium text-foreground">
              Día
            </label>
            <select
              id="pendiente-dia"
              className={SELECT_CLASS}
              value={diaElegido}
              onChange={(e) => setDiaElegido(e.target.value)}
              aria-label="Día de atención"
            >
              {diaOptions.map((d) => (
                <option key={d} value={d}>
                  {d}
                </option>
              ))}
            </select>
            {dialogError ? (
              <p className="text-sm text-destructive" role="alert">
                {dialogError}
              </p>
            ) : null}
          </div>
          <DialogFooter>
            <Button type="button" variant="outline" onClick={cerrarDialog} disabled={guardando}>
              Cancelar
            </Button>
            <Button type="button" disabled={guardando} onClick={() => void confirmarAsignar()}>
              {guardando ? (
                <>
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" aria-hidden />
                  Guardando…
                </>
              ) : (
                "Guardar"
              )}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}
