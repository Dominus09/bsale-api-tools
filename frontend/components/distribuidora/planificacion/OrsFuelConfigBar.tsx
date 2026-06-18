"use client"

import { useEffect, useState } from "react"
import { Fuel, Loader2, Save } from "lucide-react"

import {
  getDistribuidoraPlanificacionFuelConfig,
  putDistribuidoraPlanificacionFuelConfig,
} from "@/lib/api"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { toast } from "@/hooks/use-toast"

type OrsFuelConfigBarProps = {
  onSaved?: () => void
}

export function OrsFuelConfigBar({ onSaved }: OrsFuelConfigBarProps) {
  const [dieselClp, setDieselClp] = useState<string>("")
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)

  useEffect(() => {
    const ac = new AbortController()
    ;(async () => {
      setLoading(true)
      try {
        const cfg = await getDistribuidoraPlanificacionFuelConfig({ signal: ac.signal })
        setDieselClp(String(Math.round(cfg.diesel_price_per_liter)))
      } catch {
        setDieselClp("1500")
      } finally {
        setLoading(false)
      }
    })()
    return () => ac.abort()
  }, [])

  const onSave = async () => {
    const n = Number(dieselClp.replace(/\s/g, ""))
    if (!Number.isFinite(n) || n <= 0) {
      toast({
        variant: "destructive",
        title: "Valor inválido",
        description: "Ingrese un precio diesel mayor a 0.",
      })
      return
    }
    setSaving(true)
    try {
      await putDistribuidoraPlanificacionFuelConfig(n)
      toast({ title: "Precio diesel guardado" })
      onSaved?.()
    } catch (e: unknown) {
      toast({
        variant: "destructive",
        title: "No se pudo guardar",
        description: e instanceof Error ? e.message : "Error",
      })
    } finally {
      setSaving(false)
    }
  }

  return (
    <div className="flex flex-wrap items-end gap-2 rounded-md border border-border/70 bg-muted/20 px-3 py-2">
      <div className="flex items-center gap-1.5 text-xs font-medium text-muted-foreground">
        <Fuel className="size-3.5" aria-hidden />
        Diesel CLP/L
      </div>
      {loading ? (
        <Loader2 className="size-4 animate-spin text-muted-foreground" />
      ) : (
        <>
          <div className="space-y-0.5">
            <Label htmlFor="ors-diesel-clp" className="sr-only">
              Valor diesel por litro
            </Label>
            <Input
              id="ors-diesel-clp"
              type="number"
              min={1}
              step={1}
              className="h-8 w-28 text-xs tabular-nums"
              value={dieselClp}
              onChange={(e) => setDieselClp(e.target.value)}
            />
          </div>
          <Button
            type="button"
            size="sm"
            variant="secondary"
            className="h-8 gap-1 text-xs"
            disabled={saving}
            onClick={() => void onSave()}
          >
            {saving ? (
              <Loader2 className="size-3 animate-spin" />
            ) : (
              <Save className="size-3" />
            )}
            Guardar
          </Button>
        </>
      )}
    </div>
  )
}
