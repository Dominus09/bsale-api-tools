"use client"

import { AlertTriangle, Bell, TrendingDown, UserX } from "lucide-react"

import type { CommercialAlert } from "@/lib/api"
import { Badge } from "@/components/ui/badge"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { cn } from "@/lib/utils"

const ALERT_ICONS: Record<string, React.ReactNode> = {
  vip_perdido: <UserX className="h-4 w-4 text-red-500" />,
  cliente_perdido: <UserX className="h-4 w-4 text-red-500" />,
  ruta_caida: <TrendingDown className="h-4 w-4 text-amber-500" />,
  ticket_reducido: <AlertTriangle className="h-4 w-4 text-amber-500" />,
  producto_oportunidad: <Bell className="h-4 w-4 text-blue-500" />,
  cliente_recuperado: <Bell className="h-4 w-4 text-emerald-500" />,
}

export function CommercialAlertsStrip({
  alerts,
  onClientClick,
  onSellerClick,
}: {
  alerts: CommercialAlert[]
  onClientClick?: (id: number) => void
  onSellerClick?: (name: string) => void
}) {
  if (!alerts.length) return null

  return (
    <Card className="border-amber-500/30 bg-gradient-to-r from-amber-500/5 to-background">
      <CardHeader className="pb-2">
        <CardTitle className="flex items-center gap-2 text-base">
          <Bell className="h-4 w-4 text-amber-600" />
          Alertas Inteligentes
          <Badge variant="destructive" className="ml-auto">
            {alerts.length}
          </Badge>
        </CardTitle>
      </CardHeader>
      <CardContent className="grid gap-2 md:grid-cols-2 xl:grid-cols-3">
        {alerts.slice(0, 9).map((a, i) => (
          <div
            key={i}
            className={cn(
              "flex gap-3 rounded-lg border bg-background/80 p-3 text-sm",
              a.client_id && "cursor-pointer hover:bg-muted/50",
            )}
            onClick={() => a.client_id && onClientClick?.(a.client_id)}
          >
            <div className="mt-0.5 shrink-0">{ALERT_ICONS[a.tipo] ?? <Bell className="h-4 w-4" />}</div>
            <div className="min-w-0">
              <div className="mb-1 flex flex-wrap gap-1">
                <Badge variant={a.prioridad === "alta" ? "destructive" : "secondary"} className="text-[10px]">
                  {a.prioridad}
                </Badge>
                {a.vendedor && (
                  <button
                    type="button"
                    className="text-[10px] text-muted-foreground underline-offset-2 hover:underline"
                    onClick={(e) => {
                      e.stopPropagation()
                      onSellerClick?.(a.vendedor!)
                    }}
                  >
                    {a.vendedor}
                  </button>
                )}
              </div>
              <p className="font-medium leading-snug">{a.mensaje}</p>
              <p className="mt-1 text-xs text-primary">{a.accion}</p>
            </div>
          </div>
        ))}
      </CardContent>
    </Card>
  )
}
