"use client"

import { use, useState } from "react"
import Link from "next/link"
import { ArrowLeft } from "lucide-react"

import { DispatchPlanCuadraturaPanel } from "@/components/distribuidora/planificacion/DispatchPlanCuadraturaPanel"
import { Alert, AlertDescription } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"

type PageProps = {
  params: Promise<{ id: string }>
}

export default function CuadraturaDetailPage({ params }: PageProps) {
  const { id } = use(params)
  const planId = Number(id)
  const [message, setMessage] = useState<string | null>(null)

  if (!Number.isFinite(planId) || planId <= 0) {
    return (
      <div className="p-6">
        <p className="text-sm text-destructive">Identificador de plan inválido.</p>
      </div>
    )
  }

  return (
    <div className="space-y-4 p-6">
      <div className="flex flex-wrap items-center gap-3">
        <Button asChild variant="ghost" size="sm">
          <Link href="/distribuidora/cuadraturas">
            <ArrowLeft className="mr-1 size-4" />
            Cuadraturas
          </Link>
        </Button>
        <div>
          <p className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
            Cuadratura operacional
          </p>
          <h1 className="text-xl font-semibold tracking-tight">Plan #{planId}</h1>
        </div>
      </div>

      {message ? (
        <Alert>
          <AlertDescription>{message}</AlertDescription>
        </Alert>
      ) : null}

      <DispatchPlanCuadraturaPanel
        planId={planId}
        showPlanLink
        onMessage={(msg) => setMessage(msg)}
      />
    </div>
  )
}
