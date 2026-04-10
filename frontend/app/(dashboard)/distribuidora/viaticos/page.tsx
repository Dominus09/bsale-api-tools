"use client"

import { Wallet } from "lucide-react"

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"

export default function DistribuidoraViaticosPage() {
  return (
    <div className="space-y-6 p-4 md:p-6">
      <Card>
        <CardHeader>
          <div className="flex items-center gap-2">
            <Wallet className="h-5 w-5 text-muted-foreground" />
            <CardTitle>Viáticos</CardTitle>
          </div>
          <CardDescription>
            Módulo en preparación. Aquí podrás gestionar viáticos de la distribuidora.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <p className="text-sm text-muted-foreground">Próximamente.</p>
        </CardContent>
      </Card>
    </div>
  )
}
