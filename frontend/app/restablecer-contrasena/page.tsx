"use client"

import { useState } from "react"
import Link from "next/link"
import { Loader2 } from "lucide-react"

import { consumePasswordReset } from "@/lib/admin-users-api"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"

export default function RestablecerContrasenaPage() {
  const [token, setToken] = useState("")
  const [password, setPassword] = useState("")
  const [confirm, setConfirm] = useState("")
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [doneEmail, setDoneEmail] = useState<string | null>(null)

  async function onSubmit(e: React.FormEvent) {
    e.preventDefault()
    setError(null)
    if (password !== confirm) {
      setError("Las contraseñas no coinciden")
      return
    }
    if (password.length < 10) {
      setError("La contraseña debe tener al menos 10 caracteres")
      return
    }
    setBusy(true)
    try {
      const res = await consumePasswordReset(token.trim(), password)
      setDoneEmail(res.email)
    } catch (err) {
      setError(err instanceof Error ? err.message : "No se pudo restablecer")
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="flex min-h-dvh flex-col items-center justify-center bg-background px-4 py-8">
      <Card className="w-full max-w-md">
        <CardHeader className="text-center">
          <CardTitle className="text-2xl font-bold">Restablecer contraseña</CardTitle>
          <CardDescription>
            Use el token de un solo uso entregado por administración. No comparta este valor.
          </CardDescription>
        </CardHeader>
        <CardContent>
          {doneEmail ? (
            <div className="space-y-4 text-center text-sm">
              <p className="text-lq-ink">
                Contraseña actualizada para <strong>{doneEmail}</strong>.
              </p>
              <Button asChild className="w-full">
                <Link href="/login">Ir a iniciar sesión</Link>
              </Button>
            </div>
          ) : (
            <form onSubmit={onSubmit} className="space-y-4">
              {error ? (
                <div className="rounded-lg bg-destructive/10 p-3 text-sm text-destructive">{error}</div>
              ) : null}
              <div className="space-y-2">
                <Label htmlFor="token">Token</Label>
                <Input
                  id="token"
                  value={token}
                  onChange={(e) => setToken(e.target.value)}
                  required
                  disabled={busy}
                  autoComplete="off"
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="np">Nueva contraseña</Label>
                <Input
                  id="np"
                  type="password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  required
                  disabled={busy}
                  autoComplete="new-password"
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="cp">Confirmar</Label>
                <Input
                  id="cp"
                  type="password"
                  value={confirm}
                  onChange={(e) => setConfirm(e.target.value)}
                  required
                  disabled={busy}
                  autoComplete="new-password"
                />
              </div>
              <Button type="submit" className="w-full" disabled={busy}>
                {busy ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
                Guardar contraseña
              </Button>
            </form>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
