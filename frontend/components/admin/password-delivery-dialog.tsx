"use client"

import { useMemo, useState } from "react"
import { Check, Copy, Loader2 } from "lucide-react"

import type { AdminDeliveryPayload } from "@/lib/admin-users-api"
import { formatChileDateTime } from "@/lib/admin-users-format"
import { Button } from "@/components/ui/button"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"

export function buildPasswordResetUrl(delivery: AdminDeliveryPayload): string {
  if (typeof window === "undefined") {
    return delivery.one_time_path || `/restablecer-contrasena?token=${encodeURIComponent(delivery.one_time_token)}`
  }
  const path =
    delivery.one_time_path ||
    `/restablecer-contrasena?token=${encodeURIComponent(delivery.one_time_token)}`
  return `${window.location.origin}${path.startsWith("/") ? path : `/${path}`}`
}

/**
 * Diálogo modal persistente para entregar el enlace de invitación/reset.
 * No se cierra por recargas del sheet ni por fallos de red del listado.
 */
export function PasswordDeliveryDialog({
  open,
  onOpenChange,
  delivery,
  title = "Enlace de acceso (una sola vez)",
  email,
}: {
  open: boolean
  onOpenChange: (open: boolean) => void
  delivery: AdminDeliveryPayload | null
  title?: string
  email?: string | null
}) {
  const [copied, setCopied] = useState<"url" | "token" | null>(null)
  const url = useMemo(() => (delivery ? buildPasswordResetUrl(delivery) : ""), [delivery])

  async function copy(kind: "url" | "token", value: string) {
    try {
      await navigator.clipboard.writeText(value)
      setCopied(kind)
      window.setTimeout(() => setCopied(null), 2000)
    } catch {
      setCopied(null)
    }
  }

  return (
    <Dialog open={open && Boolean(delivery)} onOpenChange={onOpenChange}>
      <DialogContent
        className="border-lq-line bg-lq-surface sm:max-w-lg"
        onPointerDownOutside={(e) => e.preventDefault()}
        onEscapeKeyDown={(e) => e.preventDefault()}
      >
        <DialogHeader>
          <DialogTitle className="tracking-[-0.015em]">{title}</DialogTitle>
          <DialogDescription>
            {email
              ? `Entregue este enlace solo a ${email} por un canal seguro. No se volverá a mostrar.`
              : "Entregue este enlace solo al titular por un canal seguro. No se volverá a mostrar."}
          </DialogDescription>
        </DialogHeader>

        {delivery ? (
          <div className="space-y-4">
            <div className="rounded-lg border border-lq-line bg-lq-canvas px-3 py-2 text-xs text-lq-muted">
              Caduca: <span className="font-medium text-lq-ink">{formatChileDateTime(delivery.expires_at)}</span>
              {" · "}
              Propósito: {delivery.purpose === "invite" ? "Invitación" : "Restablecimiento"}
            </div>

            <div className="space-y-1.5">
              <Label htmlFor="pwd-delivery-url">Enlace de un solo uso</Label>
              <div className="flex gap-2">
                <Input
                  id="pwd-delivery-url"
                  readOnly
                  value={url}
                  className="font-mono text-xs"
                  onFocus={(e) => e.target.select()}
                />
                <Button
                  type="button"
                  variant="outline"
                  size="icon"
                  className="shrink-0"
                  onClick={() => void copy("url", url)}
                  aria-label="Copiar enlace"
                >
                  {copied === "url" ? <Check className="h-4 w-4" /> : <Copy className="h-4 w-4" />}
                </Button>
              </div>
            </div>

            <details className="rounded-lg border border-lq-line-soft px-3 py-2 text-xs text-lq-faint">
              <summary className="cursor-pointer text-lq-muted">Token (solo si el enlace no basta)</summary>
              <div className="mt-2 flex gap-2">
                <Input readOnly value={delivery.one_time_token} className="font-mono text-xs" />
                <Button
                  type="button"
                  variant="outline"
                  size="icon"
                  className="shrink-0"
                  onClick={() => void copy("token", delivery.one_time_token)}
                  aria-label="Copiar token"
                >
                  {copied === "token" ? <Check className="h-4 w-4" /> : <Copy className="h-4 w-4" />}
                </Button>
              </div>
            </details>

            <p className="text-xs text-lq-faint">
              El titular abre el enlace, define su contraseña y puede iniciar sesión. No registre el token en tickets ni logs.
            </p>
          </div>
        ) : (
          <div className="flex items-center gap-2 text-sm text-lq-muted">
            <Loader2 className="h-4 w-4 animate-spin" />
            Preparando entrega…
          </div>
        )}

        <DialogFooter>
          <Button type="button" onClick={() => onOpenChange(false)}>
            Ya copié el enlace
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
