"use client"

import { useEffect, useMemo, useState } from "react"
import { Loader2 } from "lucide-react"

import {
  createAdminUser,
  resetAdminUserPassword,
  updateAdminUser,
  type AdminCompanyRef,
  type AdminDeliveryPayload,
  type AdminUserRow,
} from "@/lib/admin-users-api"
import { formatChileDateTime, roleLabel } from "@/lib/admin-users-format"
import { SectionCard, StatusBadge } from "@/components/analytics-v2/shared/analytics-ui"
import { Button } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle } from "@/components/ui/sheet"

type Mode = "view" | "create"

export function UserAdminSheet({
  open,
  onOpenChange,
  mode,
  user,
  canMutate,
  canAssignAdm = false,
  assignableRoles,
  companiesCatalog,
  onSaved,
}: {
  open: boolean
  onOpenChange: (open: boolean) => void
  mode: Mode
  user: AdminUserRow | null
  canMutate: boolean
  /** Solo si /auth/me indica user_admin_assign_adm. */
  canAssignAdm?: boolean
  assignableRoles: string[]
  companiesCatalog: AdminCompanyRef[]
  onSaved: () => void
}) {
  const [nombre, setNombre] = useState("")
  const [email, setEmail] = useState("")
  const [role, setRole] = useState("operativo")
  const [active, setActive] = useState(true)
  const [companyIds, setCompanyIds] = useState<number[]>([])
  const [invite, setInvite] = useState(true)
  const [password, setPassword] = useState("")
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [delivery, setDelivery] = useState<AdminDeliveryPayload | null>(null)

  useEffect(() => {
    if (!open) return
    setError(null)
    setDelivery(null)
    setPassword("")
    if (mode === "create") {
      setNombre("")
      setEmail("")
      setRole(assignableRoles.includes("operativo") ? "operativo" : assignableRoles[0] || "operativo")
      setActive(true)
      setCompanyIds([])
      setInvite(true)
      return
    }
    if (user) {
      setNombre(user.nombre_usuario || "")
      setEmail(user.email)
      setRole(user.role)
      setActive(user.active)
      setCompanyIds(user.companies.map((c) => c.company_id))
    }
  }, [open, mode, user, assignableRoles])

  const title = mode === "create" ? "Nuevo usuario" : user?.nombre_usuario || user?.email || "Usuario"

  const companiesLabel = useMemo(() => {
    if (!companyIds.length) return "Sin empresas asignadas (alcance sin restringir hasta cutover)"
    return companyIds
      .map((id) => companiesCatalog.find((c) => c.company_id === id)?.name || `#${id}`)
      .join(", ")
  }, [companyIds, companiesCatalog])

  function toggleCompany(id: number) {
    setCompanyIds((prev) => (prev.includes(id) ? prev.filter((x) => x !== id) : [...prev, id]))
  }

  async function handleSave() {
    if (!canMutate) return
    if (role === "adm" && !canAssignAdm) {
      setError("No está autorizado a asignar el rol de administración")
      return
    }
    setBusy(true)
    setError(null)
    try {
      if (mode === "create") {
        const created = await createAdminUser({
          email,
          nombre_usuario: nombre,
          role,
          company_ids: companyIds,
          invite,
          password: invite ? null : password,
        })
        if (created.delivery) setDelivery(created.delivery)
        onSaved()
      } else if (user) {
        await updateAdminUser(user.id, {
          email,
          nombre_usuario: nombre,
          role,
          active,
          company_ids: companyIds,
        })
        onSaved()
        onOpenChange(false)
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : "No se pudo guardar")
    } finally {
      setBusy(false)
    }
  }

  async function handleReset() {
    if (!canMutate || !user) return
    setBusy(true)
    setError(null)
    try {
      const res = await resetAdminUserPassword(user.id)
      setDelivery(res.delivery)
      onSaved()
    } catch (e) {
      setError(e instanceof Error ? e.message : "No se pudo emitir el restablecimiento")
    } finally {
      setBusy(false)
    }
  }

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent side="right" className="w-full overflow-y-auto border-l-lq-line bg-lq-canvas p-0 sm:max-w-[min(560px,94vw)]">
        <SheetHeader className="sticky top-0 z-20 border-b border-lq-line bg-lq-surface/95 px-5 py-4 backdrop-blur md:px-6">
          <SheetTitle className="pr-8 text-xl tracking-[-0.015em]">{title}</SheetTitle>
          <SheetDescription>
            {mode === "create"
              ? "Alta de cuenta staff ERP. La contraseña actual nunca se muestra."
              : `Rol actual: ${roleLabel(user?.role)} · Último ingreso: ${formatChileDateTime(user?.last_login_at)}`}
          </SheetDescription>
        </SheetHeader>

        <div className="space-y-4 px-5 py-5 md:px-6">
          {error ? (
            <div className="rounded-lg border border-destructive/30 bg-destructive/10 px-3 py-2 text-sm text-destructive">
              {error}
            </div>
          ) : null}

          {delivery ? (
            <SectionCard
              title="Entrega segura (una sola vez)"
              description={delivery.delivery_hint}
            >
              <p className="text-xs text-lq-muted">Caduca: {formatChileDateTime(delivery.expires_at)}</p>
              <p className="mt-2 break-all rounded-md border border-lq-line bg-lq-canvas px-3 py-2 font-mono text-xs text-lq-ink">
                {delivery.one_time_token}
              </p>
              <p className="mt-2 text-xs text-lq-faint">
                Enlace sugerido: /restablecer-contrasena (el titular pega el token allí). No registre este valor en tickets ni logs.
              </p>
              <Button
                type="button"
                variant="outline"
                size="sm"
                className="mt-3"
                onClick={() => navigator.clipboard?.writeText(delivery.one_time_token)}
              >
                Copiar token
              </Button>
            </SectionCard>
          ) : null}

          <SectionCard title="Datos" description="Nombre y correo de acceso al ERP.">
            <div className="space-y-3">
              <div className="space-y-1.5">
                <Label htmlFor="ua-nombre">Nombre</Label>
                <Input
                  id="ua-nombre"
                  value={nombre}
                  onChange={(e) => setNombre(e.target.value)}
                  disabled={!canMutate || busy}
                />
              </div>
              <div className="space-y-1.5">
                <Label htmlFor="ua-email">Correo</Label>
                <Input
                  id="ua-email"
                  type="email"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  disabled={!canMutate || busy}
                />
              </div>
              <div className="space-y-1.5">
                <Label>Rol</Label>
                <Select value={role} onValueChange={setRole} disabled={!canMutate || busy}>
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {assignableRoles.map((r) => (
                      <SelectItem key={r} value={r}>
                        {roleLabel(r)}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              {mode === "view" ? (
                <div className="flex items-center gap-2 pt-1">
                  <Checkbox
                    id="ua-active"
                    checked={active}
                    onCheckedChange={(v) => setActive(v === true)}
                    disabled={!canMutate || busy}
                  />
                  <Label htmlFor="ua-active">Cuenta activa</Label>
                  <StatusBadge tone={active ? "success" : "neutral"}>{active ? "Activa" : "Inactiva"}</StatusBadge>
                </div>
              ) : null}
            </div>
          </SectionCard>

          <SectionCard title="Empresas autorizadas" description={companiesLabel}>
            <div className="max-h-48 space-y-2 overflow-y-auto">
              {companiesCatalog.length === 0 ? (
                <p className="text-sm text-lq-muted">No hay empresas en catálogo.</p>
              ) : (
                companiesCatalog.map((c) => (
                  <label key={c.company_id} className="flex items-center gap-2 text-sm text-lq-ink">
                    <Checkbox
                      checked={companyIds.includes(c.company_id)}
                      onCheckedChange={() => toggleCompany(c.company_id)}
                      disabled={!canMutate || busy}
                    />
                    <span>
                      {c.name || `Empresa ${c.company_id}`}{" "}
                      <span className="text-lq-faint">#{c.company_id}</span>
                    </span>
                  </label>
                ))
              )}
            </div>
            <p className="mt-2 text-xs text-lq-faint">
              Requiere migración 061. AUTH_REQUIRE_USER_COMPANIES permanece desactivado hasta validar todas las cuentas.
            </p>
          </SectionCard>

          {mode === "create" ? (
            <SectionCard title="Credenciales" description="Nunca se muestra una contraseña existente.">
              <div className="flex items-center gap-2">
                <Checkbox
                  id="ua-invite"
                  checked={invite}
                  onCheckedChange={(v) => setInvite(v === true)}
                  disabled={!canMutate || busy}
                />
                <Label htmlFor="ua-invite">Invitación segura (token de un solo uso)</Label>
              </div>
              {!invite ? (
                <div className="mt-3 space-y-1.5">
                  <Label htmlFor="ua-pass">Contraseña inicial (mín. 10)</Label>
                  <Input
                    id="ua-pass"
                    type="password"
                    autoComplete="new-password"
                    value={password}
                    onChange={(e) => setPassword(e.target.value)}
                    disabled={!canMutate || busy}
                  />
                </div>
              ) : null}
            </SectionCard>
          ) : null}

          {canMutate ? (
            <div className="flex flex-wrap gap-2 border-t border-lq-line pt-4">
              <Button type="button" onClick={handleSave} disabled={busy}>
                {busy ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
                {mode === "create" ? "Crear usuario" : "Guardar cambios"}
              </Button>
              {mode === "view" && user ? (
                <Button type="button" variant="outline" onClick={handleReset} disabled={busy || !user.active}>
                  Restablecer contraseña
                </Button>
              ) : null}
            </div>
          ) : (
            <p className="text-sm text-lq-muted">Solo lectura: las mutaciones requieren rol de administración.</p>
          )}
        </div>
      </SheetContent>
    </Sheet>
  )
}
