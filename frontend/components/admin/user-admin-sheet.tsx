"use client"

import { useEffect, useMemo, useState } from "react"
import { Loader2 } from "lucide-react"

import {
  createAdminUser,
  fetchAdminOfficesCatalog,
  resetAdminUserPassword,
  updateAdminUser,
  type AdminAppRef,
  type AdminCompanyRef,
  type AdminDeliveryPayload,
  type AdminOfficeRef,
  type AdminProfileRef,
  type AdminUserRow,
} from "@/lib/admin-users-api"
import { OwnerBadge } from "@/components/admin/owner-badge"
import { PasswordDeliveryDialog } from "@/components/admin/password-delivery-dialog"
import { formatChileDateTime, roleLabel, staffRoleDisplay } from "@/lib/admin-users-format"
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

function officeKey(companyId: number, officeId: number) {
  return `${companyId}:${officeId}`
}

export function UserAdminSheet({
  open,
  onOpenChange,
  mode,
  user,
  canMutate,
  canAssignAdm = false,
  assignableRoles,
  companiesCatalog,
  appsCatalog,
  profilesCatalog,
  multiappReady = false,
  targetIsOwner = false,
  onSaved,
}: {
  open: boolean
  onOpenChange: (open: boolean) => void
  mode: Mode
  user: AdminUserRow | null
  canMutate: boolean
  canAssignAdm?: boolean
  assignableRoles: string[]
  companiesCatalog: AdminCompanyRef[]
  appsCatalog: AdminAppRef[]
  profilesCatalog: AdminProfileRef[]
  multiappReady?: boolean
  targetIsOwner?: boolean
  onSaved: () => void
}) {
  const lockedOwner = mode === "view" && (targetIsOwner || Boolean(user?.is_owner))
  const allowMutate = canMutate && !lockedOwner
  const [nombre, setNombre] = useState("")
  const [email, setEmail] = useState("")
  const [role, setRole] = useState("operativo")
  const [active, setActive] = useState(true)
  const [companyIds, setCompanyIds] = useState<number[]>([])
  const [apps, setApps] = useState<string[]>(["erp"])
  const [profiles, setProfiles] = useState<string[]>([])
  const [officeKeys, setOfficeKeys] = useState<string[]>([])
  const [officesCatalog, setOfficesCatalog] = useState<AdminOfficeRef[]>([])
  const [officesLoading, setOfficesLoading] = useState(false)
  const [invite, setInvite] = useState(true)
  const [password, setPassword] = useState("")
  const [confirmOpen, setConfirmOpen] = useState(false)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [delivery, setDelivery] = useState<AdminDeliveryPayload | null>(null)
  const [deliveryOpen, setDeliveryOpen] = useState(false)
  const [deliveryEmail, setDeliveryEmail] = useState<string | null>(null)
  const [deliveryTitle, setDeliveryTitle] = useState("Enlace de acceso (una sola vez)")

  useEffect(() => {
    if (!open) return
    setError(null)
    setPassword("")
    setConfirmOpen(false)
    if (mode === "create") {
      setNombre("")
      setEmail("")
      setRole(assignableRoles.includes("operativo") ? "operativo" : assignableRoles[0] || "operativo")
      setActive(true)
      setCompanyIds([])
      setApps(["erp"])
      setProfiles([])
      setOfficeKeys([])
      setInvite(true)
      return
    }
    if (user) {
      setNombre(user.nombre_usuario || "")
      setEmail(user.email)
      setRole(user.role)
      setActive(user.active)
      setCompanyIds(user.companies.map((c) => c.company_id))
      if (user.apps && user.apps.length) {
        setApps([...user.apps])
      } else if (user.apps_legacy_erp || user.has_erp_access !== false) {
        setApps(["erp"])
      } else {
        setApps([])
      }
      setProfiles([...(user.profiles || [])])
      setOfficeKeys(
        (user.offices || []).map((o) => officeKey(o.company_id, o.office_id)),
      )
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps -- no resetear por catálogos
  }, [open, mode, user?.id])

  useEffect(() => {
    if (!open || !multiappReady || companyIds.length === 0) {
      setOfficesCatalog([])
      return
    }
    let cancelled = false
    setOfficesLoading(true)
    void fetchAdminOfficesCatalog({ companyIds })
      .then((res) => {
        if (!cancelled) setOfficesCatalog(res.items)
      })
      .catch(() => {
        if (!cancelled) setOfficesCatalog([])
      })
      .finally(() => {
        if (!cancelled) setOfficesLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [open, multiappReady, companyIds.join(",")])

  // Al quitar empresas, descartar oficinas de esas empresas.
  useEffect(() => {
    const allowed = new Set(companyIds)
    setOfficeKeys((prev) =>
      prev.filter((k) => {
        const companyId = Number(k.split(":")[0])
        return allowed.has(companyId)
      }),
    )
  }, [companyIds.join(",")])

  // Sin PWA: limpiar perfiles (no se envían huérfanos).
  useEffect(() => {
    if (!apps.includes("pwa") && profiles.length) {
      setProfiles([])
    }
  }, [apps.join(",")])

  const title = mode === "create" ? "Nuevo trabajador" : user?.nombre_usuario || user?.email || "Usuario"

  const companiesLabel = useMemo(() => {
    if (!companyIds.length) return "Sin empresas asignadas"
    return companyIds
      .map((id) => companiesCatalog.find((c) => c.company_id === id)?.name || `#${id}`)
      .join(", ")
  }, [companyIds, companiesCatalog])

  const effectiveSummary = useMemo(() => {
    const appLabels = apps.map((a) => appsCatalog.find((x) => x.code === a)?.label || a.toUpperCase())
    const profileLabels = profiles.map(
      (p) => profilesCatalog.find((x) => x.code === p)?.label || p,
    )
    const officeLabels = officeKeys.map((k) => {
      const [c, o] = k.split(":").map(Number)
      const hit = officesCatalog.find((x) => x.company_id === c && x.office_id === o)
      return hit?.name || `Emp ${c} / Of ${o}`
    })
    return {
      apps: appLabels.length ? appLabels.join(", ") : "Ninguna (legacy ERP si no hay filas)",
      profiles: profileLabels.length ? profileLabels.join(", ") : "—",
      companies: companiesLabel,
      offices: officeLabels.length ? officeLabels.join(", ") : "—",
      erp: apps.includes("erp") || (!apps.length && mode === "view" && Boolean(user?.apps_legacy_erp)),
      pwa: apps.includes("pwa"),
    }
  }, [
    apps,
    profiles,
    officeKeys,
    appsCatalog,
    profilesCatalog,
    officesCatalog,
    companiesLabel,
    mode,
    user?.apps_legacy_erp,
  ])

  function toggleCompany(id: number) {
    setCompanyIds((prev) => (prev.includes(id) ? prev.filter((x) => x !== id) : [...prev, id]))
  }

  function toggleApp(code: string) {
    setApps((prev) => (prev.includes(code) ? prev.filter((x) => x !== code) : [...prev, code]))
  }

  function toggleProfile(code: string) {
    setProfiles((prev) => (prev.includes(code) ? prev.filter((x) => x !== code) : [...prev, code]))
  }

  function toggleOffice(companyId: number, officeId: number) {
    const key = officeKey(companyId, officeId)
    setOfficeKeys((prev) => (prev.includes(key) ? prev.filter((x) => x !== key) : [...prev, key]))
  }

  function showDelivery(payload: AdminDeliveryPayload, forEmail: string, titleText: string) {
    setDelivery(payload)
    setDeliveryEmail(forEmail)
    setDeliveryTitle(titleText)
    setDeliveryOpen(true)
  }

  function validateBeforeSave(): string | null {
    if (role === "adm" && !canAssignAdm) {
      return "No está autorizado a asignar el rol de administración"
    }
    if (role === "gerencia" && !assignableRoles.includes("gerencia")) {
      return "Solo el propietario puede asignar el rol de gerencia"
    }
    if (multiappReady && profiles.length > 0 && !apps.includes("pwa")) {
      return "Los perfiles operativos requieren acceso PWA"
    }
    if (multiappReady && apps.length === 0) {
      return "Seleccione al menos ERP, PWA o ambas"
    }
    return null
  }

  async function persist() {
    if (!allowMutate) return
    const validation = validateBeforeSave()
    if (validation) {
      setError(validation)
      setConfirmOpen(false)
      return
    }
    setBusy(true)
    setError(null)
    const office_scopes = officeKeys.map((k) => {
      const [company_id, office_id] = k.split(":").map(Number)
      return { company_id, office_id }
    })
    try {
      if (mode === "create") {
        const body: Parameters<typeof createAdminUser>[0] = {
          email,
          nombre_usuario: nombre,
          role,
          company_ids: companyIds,
          invite,
          password: invite ? null : password,
        }
        if (multiappReady) {
          body.apps = apps
          body.profiles = apps.includes("pwa") ? profiles : []
          body.office_scopes = office_scopes
        }
        const created = await createAdminUser(body)
        onSaved()
        setConfirmOpen(false)
        if (created.delivery) {
          showDelivery(
            created.delivery,
            created.email || email,
            "Invitación segura (una sola vez)",
          )
        } else {
          onOpenChange(false)
        }
      } else if (user) {
        const body: Parameters<typeof updateAdminUser>[1] = {
          email,
          nombre_usuario: nombre,
          role,
          active,
          company_ids: companyIds,
        }
        if (multiappReady) {
          body.apps = apps
          body.profiles = apps.includes("pwa") ? profiles : []
          body.office_scopes = office_scopes
        }
        await updateAdminUser(user.id, body)
        onSaved()
        setConfirmOpen(false)
        onOpenChange(false)
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : "No se pudo guardar")
      setConfirmOpen(false)
    } finally {
      setBusy(false)
    }
  }

  async function handleReset() {
    if (!allowMutate || !user || lockedOwner) return
    setBusy(true)
    setError(null)
    try {
      const res = await resetAdminUserPassword(user.id)
      if (!res.delivery?.one_time_token) {
        setError("El servidor no devolvió un enlace de restablecimiento")
        return
      }
      showDelivery(res.delivery, res.email || user.email, "Restablecer contraseña (una sola vez)")
      onSaved()
    } catch (e) {
      setError(e instanceof Error ? e.message : "No se pudo emitir el restablecimiento")
    } finally {
      setBusy(false)
    }
  }

  return (
    <>
      <Sheet
        open={open}
        onOpenChange={(next) => {
          if (!next && deliveryOpen) return
          onOpenChange(next)
        }}
      >
        <SheetContent side="right" className="w-full overflow-y-auto border-l-lq-line bg-lq-canvas p-0 sm:max-w-[min(560px,94vw)]">
          <SheetHeader className="sticky top-0 z-20 border-b border-lq-line bg-lq-surface/95 px-5 py-4 backdrop-blur md:px-6">
            <SheetTitle className="flex flex-wrap items-center gap-2 pr-8 text-xl tracking-[-0.015em]">
              {title}
              {lockedOwner ? <OwnerBadge /> : null}
            </SheetTitle>
            <SheetDescription>
              {mode === "create"
                ? "Alta de trabajador ERP y/o PWA. La contraseña nunca se muestra."
                : lockedOwner
                  ? "Cuenta OWNER: identidad, rol y estado protegidos. Apps, perfiles y alcance se muestran en solo lectura."
                  : `Rol: ${staffRoleDisplay(user?.role, user?.is_owner)} · Último ingreso: ${formatChileDateTime(user?.last_login_at)}`}
            </SheetDescription>
          </SheetHeader>

          <div className="space-y-4 px-5 py-5 md:px-6">
            {error ? (
              <div
                role="alert"
                className="rounded-lg border border-destructive/30 bg-destructive/10 px-3 py-2 text-sm text-destructive"
              >
                {error}
              </div>
            ) : null}

            {!multiappReady ? (
              <div className="rounded-lg border border-lq-line bg-lq-canvas px-3 py-2 text-xs text-lq-muted">
                Migraciones multiapp 064–066 pendientes. Puede gestionar datos ERP habituales; apps,
                perfiles y oficinas se habilitarán tras aplicarlas.
              </div>
            ) : null}

            <SectionCard title="Datos del trabajador" description="Identidad y rol jerárquico del ERP.">
              <div className="space-y-3">
                <div className="space-y-1.5">
                  <Label htmlFor="ua-nombre">Nombre</Label>
                  <Input
                    id="ua-nombre"
                    value={nombre}
                    onChange={(e) => setNombre(e.target.value)}
                    disabled={!allowMutate || busy}
                  />
                </div>
                <div className="space-y-1.5">
                  <Label htmlFor="ua-email">Correo</Label>
                  <Input
                    id="ua-email"
                    type="email"
                    value={email}
                    onChange={(e) => setEmail(e.target.value)}
                    disabled={!allowMutate || busy}
                  />
                </div>
                <div className="space-y-1.5">
                  <Label>Rol administrativo</Label>
                  <Select value={role} onValueChange={setRole} disabled={!allowMutate || busy}>
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
                  <p className="text-xs text-lq-faint">
                    Independiente de los perfiles operativos (chofer, bodega, etc.).
                  </p>
                </div>
                {mode === "view" ? (
                  <div className="flex items-center gap-2 pt-1">
                    <Checkbox
                      id="ua-active"
                      checked={active}
                      onCheckedChange={(v) => setActive(v === true)}
                      disabled={!allowMutate || busy}
                    />
                    <Label htmlFor="ua-active">Cuenta activa</Label>
                    <StatusBadge tone={active ? "success" : "neutral"}>
                      {active ? "Activa" : "Inactiva"}
                    </StatusBadge>
                  </div>
                ) : null}
              </div>
            </SectionCard>

            {multiappReady ? (
              <SectionCard
                title="Aplicaciones"
                description="ERP, PWA operativa o ambas. PWA-only no obtiene ERP automáticamente."
              >
                <div className="space-y-2">
                  {(appsCatalog.length
                    ? appsCatalog
                    : [
                        { code: "erp", label: "ERP" },
                        { code: "pwa", label: "PWA operativa" },
                      ]
                  ).map((a) => (
                    <label key={a.code} className="flex items-center gap-2 text-sm text-lq-ink">
                      <Checkbox
                        checked={apps.includes(a.code)}
                        onCheckedChange={() => toggleApp(a.code)}
                        disabled={!allowMutate || busy}
                      />
                      <span>{a.label}</span>
                    </label>
                  ))}
                </div>
              </SectionCard>
            ) : null}

            {multiappReady && apps.includes("pwa") ? (
              <SectionCard
                title="Perfiles operativos"
                description="Solo aplican a la PWA. No convierten al usuario en administrador ERP."
              >
                <div className="grid gap-2 sm:grid-cols-2">
                  {profilesCatalog.map((p) => (
                    <label key={p.code} className="flex items-center gap-2 text-sm text-lq-ink">
                      <Checkbox
                        checked={profiles.includes(p.code)}
                        onCheckedChange={() => toggleProfile(p.code)}
                        disabled={!allowMutate || busy}
                      />
                      <span>{p.label}</span>
                    </label>
                  ))}
                </div>
              </SectionCard>
            ) : null}

            <SectionCard title="Empresas autorizadas" description={companiesLabel}>
              <div className="max-h-40 space-y-2 overflow-y-auto">
                {companiesCatalog.length === 0 ? (
                  <p className="text-sm text-lq-muted">No hay empresas en catálogo.</p>
                ) : (
                  companiesCatalog.map((c) => (
                    <label key={c.company_id} className="flex items-center gap-2 text-sm text-lq-ink">
                      <Checkbox
                        checked={companyIds.includes(c.company_id)}
                        onCheckedChange={() => toggleCompany(c.company_id)}
                        disabled={!allowMutate || busy}
                      />
                      <span>
                        {c.name || `Empresa ${c.company_id}`}{" "}
                        <span className="text-lq-faint">#{c.company_id}</span>
                      </span>
                    </label>
                  ))
                )}
              </div>
            </SectionCard>

            {multiappReady ? (
              <SectionCard
                title="Oficinas / sucursales"
                description="Solo oficinas de las empresas marcadas. Clave company_id + office_id."
              >
                {!companyIds.length ? (
                  <p className="text-sm text-lq-muted">Seleccione empresas para listar oficinas.</p>
                ) : officesLoading ? (
                  <p className="flex items-center gap-2 text-sm text-lq-muted">
                    <Loader2 className="h-3.5 w-3.5 animate-spin" /> Cargando oficinas…
                  </p>
                ) : officesCatalog.length === 0 ? (
                  <p className="text-sm text-lq-muted">Sin oficinas activas para las empresas elegidas.</p>
                ) : (
                  <div className="max-h-48 space-y-2 overflow-y-auto">
                    {officesCatalog.map((o) => {
                      const key = officeKey(o.company_id, o.office_id)
                      const companyName =
                        companiesCatalog.find((c) => c.company_id === o.company_id)?.name ||
                        `#${o.company_id}`
                      return (
                        <label key={key} className="flex items-center gap-2 text-sm text-lq-ink">
                          <Checkbox
                            checked={officeKeys.includes(key)}
                            onCheckedChange={() => toggleOffice(o.company_id, o.office_id)}
                            disabled={!allowMutate || busy}
                          />
                          <span>
                            {o.name || `Oficina ${o.office_id}`}{" "}
                            <span className="text-lq-faint">
                              · {companyName} · #{o.office_id}
                            </span>
                          </span>
                        </label>
                      )
                    })}
                  </div>
                )}
              </SectionCard>
            ) : null}

            <SectionCard
              title="Accesos efectivos (vista)"
              description="Resumen según el formulario. La autorización real la aplica el backend."
            >
              <ul className="space-y-1 text-sm text-lq-body">
                <li>
                  <span className="text-lq-faint">Apps:</span> {effectiveSummary.apps}
                </li>
                <li>
                  <span className="text-lq-faint">Perfiles:</span> {effectiveSummary.profiles}
                </li>
                <li>
                  <span className="text-lq-faint">Empresas:</span> {effectiveSummary.companies}
                </li>
                <li>
                  <span className="text-lq-faint">Oficinas:</span> {effectiveSummary.offices}
                </li>
              </ul>
            </SectionCard>

            {mode === "create" ? (
              <SectionCard title="Credenciales" description="Nunca se muestra una contraseña existente.">
                <div className="flex items-center gap-2">
                  <Checkbox
                    id="ua-invite"
                    checked={invite}
                    onCheckedChange={(v) => setInvite(v === true)}
                    disabled={!allowMutate || busy}
                  />
                  <Label htmlFor="ua-invite">Invitación segura (enlace de un solo uso)</Label>
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
                      disabled={!allowMutate || busy}
                    />
                  </div>
                ) : null}
              </SectionCard>
            ) : null}

            {confirmOpen ? (
              <SectionCard title="Confirmar guardado" description="Revise el resumen antes de aplicar.">
                <ul className="mb-3 space-y-1 text-sm">
                  <li>
                    <strong>{nombre || email}</strong> · {roleLabel(role)}
                  </li>
                  <li>Apps: {effectiveSummary.apps}</li>
                  <li>Perfiles: {effectiveSummary.profiles}</li>
                  <li>Empresas: {effectiveSummary.companies}</li>
                  <li>Oficinas: {effectiveSummary.offices}</li>
                </ul>
                <div className="flex flex-wrap gap-2">
                  <Button type="button" onClick={() => void persist()} disabled={busy}>
                    {busy ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
                    Confirmar
                  </Button>
                  <Button
                    type="button"
                    variant="outline"
                    onClick={() => setConfirmOpen(false)}
                    disabled={busy}
                  >
                    Volver
                  </Button>
                </div>
              </SectionCard>
            ) : null}

            {allowMutate && !confirmOpen ? (
              <div className="flex flex-wrap gap-2 border-t border-lq-line pt-4">
                <Button
                  type="button"
                  onClick={() => {
                    const validation = validateBeforeSave()
                    if (validation) {
                      setError(validation)
                      return
                    }
                    setError(null)
                    setConfirmOpen(true)
                  }}
                  disabled={busy}
                >
                  {mode === "create" ? "Revisar y crear" : "Revisar y guardar"}
                </Button>
                {mode === "view" && user ? (
                  <Button
                    type="button"
                    variant="outline"
                    onClick={() => void handleReset()}
                    disabled={busy || !user.active}
                  >
                    Restablecer contraseña
                  </Button>
                ) : null}
              </div>
            ) : !allowMutate ? (
              <p className="text-sm text-lq-muted">
                {lockedOwner
                  ? "Cuenta OWNER protegida: use el procedimiento de recuperación documentado."
                  : "Solo lectura: las mutaciones requieren autorización explícita."}
              </p>
            ) : null}
          </div>
        </SheetContent>
      </Sheet>

      <PasswordDeliveryDialog
        open={deliveryOpen}
        onOpenChange={(next) => {
          setDeliveryOpen(next)
          if (!next) {
            setDelivery(null)
            setDeliveryEmail(null)
          }
        }}
        delivery={delivery}
        title={deliveryTitle}
        email={deliveryEmail}
      />
    </>
  )
}
