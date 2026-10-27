"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import { Loader2, Plus, RefreshCw, Search } from "lucide-react"

import { OwnerBadge } from "@/components/admin/owner-badge"
import { UserAdminSheet } from "@/components/admin/user-admin-sheet"
import {
  AnalyticPageHeader,
  AnalyticPageShell,
  EmptyState,
  ProfessionalDataTable,
  SectionCard,
  StatusBadge,
} from "@/components/analytics-v2/shared/analytics-ui"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import {
  fetchAdminAuthEvents,
  fetchAdminCompaniesCatalog,
  fetchAdminRolesCatalog,
  fetchAdminUsers,
  type AdminAuthEvent,
  type AdminCompanyRef,
  type AdminUserRow,
  type RolesCatalog,
} from "@/lib/admin-users-api"
import { formatChileDateTime, staffRoleDisplay } from "@/lib/admin-users-format"
import {
  canAssignAdmRole,
  canMutateUserAdmin,
  canViewUserAdmin,
  isOwnerUser,
} from "@/lib/permissions"
import { useAuthMe } from "@/lib/use-auth-me"

export default function UsuariosAccesosPage() {
  const { meState, staff, reload: loadMe } = useAuthMe()
  const [tab, setTab] = useState("usuarios")
  const [q, setQ] = useState("")
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [users, setUsers] = useState<AdminUserRow[]>([])
  const [rolesCatalog, setRolesCatalog] = useState<RolesCatalog | null>(null)
  const [companies, setCompanies] = useState<AdminCompanyRef[]>([])
  const [events, setEvents] = useState<AdminAuthEvent[]>([])
  const [eventsMeta, setEventsMeta] = useState<{ available: boolean; message?: string }>({
    available: false,
  })
  const [eventsTotal, setEventsTotal] = useState(0)
  const [sheetOpen, setSheetOpen] = useState(false)
  const [sheetMode, setSheetMode] = useState<"view" | "create">("view")
  const [selected, setSelected] = useState<AdminUserRow | null>(null)

  const canView = meState === "ok" && canViewUserAdmin(staff)
  // Acciones sensibles: solo con /auth/me OK y flags explícitos (nunca durante carga/fallo).
  const canMutate = meState === "ok" && canMutateUserAdmin(staff)
  const canAssignAdm = meState === "ok" && canAssignAdmRole(staff)
  const actorIsOwner = meState === "ok" && isOwnerUser(staff)

  const assignableRoles = useMemo(() => {
    let base = rolesCatalog?.assignable_roles || ["operativo", "gerencia", "adm"]
    base = base.filter((r) => r !== "owner")
    if (!actorIsOwner) base = base.filter((r) => r !== "gerencia")
    if (!canAssignAdm) base = base.filter((r) => r !== "adm")
    return base
  }, [rolesCatalog, canAssignAdm, actorIsOwner])

  const loadUsers = useCallback(async () => {
    if (!canView) return
    setLoading(true)
    setError(null)
    try {
      const [u, r, c] = await Promise.all([
        fetchAdminUsers({ q: q || undefined }),
        fetchAdminRolesCatalog(),
        fetchAdminCompaniesCatalog(),
      ])
      setUsers(u.items)
      setRolesCatalog(r)
      setCompanies(c.items)
    } catch (e) {
      setError(e instanceof Error ? e.message : "No se pudo cargar usuarios")
    } finally {
      setLoading(false)
    }
  }, [canView, q])

  const loadEvents = useCallback(async () => {
    if (!canView) return
    setLoading(true)
    setError(null)
    try {
      const res = await fetchAdminAuthEvents({ limit: 100 })
      setEvents(res.items)
      setEventsTotal(res.total)
      setEventsMeta(res.meta)
    } catch (e) {
      setError(e instanceof Error ? e.message : "No se pudo cargar el historial")
    } finally {
      setLoading(false)
    }
  }, [canView])

  useEffect(() => {
    if (!canView) return
    if (tab === "usuarios" || tab === "permisos") void loadUsers()
    if (tab === "historial") void loadEvents()
  }, [canView, tab, loadUsers, loadEvents])

  if (meState === "loading") {
    return (
      <div className="flex min-h-[40vh] flex-col items-center justify-center gap-2 text-lq-muted">
        <Loader2 className="h-5 w-5 animate-spin" />
        <p className="text-sm">Verificando permisos…</p>
      </div>
    )
  }

  if (meState === "unauthorized") {
    return (
      <AnalyticPageShell>
        <AnalyticPageHeader
          eyebrow="Administración"
          title="Usuarios y accesos"
          description="Sesión no válida o expirada. Vuelva a iniciar sesión."
        />
      </AnalyticPageShell>
    )
  }

  if (meState === "unavailable" || !staff) {
    return (
      <AnalyticPageShell>
        <AnalyticPageHeader
          eyebrow="Administración"
          title="Usuarios y accesos"
          description="No se pudieron obtener los permisos del servidor. Las acciones administrativas permanecen ocultas."
          actions={
            <Button type="button" variant="outline" size="sm" onClick={() => void loadMe()}>
              Reintentar
            </Button>
          }
        />
      </AnalyticPageShell>
    )
  }

  if (!canView) {
    return (
      <AnalyticPageShell>
        <AnalyticPageHeader
          eyebrow="Administración"
          title="Usuarios y accesos"
          description="No tiene permiso para consultar este módulo (user_admin_view)."
        />
      </AnalyticPageShell>
    )
  }

  return (
    <AnalyticPageShell>
      <AnalyticPageHeader
        eyebrow="Administración"
        title="Usuarios y accesos"
        description="Gestione cuentas staff del ERP, roles existentes y el historial de accesos. Las contraseñas nunca se muestran."
        actions={
          <div className="flex gap-2">
            <Button
              type="button"
              variant="outline"
              size="sm"
              onClick={() => (tab === "historial" ? loadEvents() : loadUsers())}
              disabled={loading}
            >
              <RefreshCw className={`mr-1.5 h-3.5 w-3.5 ${loading ? "animate-spin" : ""}`} />
              Actualizar
            </Button>
            {canMutate && tab === "usuarios" ? (
              <Button
                type="button"
                size="sm"
                onClick={() => {
                  setSelected(null)
                  setSheetMode("create")
                  setSheetOpen(true)
                }}
              >
                <Plus className="mr-1.5 h-3.5 w-3.5" />
                Nuevo usuario
              </Button>
            ) : null}
          </div>
        }
      />

      {error ? (
        <div className="rounded-lg border border-destructive/30 bg-destructive/10 px-3 py-2 text-sm text-destructive">
          {error}
        </div>
      ) : null}

      <Tabs value={tab} onValueChange={setTab}>
        <TabsList>
          <TabsTrigger value="usuarios">Usuarios</TabsTrigger>
          <TabsTrigger value="permisos">Permisos</TabsTrigger>
          <TabsTrigger value="historial">Historial de accesos</TabsTrigger>
        </TabsList>

        <TabsContent value="usuarios" className="mt-4 space-y-4">
          <div className="relative max-w-md">
            <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-lq-faint" />
            <Input
              className="pl-9"
              placeholder="Buscar por nombre o correo"
              value={q}
              onChange={(e) => setQ(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter") void loadUsers()
              }}
            />
          </div>

          <ProfessionalDataTable loading={loading}>
            <table className="w-full min-w-[860px] text-left text-sm">
              <thead className="border-b border-lq-line bg-lq-canvas/80 text-[11px] uppercase tracking-[0.08em] text-lq-faint">
                <tr>
                  <th className="px-4 py-3 font-medium">Nombre</th>
                  <th className="px-4 py-3 font-medium">Correo</th>
                  <th className="px-4 py-3 font-medium">Rol</th>
                  <th className="px-4 py-3 font-medium">Estado</th>
                  <th className="px-4 py-3 font-medium">Empresas</th>
                  <th className="px-4 py-3 font-medium">Último ingreso</th>
                </tr>
              </thead>
              <tbody>
                {users.length === 0 && !loading ? (
                  <tr>
                    <td colSpan={6} className="p-0">
                      <EmptyState
                        title="Sin usuarios"
                        description="No hay resultados para la búsqueda actual."
                      />
                    </td>
                  </tr>
                ) : (
                  users.map((u) => (
                    <tr
                      key={u.id}
                      className="cursor-pointer border-b border-lq-line-soft transition-colors hover:bg-lq-canvas/70"
                      onClick={() => {
                        setSelected(u)
                        setSheetMode("view")
                        setSheetOpen(true)
                      }}
                    >
                      <td className="px-4 py-3 font-medium text-lq-ink">
                        <span className="inline-flex items-center gap-2">
                          {u.nombre_usuario || "—"}
                          {u.is_owner ? <OwnerBadge compact /> : null}
                        </span>
                      </td>
                      <td className="px-4 py-3 text-lq-body">{u.email}</td>
                      <td className="px-4 py-3">{staffRoleDisplay(u.role, u.is_owner)}</td>
                      <td className="px-4 py-3">
                        <StatusBadge tone={u.active ? "success" : "neutral"}>
                          {u.active ? "Activa" : "Inactiva"}
                        </StatusBadge>
                      </td>
                      <td className="px-4 py-3 text-lq-muted">
                        {u.companies.length
                          ? u.companies.map((c) => c.name || c.company_id).join(", ")
                          : "Sin restringir"}
                      </td>
                      <td className="px-4 py-3 tabular-nums text-lq-muted">
                        {formatChileDateTime(u.last_login_at)}
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </ProfessionalDataTable>
        </TabsContent>

        <TabsContent value="permisos" className="mt-4 space-y-4">
          <SectionCard
            title="Roles del ERP"
            description="Se reutilizan los roles existentes. No se crea un segundo sistema de permisos."
          >
            <div className="overflow-x-auto">
              <table className="w-full min-w-[640px] text-left text-sm">
                <thead className="border-b border-lq-line text-[11px] uppercase tracking-[0.08em] text-lq-faint">
                  <tr>
                    <th className="px-2 py-2">Rol</th>
                    <th className="px-2 py-2">Usuarios</th>
                    <th className="px-2 py-2">Costos</th>
                    <th className="px-2 py-2">Márgenes</th>
                    <th className="px-2 py-2">Diagnóstico</th>
                    <th className="px-2 py-2">Góndolas</th>
                  </tr>
                </thead>
                <tbody>
                  {(rolesCatalog?.module_matrix || []).map((row) => (
                    <tr key={row.role} className="border-b border-lq-line-soft">
                      <td className="px-2 py-2.5 font-medium">{row.label}</td>
                      <td className="px-2 py-2.5">
                        {row.modules.user_admin
                          ? "Administrar"
                          : row.modules.user_admin_view
                            ? "Solo lectura"
                            : "—"}
                      </td>
                      <td className="px-2 py-2.5">{row.modules.costs ? "Sí" : "—"}</td>
                      <td className="px-2 py-2.5">{row.modules.margins ? "Sí" : "—"}</td>
                      <td className="px-2 py-2.5">{row.modules.diagnostics ? "Sí" : "—"}</td>
                      <td className="px-2 py-2.5">
                        {row.modules.gondolas_operate
                          ? row.modules.gondolas_cancel
                            ? "Operar y cancelar"
                            : "Operar"
                          : "—"}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <ul className="mt-3 list-disc space-y-1 pl-5 text-xs text-lq-muted">
              {(rolesCatalog?.notes || []).map((n) => (
                <li key={n}>{n}</li>
              ))}
            </ul>
          </SectionCard>
        </TabsContent>

        <TabsContent value="historial" className="mt-4 space-y-4">
          {!eventsMeta.available ? (
            <SectionCard
              title="Historial de accesos"
              description="Aún no hay eventos persistidos. No se inventa historial anterior."
            >
              <p className="text-sm text-lq-muted">
                {eventsMeta.message ||
                  "Active AUTH_AUDIT_EVENTS=1 en un despliegue controlado para registrar eventos futuros."}
              </p>
            </SectionCard>
          ) : (
            <>
              <p className="text-xs text-lq-faint">{eventsTotal} evento(s) · hora Chile</p>
              <ProfessionalDataTable loading={loading}>
                <table className="w-full min-w-[900px] text-left text-sm">
                  <thead className="border-b border-lq-line bg-lq-canvas/80 text-[11px] uppercase tracking-[0.08em] text-lq-faint">
                    <tr>
                      <th className="px-4 py-3 font-medium">Fecha</th>
                      <th className="px-4 py-3 font-medium">Usuario</th>
                      <th className="px-4 py-3 font-medium">Evento</th>
                      <th className="px-4 py-3 font-medium">Resultado</th>
                      <th className="px-4 py-3 font-medium">IP</th>
                      <th className="px-4 py-3 font-medium">Navegador</th>
                    </tr>
                  </thead>
                  <tbody>
                    {events.map((ev) => (
                      <tr key={ev.id} className="border-b border-lq-line-soft">
                        <td className="px-4 py-3 tabular-nums text-lq-muted">
                          {formatChileDateTime(ev.created_at)}
                        </td>
                        <td className="px-4 py-3">{ev.email || (ev.user_id != null ? `#${ev.user_id}` : "—")}</td>
                        <td className="px-4 py-3">{ev.event_type}</td>
                        <td className="px-4 py-3">
                          {ev.success == null ? "—" : ev.success ? "OK" : "Fallido"}
                        </td>
                        <td className="px-4 py-3 font-mono text-xs text-lq-muted">{ev.ip || "—"}</td>
                        <td className="max-w-[240px] truncate px-4 py-3 text-xs text-lq-faint" title={ev.user_agent || ""}>
                          {ev.user_agent || "—"}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </ProfessionalDataTable>
            </>
          )}
        </TabsContent>
      </Tabs>

      <UserAdminSheet
        open={sheetOpen}
        onOpenChange={setSheetOpen}
        mode={sheetMode}
        user={selected}
        canMutate={canMutate && !(selected?.is_owner && sheetMode === "view")}
        canAssignAdm={canAssignAdm}
        assignableRoles={assignableRoles}
        companiesCatalog={companies}
        targetIsOwner={Boolean(selected?.is_owner)}
        onSaved={() => {
          void loadUsers()
        }}
      />
    </AnalyticPageShell>
  )
}
