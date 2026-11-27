"use client"

import { useEffect, useRef, useState } from "react"
import { History, Loader2, RefreshCw, Settings, ShieldCheck } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Switch } from "@/components/ui/switch"
import { Badge } from "@/components/ui/badge"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { isOwnerUser } from "@/lib/permissions"
import { useAuthMe } from "@/lib/use-auth-me"
import { getSystemFeatures, updateSystemFeature, type SystemFeature, type SystemFeatureState } from "@/lib/system-feature-controls-api"

function formatDate(value: string | null) {
  return value ? new Date(value).toLocaleString("es-CL", { timeZone: "America/Santiago", dateStyle: "short", timeStyle: "short" }) : "—"
}
function stateLabel(value: boolean) { return value ? "Activado" : "Desactivado" }

export default function SystemConfigurationPage() {
  const { meState, staff, reload } = useAuthMe()
  const owner = meState === "ok" && isOwnerUser(staff)
  const [data, setData] = useState<SystemFeatureState | null>(null)
  const [loading, setLoading] = useState(false)
  const [saving, setSaving] = useState<string | null>(null)
  const [error, setError] = useState("")
  const [notice, setNotice] = useState("")
  const [revision, setRevision] = useState(0)
  const action = useRef(false)

  useEffect(() => {
    if (!owner) { setData(null); return }
    const controller = new AbortController()
    setLoading(true); setError("")
    void getSystemFeatures(controller.signal).then(value => {
      if (!controller.signal.aborted) setData(value)
    }).catch(e => {
      if (!controller.signal.aborted) { setData(null); setError(String(e.message || e)) }
    }).finally(() => { if (!controller.signal.aborted) setLoading(false) })
    return () => controller.abort()
  }, [owner, revision])

  async function change(item: SystemFeature, next: boolean | null) {
    if (!owner || loading || action.current || !data?.schema_ready) return
    action.current = true
    setSaving(item.key); setError(""); setNotice("")
    try {
      const result = await updateSystemFeature(item, next)
      setData(result)
      setNotice(item.title + ": configuración guardada.")
    } catch (e) {
      setError(e instanceof Error ? e.message : "No se pudo guardar.")
      setData(null)
    } finally {
      action.current = false
      setSaving(null)
    }
  }

  if (meState === "loading") return <p className="p-6 text-sm text-muted-foreground">Verificando acceso…</p>
  if (!owner) return <Card><CardContent className="space-y-3 p-6"><h1 className="font-semibold">Configuración del sistema</h1><p className="text-sm text-muted-foreground">Acceso exclusivo para OWNER. {meState !== "ok" ? "No se pudo verificar la sesión." : ""}</p>{meState !== "ok" ? <Button variant="outline" onClick={() => void reload()}>Reintentar</Button> : null}</CardContent></Card>

  const titles = new Map(data?.items.map(item => [item.key, item.title]) ?? [])
  return <div className="mx-auto max-w-6xl space-y-6 pb-8">
    <div className="flex flex-wrap items-start justify-between gap-3">
      <div className="space-y-1">
        <div className="flex items-center gap-2 text-xs font-semibold uppercase tracking-wide text-muted-foreground"><Settings className="size-4" /> Administración</div>
        <h1 className="text-2xl font-semibold tracking-tight">Configuración del sistema</h1>
        <p className="text-sm text-muted-foreground">Administra funcionalidades operativas de todo el grupo desde el ERP.</p>
      </div>
      <div className="flex items-center gap-2"><Badge variant="outline" className="gap-1"><ShieldCheck className="size-3.5" /> OWNER</Badge><Button variant="outline" size="sm" disabled={loading || !!saving} onClick={() => setRevision(v => v + 1)}><RefreshCw className={"mr-2 size-4 " + (loading ? "animate-spin" : "")} /> Actualizar</Button></div>
    </div>
    <div className="rounded-xl border bg-muted/20 p-4 text-sm text-muted-foreground">Los cambios se guardan en la base de datos y se aplican a las solicitudes nuevas, sin redeploy. Las credenciales, conexiones, compilación y autorización siguen administradas en el servidor. Las acciones ya iniciadas pueden finalizar.</div>
    {error ? <p role="alert" className="rounded-lg border border-destructive/30 bg-destructive/10 p-3 text-sm text-destructive">{error}</p> : null}
    {notice ? <p role="status" className="rounded-lg border border-emerald-500/30 bg-emerald-500/10 p-3 text-sm text-emerald-700 dark:text-emerald-300">{notice}</p> : null}
    {loading && !data ? <p className="flex items-center gap-2 p-4 text-sm"><Loader2 className="size-4 animate-spin" /> Consultando configuración…</p> : null}
    {data && !data.schema_ready ? <p role="status" className="rounded-lg border border-amber-500/30 bg-amber-500/10 p-3 text-sm">Migración 071 pendiente. Los valores actuales se conservan; la edición desde el ERP todavía no está habilitada.</p> : null}
    {data ? <>
      <Card><CardHeader><CardTitle>Funcionalidades operativas</CardTitle></CardHeader>
        <CardContent className="divide-y">
          {data.items.map(item => <div key={item.key} className="flex flex-col gap-3 py-4 first:pt-0 last:pb-0 sm:flex-row sm:items-start sm:justify-between">
            <div className="min-w-0 flex-1 space-y-2">
              <div className="flex flex-wrap items-center gap-2"><h2 className="font-semibold">{item.title}</h2><Badge variant="outline" className={item.effective_enabled ? "border-emerald-500/30 text-emerald-700 dark:text-emerald-300" : "text-muted-foreground"}>{item.configured_enabled && !item.requirements_ready ? "Bloqueado" : stateLabel(item.effective_enabled)}</Badge></div>
              <p className="text-sm text-muted-foreground">{item.description}</p>
              {item.blocked_reason ? <p className="text-xs text-amber-700 dark:text-amber-300">{item.blocked_reason}</p> : null}
              <p className="text-xs text-muted-foreground">Origen: {item.source === "erp" ? "Configuración ERP" : "Configuración anterior del servidor"} · {item.updated_by != null ? "Usuario #" + item.updated_by + " · " + formatDate(item.updated_at) : "Sin cambios manuales"}</p>
              {item.key === "catalog_photo_writes" ? <p className="text-xs text-muted-foreground">Se mantienen permisos, validación del producto, confirmación y auditoría de publicación. Los permisos reales del token se comprueban al publicar.</p> : null}
            </div>
            <div className="flex items-center gap-3 sm:flex-col sm:items-end">
              <div className="flex items-center gap-2"><span className="text-sm">{stateLabel(item.configured_enabled)}</span><Switch aria-label={item.title} checked={item.configured_enabled} disabled={!data.schema_ready || loading || !!saving || (!item.configured_enabled && !item.requirements_ready)} onCheckedChange={value => void change(item, value)} /></div>
              {saving === item.key ? <span className="flex items-center gap-1 text-xs text-muted-foreground"><Loader2 className="size-3 animate-spin" /> Guardando…</span> : item.enabled_override !== null ? <Button variant="ghost" size="sm" disabled={!data.schema_ready || loading || !!saving || (item.environment_default && !item.requirements_ready)} onClick={() => void change(item, null)}>Usar valor del servidor</Button> : null}
            </div>
          </div>)}
        </CardContent>
      </Card>
      <Card><CardHeader><CardTitle className="flex items-center gap-2"><History className="size-4" /> Historial de cambios</CardTitle></CardHeader><CardContent>
        {data.history.length ? <div className="overflow-x-auto"><table className="w-full min-w-[560px] text-sm"><thead><tr className="border-b text-left text-muted-foreground"><th className="py-2 pr-3">Fecha</th><th className="py-2 pr-3">Funcionalidad</th><th className="py-2 pr-3">Cambio configurado</th><th className="py-2">Usuario</th></tr></thead><tbody>{data.history.map(event => <tr key={event.id} className="border-b last:border-0"><td className="py-2 pr-3 whitespace-nowrap">{formatDate(event.created_at)}</td><td className="py-2 pr-3">{titles.get(event.key) || event.key}</td><td className="py-2 pr-3">{stateLabel(event.before_configured)} → {stateLabel(event.after_configured)} <span className="block text-xs text-muted-foreground">{event.after_override === null ? "Hereda servidor" : "Control ERP"}</span></td><td className="py-2">#{event.actor_user_id}</td></tr>)}</tbody></table></div> : <p className="text-sm text-muted-foreground">Aún no hay cambios registrados.</p>}
      </CardContent></Card>
    </> : null}
  </div>
}
