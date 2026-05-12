"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import {
  fetchDiagnosticsEndpoints,
  fetchDiagnosticsErrors,
  fetchDiagnosticsHealth,
  fetchDiagnosticsLogs,
  fetchDiagnosticsRequests,
  type DiagnosticsErrorRow,
  type DiagnosticsHealth,
  type DiagnosticsLogRow,
  type DiagnosticsRequestRow,
  type ObservedRoute,
  type RegisteredRoute,
} from "@/lib/diagnostics-api"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Loader2, RefreshCw, Download, FilterX } from "lucide-react"

const ADMIN_ROLES = new Set(["admin", "superadmin", "super_admin", "administrator"])

function readStaffRole(): string | null {
  if (typeof window === "undefined") return null
  return localStorage.getItem("role")
}

function isStaffAdmin(role: string | null): boolean {
  if (!role) return false
  return ADMIN_ROLES.has(role.toLowerCase().trim())
}

const FE_VERSION = process.env.NEXT_PUBLIC_APP_VERSION ?? "0.1.0"

export default function DiagnosticoErpPage() {
  const [role, setRole] = useState<string | null>(null)
  const [hydrated, setHydrated] = useState(false)
  const [loading, setLoading] = useState(false)
  const [loadError, setLoadError] = useState<string | null>(null)
  const [updatedAt, setUpdatedAt] = useState<string | null>(null)
  const [health, setHealth] = useState<DiagnosticsHealth | null>(null)
  const [requests, setRequests] = useState<DiagnosticsRequestRow[]>([])
  const [logs, setLogs] = useState<DiagnosticsLogRow[]>([])
  const [errors, setErrors] = useState<DiagnosticsErrorRow[]>([])
  const [registered, setRegistered] = useState<RegisteredRoute[]>([])
  const [observed, setObserved] = useState<ObservedRoute[]>([])

  const [reqMethod, setReqMethod] = useState<string>("all")
  const [reqStatus, setReqStatus] = useState<string>("")
  const [reqPath, setReqPath] = useState("")
  const [reqErrorsOnly, setReqErrorsOnly] = useState(false)

  const [logLevel, setLogLevel] = useState<string>("all")
  const [logSearch, setLogSearch] = useState("")
  const [logErrorsOnly, setLogErrorsOnly] = useState(false)

  useEffect(() => {
    setRole(readStaffRole())
    setHydrated(true)
  }, [])

  const canAccess = isStaffAdmin(role)

  const loadAll = useCallback(async () => {
    if (!canAccess) return
    setLoading(true)
    setLoadError(null)
    try {
      const [h, r, l, e, ep] = await Promise.all([
        fetchDiagnosticsHealth(),
        fetchDiagnosticsRequests(250),
        fetchDiagnosticsLogs(250),
        fetchDiagnosticsErrors(150),
        fetchDiagnosticsEndpoints(),
      ])
      setHealth(h)
      setRequests(r.items)
      setLogs(l.items)
      setErrors(e.items)
      setRegistered(ep.registered)
      setObserved(ep.observed)
      setUpdatedAt(new Date().toISOString())
    } catch (err) {
      setLoadError(err instanceof Error ? err.message : String(err))
    } finally {
      setLoading(false)
    }
  }, [canAccess])

  useEffect(() => {
    if (hydrated && canAccess) void loadAll()
  }, [hydrated, canAccess, loadAll])

  const filteredRequests = useMemo(() => {
    return requests.filter((row) => {
      if (reqErrorsOnly && row.statusCode < 400 && !row.error) return false
      if (reqMethod !== "all" && row.method !== reqMethod) return false
      if (reqStatus.trim()) {
        const n = parseInt(reqStatus, 10)
        if (Number.isFinite(n) && row.statusCode !== n) return false
      }
      if (reqPath.trim() && !row.path.toLowerCase().includes(reqPath.trim().toLowerCase())) return false
      return true
    })
  }, [requests, reqErrorsOnly, reqMethod, reqStatus, reqPath])

  const filteredLogs = useMemo(() => {
    return logs.filter((row) => {
      if (logErrorsOnly && !["error", "critical"].includes(row.level.toLowerCase())) return false
      if (logLevel !== "all") {
        const lv = row.level.toLowerCase()
        const want = logLevel.toLowerCase()
        if (want === "warn" || want === "warning") {
          if (lv !== "warning" && lv !== "warn") return false
        } else if (lv !== want) {
          return false
        }
      }
      if (logSearch.trim()) {
        const q = logSearch.trim().toLowerCase()
        if (
          !row.message.toLowerCase().includes(q) &&
          !row.module.toLowerCase().includes(q) &&
          !(row.detail ?? "").toLowerCase().includes(q)
        ) {
          return false
        }
      }
      return true
    })
  }, [logs, logErrorsOnly, logLevel, logSearch])

  const clearVisualFilters = () => {
    setReqMethod("all")
    setReqStatus("")
    setReqPath("")
    setReqErrorsOnly(false)
    setLogLevel("all")
    setLogSearch("")
    setLogErrorsOnly(false)
  }

  const downloadJson = () => {
    const payload = {
      exportedAt: new Date().toISOString(),
      health,
      requests: filteredRequests,
      logs: filteredLogs,
      errors,
      endpoints: { registered, observed },
    }
    const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" })
    const url = URL.createObjectURL(blob)
    const a = document.createElement("a")
    a.href = url
    a.download = `diagnostico-erp-${new Date().toISOString().replace(/[:.]/g, "-")}.json`
    a.click()
    URL.revokeObjectURL(url)
  }

  const frontendOnline = typeof navigator !== "undefined" ? navigator.onLine : true
  const backendOk = health?.backend === "online" && health?.status !== "degraded"
  const dbOk = health?.database === "connected"

  if (!hydrated) {
    return (
      <div className="flex min-h-[40vh] items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    )
  }

  if (!canAccess) {
    return (
      <div className="mx-auto max-w-2xl space-y-4">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">Panel de Diagnóstico ERP</h1>
          <p className="text-muted-foreground text-sm">
            Vista interna para revisar estado del sistema, peticiones, endpoints y logs recientes.
          </p>
        </div>
        <Alert variant="destructive">
          <AlertTitle>Acceso restringido</AlertTitle>
          <AlertDescription>
            Esta ruta está reservada para usuarios con rol de administración en el login staff
            (por ejemplo <code className="text-xs">admin</code>). Tu sesión no tiene ese rol o no
            hay token. No uses esta página en producción sin autenticación y roles definidos; ver{" "}
            <code className="text-xs">docs/diagnostics/PANEL_DIAGNOSTICO_ERP.md</code>.
          </AlertDescription>
        </Alert>
      </div>
    )
  }

  return (
    <div className="mx-auto max-w-7xl space-y-8">
      <div className="flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">Panel de Diagnóstico ERP</h1>
          <p className="text-muted-foreground max-w-3xl text-sm">
            Vista interna para revisar estado del sistema, peticiones, endpoints y logs recientes.
            Los datos sensibles no se incluyen en las respuestas del API de diagnóstico.
          </p>
        </div>
        <div className="flex flex-wrap gap-2">
          <Button type="button" variant="default" size="sm" onClick={() => void loadAll()} disabled={loading}>
            {loading ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <RefreshCw className="mr-2 h-4 w-4" />}
            Refrescar datos
          </Button>
          <Button type="button" variant="outline" size="sm" onClick={clearVisualFilters}>
            <FilterX className="mr-2 h-4 w-4" />
            Limpiar filtros
          </Button>
          <Button type="button" variant="outline" size="sm" onClick={downloadJson} disabled={!health}>
            <Download className="mr-2 h-4 w-4" />
            Descargar JSON
          </Button>
        </div>
      </div>

      {loadError ? (
        <Alert variant="destructive">
          <AlertTitle>No se pudo cargar el diagnóstico</AlertTitle>
          <AlertDescription className="space-y-2">
            <p className="text-sm">{loadError}</p>
            <p className="text-muted-foreground text-xs">
              Si ves 404, en staging/producción el API puede estar deshabilitado: define{" "}
              <code className="rounded bg-muted px-1">ENABLE_DIAGNOSTICS=true</code> en el backend y
              reinicia. Sigue necesitando JWT de usuario admin.
            </p>
          </AlertDescription>
        </Alert>
      ) : null}

      <section className="space-y-3">
        <h2 className="text-lg font-medium">Estado general</h2>
        <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium">Frontend</CardTitle>
              <CardDescription>Navegador / build</CardDescription>
            </CardHeader>
            <CardContent className="space-y-1 text-sm">
              <div className="flex items-center gap-2">
                <Badge variant={frontendOnline ? "default" : "destructive"}>
                  {frontendOnline ? "Online" : "Offline"}
                </Badge>
              </div>
              <p className="text-muted-foreground text-xs">Versión UI: {FE_VERSION}</p>
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium">Backend</CardTitle>
              <CardDescription>API staff</CardDescription>
            </CardHeader>
            <CardContent className="space-y-1 text-sm">
              <Badge variant={backendOk ? "default" : "secondary"}>
                {health ? (backendOk ? "Operativo" : "Degradado / revisar") : loading ? "…" : "Sin datos"}
              </Badge>
              {health?.diagnosticsApiEnabled === false ? (
                <p className="text-muted-foreground text-xs">API diagnóstico deshabilitada en este entorno.</p>
              ) : null}
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium">Base de datos</CardTitle>
              <CardDescription>PostgreSQL (ping)</CardDescription>
            </CardHeader>
            <CardContent>
              <Badge variant={dbOk ? "default" : "destructive"}>
                {health ? (dbOk ? "Conectada" : "Desconectada") : "…"}
              </Badge>
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium">Servidor / panel</CardTitle>
              <CardDescription>Tiempos y versión API</CardDescription>
            </CardHeader>
            <CardContent className="space-y-1 text-xs text-muted-foreground">
              <p>
                <span className="text-foreground">Hora servidor (UTC):</span> {health?.serverTime ?? "—"}
              </p>
              <p>
                <span className="text-foreground">Ambiente:</span> {health?.environment ?? "—"}
              </p>
              <p>
                <span className="text-foreground">Versión API:</span> {health?.version ?? "—"}
              </p>
              <p>
                <span className="text-foreground">Uptime proceso (s):</span> {health?.uptime ?? "—"}
              </p>
              <p>
                <span className="text-foreground">Última actualización panel:</span>{" "}
                {updatedAt ? new Date(updatedAt).toLocaleString() : "—"}
              </p>
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium">Requests (buffer)</CardTitle>
              <CardDescription>Muestras en memoria</CardDescription>
            </CardHeader>
            <CardContent className="text-2xl font-semibold">{health?.recentRequestCount ?? "—"}</CardContent>
          </Card>
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium">Errores (buffer)</CardTitle>
              <CardDescription>Contador reciente</CardDescription>
            </CardHeader>
            <CardContent className="text-2xl font-semibold text-destructive">
              {health?.recentErrorCount ?? "—"}
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium">Tiempo medio respuesta</CardTitle>
              <CardDescription>Desde buffer de peticiones</CardDescription>
            </CardHeader>
            <CardContent className="text-2xl font-semibold">
              {health?.avgResponseTimeMs != null ? `${health.avgResponseTimeMs} ms` : "—"}
            </CardContent>
          </Card>
        </div>
      </section>

      <section className="space-y-3">
        <h2 className="text-lg font-medium">Salud del backend</h2>
        <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm">Backend operativo</CardTitle>
            </CardHeader>
            <CardContent>
              <Badge variant={health?.backend === "online" ? "default" : "destructive"}>
                {health?.backend === "online" ? "Sí" : "No"}
              </Badge>
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm">Base de datos</CardTitle>
            </CardHeader>
            <CardContent>
              <Badge variant={dbOk ? "default" : "destructive"}>{dbOk ? "Conectada" : "No"}</Badge>
              {health?.databaseError ? (
                <p className="text-muted-foreground mt-2 text-xs">{health.databaseError}</p>
              ) : null}
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm">Resumen buffer</CardTitle>
              <CardDescription>Peticiones / errores / ms medio</CardDescription>
            </CardHeader>
            <CardContent className="text-sm text-muted-foreground space-y-1">
              <p>
                <span className="text-foreground">Peticiones:</span> {health?.recentRequestCount ?? "—"}
              </p>
              <p>
                <span className="text-foreground">Errores:</span> {health?.recentErrorCount ?? "—"}
              </p>
              <p>
                <span className="text-foreground">Tiempo medio:</span>{" "}
                {health?.avgResponseTimeMs != null ? `${health.avgResponseTimeMs} ms` : "—"}
              </p>
            </CardContent>
          </Card>
        </div>
        <p className="text-muted-foreground text-xs">
          Servicios externos (Bsale, ORS, etc.) no se prueban automáticamente aquí;{" "}
          <strong>TODO</strong>: health checks opcionales con timeouts cortos y sin secretos.
        </p>
      </section>

      <section className="space-y-3">
        <h2 className="text-lg font-medium">Acciones rápidas (filtros)</h2>
        <div className="flex flex-wrap gap-2">
          <Button type="button" size="sm" variant={reqErrorsOnly ? "default" : "outline"} onClick={() => setReqErrorsOnly((v) => !v)}>
            Solo errores HTTP (4xx/5xx)
          </Button>
          <Button type="button" size="sm" variant="outline" onClick={() => setReqMethod("GET")}>
            Solo GET
          </Button>
          <Button type="button" size="sm" variant="outline" onClick={() => setReqMethod("POST")}>
            Solo POST
          </Button>
          <Button type="button" size="sm" variant="outline" onClick={() => setReqStatus("500")}>
            Solo 500
          </Button>
          <Button type="button" size="sm" variant="outline" onClick={() => setReqStatus("400")}>
            Solo 400
          </Button>
          <Button type="button" size="sm" variant="outline" onClick={() => setReqStatus("401")}>
            401
          </Button>
          <Button type="button" size="sm" variant="outline" onClick={() => setReqStatus("403")}>
            403
          </Button>
          <Button type="button" size="sm" variant="outline" onClick={() => setReqStatus("404")}>
            404
          </Button>
        </div>
        <p className="text-muted-foreground text-xs">
          Los botones de código HTTP fijan el filtro &quot;Status&quot; numérico en la pestaña Peticiones.
        </p>
      </section>

      <Tabs defaultValue="requests" className="w-full">
        <TabsList className="flex flex-wrap">
          <TabsTrigger value="requests">Peticiones</TabsTrigger>
          <TabsTrigger value="logs">Logs</TabsTrigger>
          <TabsTrigger value="errors">Errores</TabsTrigger>
          <TabsTrigger value="endpoints">Endpoints</TabsTrigger>
        </TabsList>

        <TabsContent value="requests" className="space-y-3">
          <Card>
            <CardHeader>
              <CardTitle>Peticiones HTTP recientes</CardTitle>
              <CardDescription>Registradas en el backend (sin cuerpos ni cabeceras sensibles).</CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="grid gap-3 md:grid-cols-4">
                <div className="space-y-1">
                  <Label>Método</Label>
                  <Select value={reqMethod} onValueChange={setReqMethod}>
                    <SelectTrigger>
                      <SelectValue placeholder="Método" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="all">Todos</SelectItem>
                      <SelectItem value="GET">GET</SelectItem>
                      <SelectItem value="POST">POST</SelectItem>
                      <SelectItem value="PUT">PUT</SelectItem>
                      <SelectItem value="PATCH">PATCH</SelectItem>
                      <SelectItem value="DELETE">DELETE</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <div className="space-y-1">
                  <Label>Status</Label>
                  <Input
                    placeholder="ej. 500"
                    value={reqStatus}
                    onChange={(e) => setReqStatus(e.target.value.replace(/[^\d]/g, "").slice(0, 3))}
                  />
                </div>
                <div className="space-y-1 md:col-span-2">
                  <Label>Endpoint (contiene)</Label>
                  <Input placeholder="/diagnostics…" value={reqPath} onChange={(e) => setReqPath(e.target.value)} />
                </div>
              </div>
              <div className="rounded-md border">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Fecha/Hora</TableHead>
                      <TableHead>Método</TableHead>
                      <TableHead>Endpoint</TableHead>
                      <TableHead>Status</TableHead>
                      <TableHead>ms</TableHead>
                      <TableHead>Usuario</TableHead>
                      <TableHead>IP</TableHead>
                      <TableHead>Origen</TableHead>
                      <TableHead>Error</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {filteredRequests.length === 0 ? (
                      <TableRow>
                        <TableCell colSpan={9} className="text-muted-foreground text-center text-sm">
                          Sin peticiones en el buffer o ninguna coincide con los filtros.
                        </TableCell>
                      </TableRow>
                    ) : (
                      filteredRequests.map((row, i) => (
                        <TableRow key={`${row.timestamp}-${i}`}>
                          <TableCell className="whitespace-nowrap text-xs">{row.timestamp}</TableCell>
                          <TableCell>
                            <Badge variant="outline">{row.method}</Badge>
                          </TableCell>
                          <TableCell className="max-w-[240px] truncate text-xs" title={row.path}>
                            {row.path}
                          </TableCell>
                          <TableCell>{row.statusCode}</TableCell>
                          <TableCell>{row.durationMs}</TableCell>
                          <TableCell className="max-w-[120px] truncate text-xs">{row.user ?? "—"}</TableCell>
                          <TableCell className="text-xs">{row.clientIp ?? "—"}</TableCell>
                          <TableCell className="max-w-[140px] truncate text-xs" title={row.origin ?? ""}>
                            {row.origin ?? "—"}
                          </TableCell>
                          <TableCell className="max-w-[200px] truncate text-xs text-destructive" title={row.error ?? ""}>
                            {row.error ?? "—"}
                          </TableCell>
                        </TableRow>
                      ))
                    )}
                  </TableBody>
                </Table>
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="logs" className="space-y-3">
          <Card>
            <CardHeader>
              <CardTitle>Logs recientes</CardTitle>
              <CardDescription>Handlers en memoria (backend, uvicorn). Se pierden al reiniciar el proceso.</CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="grid gap-3 md:grid-cols-3">
                <div className="space-y-1">
                  <Label>Nivel</Label>
                  <Select value={logLevel} onValueChange={setLogLevel}>
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="all">Todos</SelectItem>
                      <SelectItem value="info">info</SelectItem>
                      <SelectItem value="warning">warning</SelectItem>
                      <SelectItem value="error">error</SelectItem>
                      <SelectItem value="debug">debug</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <div className="space-y-1 md:col-span-2">
                  <Label>Buscar texto</Label>
                  <Input value={logSearch} onChange={(e) => setLogSearch(e.target.value)} placeholder="módulo o mensaje" />
                </div>
                <div className="flex items-center gap-2 md:col-span-3">
                  <Button type="button" size="sm" variant={logErrorsOnly ? "default" : "outline"} onClick={() => setLogErrorsOnly((v) => !v)}>
                    Solo errores (nivel)
                  </Button>
                </div>
              </div>
              <div className="rounded-md border">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Fecha/Hora</TableHead>
                      <TableHead>Nivel</TableHead>
                      <TableHead>Módulo</TableHead>
                      <TableHead>Mensaje</TableHead>
                      <TableHead>Detalle</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {filteredLogs.length === 0 ? (
                      <TableRow>
                        <TableCell colSpan={5} className="text-muted-foreground text-center text-sm">
                          Sin logs o ninguno coincide con los filtros.
                        </TableCell>
                      </TableRow>
                    ) : (
                      filteredLogs.map((row, i) => (
                        <TableRow key={`${row.timestamp}-${i}`}>
                          <TableCell className="whitespace-nowrap text-xs">{row.timestamp}</TableCell>
                          <TableCell>
                            <Badge variant="outline">{row.level}</Badge>
                          </TableCell>
                          <TableCell className="max-w-[160px] truncate text-xs">{row.module}</TableCell>
                          <TableCell className="max-w-[280px] truncate text-xs">{row.message}</TableCell>
                          <TableCell className="max-w-[220px] truncate text-xs text-muted-foreground" title={row.detail ?? ""}>
                            {row.detail ?? "—"}
                          </TableCell>
                        </TableRow>
                      ))
                    )}
                  </TableBody>
                </Table>
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="errors" className="space-y-3">
          <Card>
            <CardHeader>
              <CardTitle>Errores recientes</CardTitle>
              <CardDescription>HTTP 4xx/5xx desde el buffer de peticiones y niveles ERROR/CRITICAL en logs.</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="rounded-md border">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Fecha/Hora</TableHead>
                      <TableHead>Endpoint</TableHead>
                      <TableHead>Status</TableHead>
                      <TableHead>Mensaje</TableHead>
                      <TableHead>Detalle</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {errors.length === 0 ? (
                      <TableRow>
                        <TableCell colSpan={5} className="text-muted-foreground text-center text-sm">
                          Sin errores registrados en el buffer.
                        </TableCell>
                      </TableRow>
                    ) : (
                      errors.map((row, i) => (
                        <TableRow key={`${row.timestamp}-${i}`}>
                          <TableCell className="whitespace-nowrap text-xs">{row.timestamp}</TableCell>
                          <TableCell className="max-w-[220px] truncate text-xs">{row.endpoint ?? "—"}</TableCell>
                          <TableCell>{row.statusCode ?? "—"}</TableCell>
                          <TableCell className="max-w-[260px] truncate text-xs">{row.message}</TableCell>
                          <TableCell className="max-w-[220px] truncate text-xs text-muted-foreground">{row.detail ?? "—"}</TableCell>
                        </TableRow>
                      ))
                    )}
                  </TableBody>
                </Table>
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="endpoints" className="space-y-4">
          <Card>
            <CardHeader>
              <CardTitle>Endpoints registrados (FastAPI)</CardTitle>
              <CardDescription>Rutas definidas en la app (puede incluir rutas internas).</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="rounded-md border">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Método</TableHead>
                      <TableHead>Endpoint</TableHead>
                      <TableHead>Nombre</TableHead>
                      <TableHead>Descripción</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {registered.slice(0, 400).map((row, i) => (
                      <TableRow key={`${row.method}-${row.path}-${i}`}>
                        <TableCell>
                          <Badge variant="outline">{row.method}</Badge>
                        </TableCell>
                        <TableCell className="max-w-[280px] truncate text-xs">{row.path}</TableCell>
                        <TableCell className="text-xs">{row.name}</TableCell>
                        <TableCell className="text-muted-foreground text-xs">{row.description || "—"}</TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardHeader>
              <CardTitle>Tráfico observado (buffer)</CardTitle>
              <CardDescription>Agregación simple por método + path desde peticiones registradas.</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="rounded-md border">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Método</TableHead>
                      <TableHead>Endpoint</TableHead>
                      <TableHead>Estado</TableHead>
                      <TableHead>Última llamada</TableHead>
                      <TableHead>Tiempo medio ms</TableHead>
                      <TableHead>Errores recientes</TableHead>
                      <TableHead>Muestras</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {observed.length === 0 ? (
                      <TableRow>
                        <TableCell colSpan={7} className="text-muted-foreground text-center text-sm">
                          Aún no hay tráfico muestreado en memoria.
                        </TableCell>
                      </TableRow>
                    ) : (
                      observed.map((row, i) => (
                        <TableRow key={`${row.method}-${row.path}-${i}`}>
                          <TableCell>
                            <Badge variant="outline">{row.method}</Badge>
                          </TableCell>
                          <TableCell className="max-w-[280px] truncate text-xs">{row.path}</TableCell>
                          <TableCell>{row.status}</TableCell>
                          <TableCell className="text-xs">{row.lastCall ?? "—"}</TableCell>
                          <TableCell>{row.avgDurationMs}</TableCell>
                          <TableCell>{row.recentErrors}</TableCell>
                          <TableCell>{row.callCount}</TableCell>
                        </TableRow>
                      ))
                    )}
                  </TableBody>
                </Table>
              </div>
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  )
}
