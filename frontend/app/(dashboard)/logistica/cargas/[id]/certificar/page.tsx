"use client"

import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import { useParams, useRouter } from "next/navigation"
import {
  ArrowLeft,
  Check,
  Loader2,
  Minus,
  Plus,
  ScanLine,
  Search,
  ShieldCheck,
} from "lucide-react"

import {
  addCargaUnits,
  certifyCarga,
  getCarga,
  reportCargaIssue,
  searchCargaItems,
  startCarga,
  type LoadDetail,
  type LoadItem,
} from "@/lib/cargas-api"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { cn } from "@/lib/utils"

const STATUS_FILTERS = [
  { id: "all", label: "Todos" },
  { id: "pending", label: "Pendientes" },
  { id: "partial", label: "Parciales" },
  { id: "complete", label: "Completos" },
  { id: "diff", label: "Diferencias" },
] as const

const ISSUE_TYPES = [
  { id: "not_found", label: "No encontrado" },
  { id: "insufficient_stock", label: "Stock insuficiente" },
  { id: "wrong_product", label: "Producto equivocado" },
  { id: "damaged", label: "Producto dañado" },
  { id: "excess", label: "Exceso de unidades" },
  { id: "picking_error", label: "Error de picking" },
  { id: "other", label: "Otro" },
] as const

export default function CertificarCargaPage() {
  const params = useParams()
  const router = useRouter()
  const loadId = Number(params.id)
  const [load, setLoad] = useState<LoadDetail | null>(null)
  const [items, setItems] = useState<LoadItem[]>([])
  const [q, setQ] = useState("")
  const [statusFilter, setStatusFilter] = useState("pending")
  const [typeFilter, setTypeFilter] = useState("all")
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [active, setActive] = useState<LoadItem | null>(null)
  const [boxes, setBoxes] = useState(0)
  const [loose, setLoose] = useState(0)
  const [saving, setSaving] = useState(false)
  const [issueOpen, setIssueOpen] = useState(false)
  const [certifyOpen, setCertifyOpen] = useState(false)
  const [scanOpen, setScanOpen] = useState(false)
  const [scanMsg, setScanMsg] = useState<string | null>(null)
  const videoRef = useRef<HTMLVideoElement>(null)
  const streamRef = useRef<MediaStream | null>(null)
  const searchTimer = useRef<ReturnType<typeof setTimeout> | null>(null)

  const refresh = useCallback(async () => {
    const data = await getCarga(loadId)
    setLoad(data)
    return data
  }, [loadId])

  const runSearch = useCallback(
    async (query: string, status: string, productType: string) => {
      const rows = await searchCargaItems(loadId, {
        q: query,
        status: status === "all" ? undefined : status,
        product_type: productType === "all" ? undefined : productType,
      })
      setItems(rows)
    },
    [loadId],
  )

  useEffect(() => {
    let cancelled = false
    ;(async () => {
      setLoading(true)
      setError(null)
      try {
        const data = await refresh()
        if (data.status === "pending") {
          await startCarga(loadId)
          await refresh()
        }
        if (!cancelled) await runSearch("", "pending", "all")
      } catch (e) {
        if (!cancelled) setError(e instanceof Error ? e.message : "Error")
      } finally {
        if (!cancelled) setLoading(false)
      }
    })()
    return () => {
      cancelled = true
    }
  }, [loadId, refresh, runSearch])

  useEffect(() => {
    if (searchTimer.current) clearTimeout(searchTimer.current)
    searchTimer.current = setTimeout(() => {
      void runSearch(q, statusFilter, typeFilter).catch((e) =>
        setError(e instanceof Error ? e.message : "Error búsqueda"),
      )
    }, 180)
    return () => {
      if (searchTimer.current) clearTimeout(searchTimer.current)
    }
  }, [q, statusFilter, typeFilter, runSearch])

  const deltaUnits = useMemo(() => {
    if (!active) return 0
    const sec = active.sec && active.sec > 0 ? active.sec : 0
    return boxes * sec + loose
  }, [active, boxes, loose])

  function openItem(item: LoadItem) {
    setActive(item)
    setBoxes(0)
    setLoose(0)
    setError(null)
  }

  async function applyAdd(opts?: {
    complete?: boolean
    oneBox?: boolean
    oneUnit?: boolean
  }) {
    if (!active) return
    setSaving(true)
    setError(null)
    try {
      let body: Parameters<typeof addCargaUnits>[2]
      if (opts?.complete) {
        body = { complete_remaining: true }
      } else if (opts?.oneBox) {
        body = { boxes: 1, loose_units: 0 }
      } else if (opts?.oneUnit) {
        body = { boxes: 0, loose_units: 1 }
      } else {
        body = { boxes, loose_units: loose }
      }
      const updated = await addCargaUnits(loadId, active.id, body)
      setLoad(updated)
      const fresh = updated.items.find((i) => i.id === active.id) || null
      setActive(fresh)
      setBoxes(0)
      setLoose(0)
      await runSearch(q, statusFilter, typeFilter)
      if (fresh?.status === "complete") {
        setTimeout(() => setActive(null), 450)
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : "Error al guardar")
    } finally {
      setSaving(false)
    }
  }

  async function onIssue(issueType: string) {
    if (!active) return
    setSaving(true)
    try {
      const updated = await reportCargaIssue(loadId, active.id, {
        issue_type: issueType,
      })
      setLoad(updated)
      setActive(updated.items.find((i) => i.id === active.id) || null)
      setIssueOpen(false)
      await runSearch(q, statusFilter, typeFilter)
    } catch (e) {
      setError(e instanceof Error ? e.message : "Error incidencia")
    } finally {
      setSaving(false)
    }
  }

  async function onCertify() {
    setSaving(true)
    try {
      const updated = await certifyCarga(loadId)
      setLoad(updated)
      setCertifyOpen(false)
      router.push(`/logistica/cargas/${loadId}`)
    } catch (e) {
      setError(e instanceof Error ? e.message : "No se pudo certificar")
    } finally {
      setSaving(false)
    }
  }

  async function startScan() {
    setScanOpen(true)
    setScanMsg(null)
    try {
      const stream = await navigator.mediaDevices.getUserMedia({
        video: { facingMode: { ideal: "environment" } },
      })
      streamRef.current = stream
      if (videoRef.current) {
        videoRef.current.srcObject = stream
        await videoRef.current.play()
      }
      const BD = (window as unknown as { BarcodeDetector?: new (o: { formats: string[] }) => {
        detect: (s: ImageBitmapSource) => Promise<{ rawValue: string }[]>
      } }).BarcodeDetector
      if (!BD || !videoRef.current) {
        setScanMsg("Escáner nativo no disponible. Use el buscador.")
        return
      }
      const detector = new BD({ formats: ["ean_13", "ean_8", "code_128", "upc_a"] })
      const tick = async () => {
        if (!videoRef.current || !streamRef.current) return
        try {
          const codes = await detector.detect(videoRef.current)
          const code = codes[0]?.rawValue
          if (code) {
            stopScan()
            const found = (load?.items || []).find(
              (i) => (i.barcode || "").replace(/\D/g, "") === code.replace(/\D/g, ""),
            )
            if (!found) {
              setScanMsg("PRODUCTO NO PERTENECE A ESTA CARGA")
              setError("PRODUCTO NO PERTENECE A ESTA CARGA")
              return
            }
            openItem(found)
            return
          }
        } catch {
          /* ignore frame errors */
        }
        requestAnimationFrame(() => void tick())
      }
      void tick()
    } catch {
      setScanMsg("No se pudo abrir la cámara")
    }
  }

  function stopScan() {
    streamRef.current?.getTracks().forEach((t) => t.stop())
    streamRef.current = null
    setScanOpen(false)
  }

  if (loading) {
    return (
      <div className="flex min-h-[50vh] items-center justify-center gap-2 text-muted-foreground">
        <Loader2 className="size-5 animate-spin" />
        Cargando carga…
      </div>
    )
  }

  if (!load) {
    return <p className="p-6 text-destructive">{error || "Carga no encontrada"}</p>
  }

  const summary = load.summary
  const canCertify =
    summary.items_complete === summary.total_items &&
    summary.total_items > 0 &&
    (summary.open_issues || 0) === 0 &&
    summary.items_excess === 0 &&
    load.status !== "certified"

  return (
    <div className="mx-auto flex min-h-[100dvh] max-w-lg flex-col bg-background pb-8">
      <header className="sticky top-0 z-20 border-b bg-background/95 px-3 py-3 backdrop-blur">
        <div className="flex items-center gap-2">
          <Button
            variant="ghost"
            size="icon"
            className="size-10"
            onClick={() => router.push("/logistica/cargas")}
          >
            <ArrowLeft className="size-5" />
          </Button>
          <div className="min-w-0 flex-1">
            <p className="truncate text-lg font-bold leading-tight">
              CARGA #{load.picking_number}
            </p>
            <p className="truncate text-xs text-muted-foreground">
              {(load.destination || "—").toUpperCase()}
              {load.truck ? ` · ${load.truck}` : ""}
            </p>
          </div>
          <Button
            size="sm"
            variant="outline"
            className="h-10"
            disabled={!canCertify}
            onClick={() => setCertifyOpen(true)}
          >
            <ShieldCheck className="mr-1 size-4" />
            Certificar
          </Button>
        </div>
        <div className="mt-3">
          <div className="mb-1 flex justify-between text-xs text-muted-foreground">
            <span>
              {summary.items_complete} / {summary.total_items} productos
            </span>
            <span>{summary.progress_pct}%</span>
          </div>
          <div className="h-2.5 overflow-hidden rounded-full bg-muted">
            <div
              className="h-full rounded-full bg-primary"
              style={{ width: `${Math.min(100, summary.progress_pct)}%` }}
            />
          </div>
        </div>
      </header>

      <div className="sticky top-[7.5rem] z-10 space-y-2 border-b bg-background px-3 py-3">
        <div className="flex gap-2">
          <div className="relative flex-1">
            <Search className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-muted-foreground" />
            <Input
              className="h-12 pl-10 text-base"
              placeholder="Buscar producto..."
              value={q}
              onChange={(e) => setQ(e.target.value)}
            />
          </div>
          <Button className="h-12 px-4" variant="secondary" onClick={() => void startScan()}>
            <ScanLine className="size-5" />
          </Button>
        </div>
        <div className="flex gap-1.5 overflow-x-auto pb-1">
          {STATUS_FILTERS.map((f) => (
            <button
              key={f.id}
              type="button"
              onClick={() => setStatusFilter(f.id)}
              className={cn(
                "shrink-0 rounded-full px-3 py-1.5 text-xs font-medium",
                statusFilter === f.id
                  ? "bg-primary text-primary-foreground"
                  : "bg-muted text-muted-foreground",
              )}
            >
              {f.label}
            </button>
          ))}
        </div>
        {(load.product_types?.length || 0) > 0 ? (
          <div className="flex gap-1.5 overflow-x-auto pb-1">
            <button
              type="button"
              onClick={() => setTypeFilter("all")}
              className={cn(
                "shrink-0 rounded-full px-3 py-1.5 text-xs",
                typeFilter === "all" ? "bg-foreground text-background" : "bg-muted",
              )}
            >
              Todos tipos
            </button>
            {load.product_types!.map((t) => (
              <button
                key={t}
                type="button"
                onClick={() => setTypeFilter(t)}
                className={cn(
                  "shrink-0 rounded-full px-3 py-1.5 text-xs",
                  typeFilter === t ? "bg-foreground text-background" : "bg-muted",
                )}
              >
                {t}
              </button>
            ))}
          </div>
        ) : null}
      </div>

      {error ? (
        <p className="mx-3 mt-3 rounded-md border border-destructive/40 bg-destructive/5 px-3 py-2 text-sm text-destructive">
          {error}
        </p>
      ) : null}

      {(load.recent_certified?.length || 0) > 0 ? (
        <div className="px-3 pt-3">
          <p className="mb-1.5 text-[11px] font-semibold uppercase tracking-wide text-muted-foreground">
            Últimos certificados
          </p>
          <div className="flex gap-2 overflow-x-auto">
            {load.recent_certified!.map((r, i) => (
              <button
                key={`${r.barcode}-${i}`}
                type="button"
                className="max-w-[9rem] shrink-0 rounded-lg border bg-emerald-50 px-2.5 py-2 text-left text-xs dark:bg-emerald-950/30"
                onClick={() => {
                  const it = load.items.find((x) => x.barcode === r.barcode)
                  if (it) openItem(it)
                }}
              >
                <p className="line-clamp-2 font-medium leading-snug">{r.product_name}</p>
              </button>
            ))}
          </div>
        </div>
      ) : null}

      <div className="flex-1 space-y-3 px-3 py-4">
        {items.length === 0 ? (
          <p className="py-12 text-center text-sm text-muted-foreground">
            Sin productos para este filtro.
          </p>
        ) : (
          items.map((item) => (
            <article
              key={item.id}
              className="rounded-xl border bg-card p-4 shadow-sm"
            >
              <p className="text-base font-bold leading-snug">{item.product_name}</p>
              <p className="mt-1 text-xs uppercase tracking-wide text-muted-foreground">
                {item.product_type || "—"}
                {item.sec ? ` · SEC ${item.sec}` : ""}
              </p>
              <div className="mt-3 grid grid-cols-2 gap-2 text-sm">
                <div>
                  <p className="text-[10px] uppercase text-muted-foreground">Solicitado</p>
                  <p className="text-lg font-semibold tabular-nums">
                    {item.requested_units} UN
                  </p>
                  {item.requested_boxes != null ? (
                    <p className="text-xs text-muted-foreground">
                      ≡ {item.requested_boxes} cajas
                    </p>
                  ) : null}
                </div>
                <div>
                  <p className="text-[10px] uppercase text-muted-foreground">Estado</p>
                  <p
                    className={cn(
                      "font-semibold capitalize",
                      item.status === "complete" && "text-emerald-700",
                      item.status === "partial" && "text-amber-700",
                      item.status === "issue" && "text-destructive",
                    )}
                  >
                    {item.status === "complete"
                      ? "✓ Completo"
                      : item.status === "partial"
                        ? "Parcial"
                        : item.status === "issue"
                          ? "Incidencia"
                          : "Pendiente"}
                  </p>
                  <p className="text-xs tabular-nums text-muted-foreground">
                    {item.certified_units} / {item.requested_units}
                  </p>
                </div>
              </div>
              <Button
                className="mt-3 h-12 w-full text-base"
                onClick={() => openItem(item)}
              >
                ABRIR
              </Button>
            </article>
          ))
        )}
      </div>

      <Dialog open={!!active} onOpenChange={(o) => !o && setActive(null)}>
        <DialogContent className="max-h-[92dvh] max-w-lg overflow-y-auto p-0 sm:rounded-xl">
          {active ? (
            <div className="space-y-4 p-4 pb-6">
              <DialogHeader className="text-left">
                <DialogTitle className="text-xl leading-snug">
                  {active.product_name}
                </DialogTitle>
                <p className="text-sm text-muted-foreground">
                  {active.product_type || "—"}
                </p>
              </DialogHeader>

              <div className="grid grid-cols-2 gap-3 rounded-xl bg-muted/50 p-3 text-sm">
                <div>
                  <p className="text-[10px] uppercase text-muted-foreground">SEC</p>
                  <p className="text-lg font-bold">{active.sec ?? "—"}</p>
                </div>
                <div>
                  <p className="text-[10px] uppercase text-muted-foreground">Barcode</p>
                  <p className="font-mono text-sm">{active.barcode || "—"}</p>
                </div>
                <div>
                  <p className="text-[10px] uppercase text-muted-foreground">Solicitado</p>
                  <p className="text-lg font-bold tabular-nums">
                    {active.requested_units} u
                  </p>
                </div>
                <div>
                  <p className="text-[10px] uppercase text-muted-foreground">Cargado</p>
                  <p className="text-lg font-bold tabular-nums">
                    {active.certified_units} / {active.requested_units}
                  </p>
                </div>
                <div className="col-span-2">
                  <p className="text-[10px] uppercase text-muted-foreground">Faltan</p>
                  <p className="text-xl font-bold tabular-nums text-amber-800">
                    {Math.max(0, active.remaining_units ?? 0)} unidades
                    {active.remaining_boxes != null
                      ? ` · ${active.remaining_boxes} cajas`
                      : ""}
                  </p>
                </div>
              </div>

              {active.status === "complete" ? (
                <p className="rounded-lg bg-emerald-100 px-3 py-3 text-center text-lg font-bold text-emerald-800">
                  ✓ COMPLETO
                </p>
              ) : (
                <>
                  <div className="grid grid-cols-2 gap-3">
                    <Stepper
                      label="Cajas"
                      value={boxes}
                      onChange={setBoxes}
                      disabled={!active.sec}
                    />
                    <Stepper label="Unidades" value={loose} onChange={setLoose} />
                  </div>
                  <p className="text-center text-sm text-muted-foreground">
                    Esta operación agregará{" "}
                    <span className="font-semibold text-foreground">{deltaUnits}</span>{" "}
                    unidades
                  </p>
                  <div className="grid grid-cols-2 gap-2">
                    <Button
                      className="h-12"
                      variant="secondary"
                      disabled={saving || !active.sec}
                      onClick={() => void applyAdd({ oneBox: true })}
                    >
                      + 1 CAJA
                    </Button>
                    <Button
                      className="h-12"
                      variant="secondary"
                      disabled={saving}
                      onClick={() => void applyAdd({ oneUnit: true })}
                    >
                      + 1 UNIDAD
                    </Button>
                  </div>
                  <Button
                    className="h-14 w-full text-base"
                    disabled={saving || deltaUnits <= 0}
                    onClick={() => void applyAdd()}
                  >
                    {saving ? <Loader2 className="size-5 animate-spin" /> : "INGRESAR CANTIDAD"}
                  </Button>
                  {(active.remaining_units || 0) > 0 ? (
                    <Button
                      className="h-12 w-full"
                      variant="outline"
                      disabled={saving}
                      onClick={() => void applyAdd({ complete: true })}
                    >
                      <Check className="mr-1 size-4" />
                      CARGAR TODO LO PENDIENTE
                      {active.remaining_boxes
                        ? ` (${active.remaining_boxes} cajas)`
                        : ""}
                    </Button>
                  ) : null}
                </>
              )}

              <Button
                variant="ghost"
                className="h-11 w-full text-destructive"
                onClick={() => setIssueOpen(true)}
              >
                REPORTAR PROBLEMA
              </Button>
            </div>
          ) : null}
        </DialogContent>
      </Dialog>

      <Dialog open={issueOpen} onOpenChange={setIssueOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Reportar problema</DialogTitle>
          </DialogHeader>
          <div className="grid gap-2">
            {ISSUE_TYPES.map((t) => (
              <Button
                key={t.id}
                variant="outline"
                className="h-12 justify-start"
                disabled={saving}
                onClick={() => void onIssue(t.id)}
              >
                {t.label}
              </Button>
            ))}
          </div>
        </DialogContent>
      </Dialog>

      <Dialog open={certifyOpen} onOpenChange={setCertifyOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Certificar carga #{load.picking_number}</DialogTitle>
          </DialogHeader>
          <ul className="space-y-1 text-sm">
            <li>
              Productos: {summary.items_complete} / {summary.total_items}
            </li>
            <li>
              Unidades: {summary.certified_units} / {summary.requested_units}
            </li>
            <li>Faltantes: {summary.items_pending}</li>
            <li>Excesos: {summary.items_excess}</li>
            <li>Incidencias: {summary.open_issues || 0}</li>
          </ul>
          <DialogFooter>
            <Button variant="outline" onClick={() => setCertifyOpen(false)}>
              Cancelar
            </Button>
            <Button disabled={saving || !canCertify} onClick={() => void onCertify()}>
              CONFIRMAR CERTIFICACIÓN
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog
        open={scanOpen}
        onOpenChange={(o) => {
          if (!o) stopScan()
        }}
      >
        <DialogContent className="max-w-md p-0 overflow-hidden">
          <div className="bg-black">
            <video ref={videoRef} className="aspect-[3/4] w-full object-cover" muted playsInline />
          </div>
          <div className="space-y-2 p-4">
            <p className="text-sm text-muted-foreground">
              Apunte al código de barras. Solo productos de esta carga.
            </p>
            {scanMsg ? <p className="text-sm text-destructive">{scanMsg}</p> : null}
            <Button className="w-full" variant="outline" onClick={stopScan}>
              Cerrar cámara
            </Button>
          </div>
        </DialogContent>
      </Dialog>
    </div>
  )
}

function Stepper({
  label,
  value,
  onChange,
  disabled,
}: {
  label: string
  value: number
  onChange: (n: number) => void
  disabled?: boolean
}) {
  return (
    <div className={cn("rounded-xl border p-3", disabled && "opacity-50")}>
      <p className="mb-2 text-center text-xs font-semibold uppercase text-muted-foreground">
        {label}
      </p>
      <div className="flex items-center justify-between gap-2">
        <Button
          type="button"
          size="icon"
          variant="secondary"
          className="size-12"
          disabled={disabled || value <= 0}
          onClick={() => onChange(Math.max(0, value - 1))}
        >
          <Minus className="size-5" />
        </Button>
        <span className="min-w-[3rem] text-center text-2xl font-bold tabular-nums">
          {value}
        </span>
        <Button
          type="button"
          size="icon"
          variant="secondary"
          className="size-12"
          disabled={disabled}
          onClick={() => onChange(value + 1)}
        >
          <Plus className="size-5" />
        </Button>
      </div>
    </div>
  )
}
