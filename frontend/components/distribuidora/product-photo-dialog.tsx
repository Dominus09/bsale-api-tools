"use client"

import { useEffect, useRef, useState, type PointerEvent } from "react"
import { CheckCircle2, Crop, ExternalLink, ImagePlus, Loader2, Upload, WandSparkles } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { getAuthHeaders, type ProductMasterRow } from "@/lib/api"
import { getApiBaseUrl } from "@/lib/api-base"

type Mode = "original" | "white" | "remove" | "remove_white"
type CropRect = { left: number; top: number; right: number; bottom: number }
const FULL_CROP: CropRect = { left: 0, top: 0, right: 100, bottom: 100 }
type CropEdge = keyof CropRect
const cropEdges: { key: CropEdge; label: string }[] = [{ key: "left", label: "Izquierda" }, { key: "right", label: "Derecha" }, { key: "top", label: "Arriba" }, { key: "bottom", label: "Abajo" }]
function cropLimits(rect: CropRect, edge: CropEdge): [number, number] {
  if (edge === "left") return [0, rect.right - 2]
  if (edge === "right") return [rect.left + 2, 100]
  if (edge === "top") return [0, rect.bottom - 2]
  return [rect.top + 2, 100]
}
function isCropped(rect: CropRect) { return cropEdges.some(({ key }) => rect[key] !== FULL_CROP[key]) }
function point(e: PointerEvent<HTMLDivElement>) {
  const bounds = e.currentTarget.getBoundingClientRect()
  return { x: Math.max(0, Math.min(100, Math.round((e.clientX - bounds.left) / bounds.width * 100))), y: Math.max(0, Math.min(100, Math.round((e.clientY - bounds.top) / bounds.height * 100))) }
}
type PhotoState = { filename: string; barcode: string; github_sha: string | null; has_photo: boolean | null; github_checked: boolean; github_error: string | null; writes_enabled: boolean; ai_enabled: boolean; catalog_asset_url: string; catalog_variant_ids: number[]; catalog_image_urls: string[]; blocked_reason: string | null }
type Preview = { data_base64: string; bytes: number; width: number; height: number; filename: string; expected_sha: string | null; preview_token: string | null; writes_enabled: boolean; publication_reason?: string | null }
type Published = { commit_sha: string; commit_url: string; catalog_asset_url: string; new_sha: string; published_to_repository: boolean; audit_id: string; audit_pending: boolean }
const modes: { value: Mode; label: string; detail: string; ai?: boolean }[] = [
  { value: "original", label: "Conservar foto", detail: "Mantiene el fondo original" },
  { value: "white", label: "Marco blanco", detail: "Centra sobre lienzo blanco" },
  { value: "remove", label: "Quitar fondo", detail: "Recorte automático transparente", ai: true },
  { value: "remove_white", label: "Recorte + blanco", detail: "Recorte automático sobre blanco", ai: true },
]

async function request<T>(id: number, suffix = "", body?: FormData, signal?: AbortSignal): Promise<T> {
  const headers = new Headers(getAuthHeaders())
  if (body) headers.delete("Content-Type")
  const response = await fetch(`${getApiBaseUrl()}/products-master/id/${id}/photo${suffix}`, {
    method: body ? "POST" : "GET", headers, body, signal, cache: "no-store",
  })
  const result = await response.json().catch(() => null)
  if (!response.ok) throw new Error(typeof result?.detail === "string" ? result.detail : "No se pudo completar la solicitud.")
  return result as T
}

function webpBlob(base64: string) {
  const binary = atob(base64)
  const bytes = Uint8Array.from(binary, c => c.charCodeAt(0))
  return new Blob([bytes], { type: "image/webp" })
}

export function ProductPhotoDialog({ product, onClose, onPublished }: { product: ProductMasterRow; onClose: () => void; onPublished?: () => void }) {
  const [state, setState] = useState<PhotoState | null>(null)
  const [file, setFile] = useState<File | null>(null)
  const [original, setOriginal] = useState<string | null>(null)
  const [mode, setMode] = useState<Mode>("original")
  const [cropRect, setCropRect] = useState<CropRect>(FULL_CROP)
  const [cropSource, setCropSource] = useState<string | null>(null)
  const [cropOpen, setCropOpen] = useState(false)
  const [cropLoaded, setCropLoaded] = useState(false)
  const drag = useRef<{ x: number; y: number } | null>(null)
  const [preview, setPreview] = useState<Preview | null>(null)
  const [published, setPublished] = useState<Published | null>(null)
  const [busy, setBusy] = useState<"loading" | "source" | "prepare" | "commit" | null>("loading")
  const [error, setError] = useState("")
  const [confirmed, setConfirmed] = useState(false)
  const [existingBroken, setExistingBroken] = useState(false)
  const [originalBroken, setOriginalBroken] = useState(false)
  const [refresh, setRefresh] = useState(0)
  const picker = useRef<HTMLInputElement>(null)
  const operation = useRef<AbortController | null>(null)

  useEffect(() => {
    const controller = new AbortController()
    setBusy("loading")
    setError("")
    setState(null)
    setPreview(null)
    setConfirmed(false)
    setExistingBroken(false)
    void request<PhotoState>(product.id, "", undefined, controller.signal).then(setState).catch(e => {
      if (!controller.signal.aborted) setError(String(e.message || e))
    }).finally(() => { if (!controller.signal.aborted) setBusy(null) })
    return () => controller.abort()
  }, [product.id, refresh])

  useEffect(() => {
    setOriginalBroken(false)
    if (!file) { setOriginal(null); return }
    const url = URL.createObjectURL(file)
    setOriginal(url)
    return () => URL.revokeObjectURL(url)
  }, [file])
  useEffect(() => () => operation.current?.abort(), [])

  function selectFile(next: File | null) {
    setPreview(null); setConfirmed(false); setPublished(null); setError("")
    setCropRect(FULL_CROP); setCropSource(null); setCropOpen(false); setCropLoaded(false); drag.current = null
    if (next && next.size > 15 * 1024 * 1024) {
      setFile(null); setError("La foto no puede superar 15 MB."); return
    }
    setFile(next)
  }

  function updateCrop(next: CropRect) {
    setCropRect(next); setPreview(null); setConfirmed(false); setError("")
  }

  async function openCrop() {
    if (!file || busy || published) return
    if (cropSource) { setCropOpen(true); return }
    setBusy("source"); setError(""); setCropLoaded(false)
    const controller = new AbortController()
    operation.current = controller
    const body = new FormData()
    body.append("image", file)
    try {
      const result = await request<{ data_base64: string }>(product.id, "/source-preview", body, controller.signal)
      if (!controller.signal.aborted) { setCropSource("data:image/webp;base64," + result.data_base64); setCropOpen(true) }
    } catch (e) { if (!controller.signal.aborted) setError(e instanceof Error ? e.message : "No se pudo abrir el recorte.") }
    finally { if (!controller.signal.aborted) setBusy(null) }
  }

  function drawCrop(e: PointerEvent<HTMLDivElement>) {
    if (!drag.current || busy || published) return
    const start = drag.current
    const end = point(e)
    const left = Math.min(start.x, end.x), right = Math.max(start.x, end.x)
    const top = Math.min(start.y, end.y), bottom = Math.max(start.y, end.y)
    if (right - left >= 2 && bottom - top >= 2) updateCrop({ left, top, right, bottom })
  }

  async function prepare() {
    if (!file || !state || busy) return
    setBusy("prepare"); setError(""); setPreview(null); setConfirmed(false)
    const controller = new AbortController()
    operation.current = controller
    const body = new FormData()
    body.append("image", file); body.append("mode", mode)
    if (isCropped(cropRect)) body.append("crop", JSON.stringify({ left: cropRect.left / 100, top: cropRect.top / 100, right: cropRect.right / 100, bottom: cropRect.bottom / 100 }))
    try { setPreview(await request<Preview>(product.id, "/prepare", body, controller.signal)) }
    catch (e) { if (!controller.signal.aborted) setError(e instanceof Error ? e.message : "Error al preparar.") }
    finally { if (!controller.signal.aborted) setBusy(null) }
  }

  async function publish() {
    if (!preview?.preview_token || !preview.writes_enabled || !confirmed || busy || published) return
    setBusy("commit"); setError("")
    const body = new FormData()
    body.append("image", webpBlob(preview.data_base64), preview.filename)
    body.append("preview_token", preview.preview_token)
    try {
      const result = await request<Published>(product.id, "/commit", body)
      setPublished(result)
      onPublished?.()
    }
    catch (e) {
      setError((e instanceof Error ? e.message : "Error al publicar.") + " Comprueba el estado antes de reintentar.")
      setPreview(null); setConfirmed(false)
    } finally { setBusy(null) }
  }

  const label = [product.product_name, product.variant_name].filter((x, i, arr) => x && arr.indexOf(x) === i).join(" · ")
  const frame = "flex h-48 items-center justify-center overflow-hidden rounded-xl border bg-[repeating-conic-gradient(#e5e7eb_0%_25%,#fff_0%_50%)] bg-[length:20px_20px]"
  return (
    <Dialog open onOpenChange={open => { if (!open && busy !== "commit") onClose() }}>
      <DialogContent className="sm:max-w-4xl max-h-[92dvh] gap-4 overflow-y-auto overscroll-contain" showCloseButton={busy !== "commit"}>
        <DialogHeader>
          <div className="text-xs font-semibold uppercase tracking-widest text-muted-foreground">Maestro de productos / Imágenes</div>
          <DialogTitle className="text-xl">Gestionar fotografía</DialogTitle>
          <DialogDescription>{product.product_type ? product.product_type + " · " : ""}{label || "Producto"} · Código {product.barcode}{product.sku ? ` · SKU ${product.sku}` : ""}</DialogDescription>
        </DialogHeader>
        {error ? <div role="alert" className="rounded-lg border border-destructive/30 bg-destructive/10 p-3 text-sm text-destructive">{error}</div> : null}
        {busy === "loading" ? <div className="flex items-center gap-2 p-4 text-sm"><Loader2 className="size-4 animate-spin" /> Consultando fotografía y variante…</div> : null}
        {!state && !busy ? <Button variant="outline" onClick={() => setRefresh(x => x + 1)}>Volver a consultar</Button> : null}
        {state ? <>
          <div className="flex flex-wrap items-center justify-between gap-3 rounded-xl border bg-muted/30 p-3">
            <div className="flex items-center gap-3">
              <div className="flex size-14 shrink-0 items-center justify-center overflow-hidden rounded-lg border bg-white text-slate-500">
                {state.has_photo && !existingBroken ? <img className="h-full w-full object-contain" src={state.catalog_asset_url} alt="Foto publicada en catálogo" onError={() => setExistingBroken(true)} /> : <ImagePlus className="size-6" />}
              </div>
              <div><p className="text-sm font-semibold">{!state.github_checked ? "Estado de GitHub sin verificar" : state.has_photo ? "Foto existente en GitHub" : "Sin archivo en GitHub"}</p><p className="break-all text-xs text-muted-foreground">{state.filename}</p><p className="text-xs text-muted-foreground">Variante catálogo: {state.catalog_variant_ids.join(", ") || "no verificada"}</p><p className="text-xs text-muted-foreground">{existingBroken ? "El archivo no está visible en el catálogo actual." : "Miniatura del sitio publicado (puede estar desactualizada)."}</p></div>
            </div>
            <Button type="button" variant="ghost" size="sm" disabled={!!busy} onClick={() => setRefresh(x => x + 1)}>Actualizar estado</Button>
          </div>
          {state.github_error ? <p role="status" className="rounded-lg border bg-muted/40 p-3 text-sm">No se pudo consultar GitHub. Puedes preparar la foto; no podrás publicarla hasta verificar su estado.</p> : null}
          {state.blocked_reason ? <p className="rounded-lg border border-amber-500/30 bg-amber-500/10 p-3 text-sm">{state.blocked_reason}</p> : null}
          {!state.writes_enabled ? <p className="rounded-lg border bg-muted/40 p-3 text-sm">Modo previsualización. La publicación requiere habilitación de gerencia y configuración del servidor.</p> : null}
          <input ref={picker} type="file" accept="image/jpeg,image/png,image/webp,image/heic,image/heif,.jpg,.jpeg,.jfif,.png,.webp,.heic,.heif" className="sr-only" onChange={e => selectFile(e.target.files?.[0] || null)} disabled={!!busy || !!published} />
          <div className="flex flex-wrap items-center gap-3 rounded-xl border border-dashed p-3">
            <Button variant="outline" type="button" onClick={() => { if (picker.current) { picker.current.value = ""; picker.current.click() } }} disabled={!!busy || !!published}><Upload className="mr-2 size-4" /> Seleccionar foto</Button>
            <div className="min-w-0"><p className="break-all text-sm font-medium">{file?.name || "JPG, PNG, WebP o HEIC"}</p><p className="text-xs text-muted-foreground">Máximo 15 MB · hasta 32 megapíxeles</p></div>
          </div>
          <div className="space-y-3 rounded-xl border p-3">
            <div className="flex flex-wrap items-center justify-between gap-2">
              <div><p className="text-sm font-semibold">Encuadre del producto</p><p className="text-xs text-muted-foreground">{isCropped(cropRect) ? "Recorte manual activo · se conservará solo el área seleccionada." : "Elimina bordes, textos y elementos laterales seleccionando solo el producto."}</p></div>
              <Button type="button" variant="outline" size="sm" disabled={!file || !!busy || !!published} onClick={() => cropOpen ? setCropOpen(false) : void openCrop()}>{busy === "source" ? <Loader2 className="mr-2 size-4 animate-spin" /> : <Crop className="mr-2 size-4" />}{cropOpen ? "Ocultar recorte" : "Recortar manualmente"}</Button>
            </div>
            {cropOpen && cropSource ? <div className="grid gap-3 sm:grid-cols-[minmax(0,1fr)_220px]">
              <div className="flex min-h-48 items-center justify-center overflow-hidden rounded-xl border bg-[repeating-conic-gradient(#e5e7eb_0%_25%,#fff_0%_50%)] bg-[length:20px_20px] p-2">
                <div className="relative inline-flex max-w-full">
                  <img src={cropSource} alt="Imagen completa para seleccionar el recorte" draggable={false} className="block h-auto max-h-[360px] w-auto max-w-full select-none object-contain" onLoad={() => setCropLoaded(true)} onError={() => { setCropLoaded(false); setError("No se pudo mostrar la imagen para recortarla.") }} />
                  {cropLoaded ? <div className={`absolute inset-0 overflow-hidden ${busy || published ? "" : "cursor-crosshair touch-none"}`} onPointerDown={e => { if (busy || published || e.button !== 0) return; e.preventDefault(); drag.current = point(e); e.currentTarget.setPointerCapture(e.pointerId) }} onPointerMove={drawCrop} onPointerUp={e => { drawCrop(e); drag.current = null; if (e.currentTarget.hasPointerCapture(e.pointerId)) e.currentTarget.releasePointerCapture(e.pointerId) }} onPointerCancel={() => { drag.current = null }} onLostPointerCapture={() => { drag.current = null }}>
                    <div className="pointer-events-none absolute border-2 border-white shadow-[0_0_0_999px_rgba(0,0,0,0.60)]" style={{ left: cropRect.left + "%", top: cropRect.top + "%", width: cropRect.right - cropRect.left + "%", height: cropRect.bottom - cropRect.top + "%" }}>
                      <span className="absolute inset-0 grid grid-cols-3 grid-rows-3">{Array.from({ length: 9 }, (_, i) => <span key={i} className="border border-white/25" />)}</span>
                    </div>
                  </div> : null}
                </div>
              </div>
              <div className="space-y-2"><p className="text-xs text-muted-foreground">Arrastra un rectángulo sobre la botella o ajusta sus cuatro bordes con precisión.</p>
                {cropEdges.map(({ key, label }) => { const [min, max] = cropLimits(cropRect, key); return <label key={key} className="block space-y-1 text-xs"><span className="flex justify-between"><span>{label}</span><span className="tabular-nums">{cropRect[key]}%</span></span><input type="range" min={min} max={max} step={1} value={cropRect[key]} disabled={!!busy || !!published || !cropLoaded} onChange={e => updateCrop({ ...cropRect, [key]: Number(e.target.value) })} className="w-full accent-primary" /></label> })}
                <Button type="button" variant="outline" size="sm" className="w-full" disabled={!!busy || !!published} onClick={() => updateCrop(FULL_CROP)}>Restablecer encuadre</Button>
                <Button type="button" size="sm" className="w-full" disabled={!!busy || !!published} onClick={() => setCropOpen(false)}>Usar este recorte</Button>
              </div>
            </div> : null}
            {isCropped(cropRect) && !cropOpen ? <Button type="button" variant="ghost" size="sm" disabled={!!busy || !!published} onClick={() => updateCrop(FULL_CROP)}>Quitar recorte</Button> : null}
          </div>
          <div className="grid grid-cols-2 gap-2 sm:grid-cols-4">
            {modes.map(option => <button type="button" key={option.value} disabled={!!busy || !!published || (!!option.ai && !state.ai_enabled)} aria-pressed={mode === option.value}
              onClick={() => { setMode(option.value); setPreview(null); setConfirmed(false) }}
              className={`min-h-20 rounded-xl border p-3 text-left text-sm transition-colors disabled:opacity-45 ${mode === option.value ? "border-primary bg-primary/10 ring-1 ring-primary" : "bg-card hover:bg-muted"}`}>
              <span className="block font-semibold">{option.label}</span><span className="mt-1 block text-xs text-muted-foreground">{option.ai && !state.ai_enabled ? "IA no habilitada" : option.detail}</span></button>)}
          </div>
          <div className="grid gap-3 sm:grid-cols-2">
            <div className="space-y-2"><p className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">Original</p><div className={frame}>
              {original && !originalBroken ? <img src={original} className="h-full w-full object-contain" alt="Fotografía original" onError={() => setOriginalBroken(true)} /> : <p className="p-3 text-center text-sm text-slate-600">{file ? "Vista local no compatible; el servidor puede convertirla." : "Selecciona una foto"}</p>}
            </div></div>
            <div className="space-y-2"><p className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">Resultado para catálogo {isCropped(cropRect) ? "· Recortado" : ""}</p><div className={frame}>
              {preview ? <img src={`data:image/webp;base64,${preview.data_base64}`} className="h-full w-full object-contain" alt="Resultado WebP que se publicará" /> : <p className="p-3 text-center text-sm text-slate-600">Aquí verás el resultado procesado</p>}
            </div></div>
          </div>
          {preview ? <div className="flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted-foreground"><span>{preview.width} × {preview.height} px</span><span>Encuadre centrado · 90% máximo</span><span>WebP · {Math.ceil(preview.bytes / 1024)} KB</span><span className="break-all font-mono">{preview.filename}</span></div> : null}
          {!published ? <Button type="button" variant="outline" disabled={!file || !!busy} onClick={() => void prepare()}>{busy === "prepare" ? <Loader2 className="mr-2 size-4 animate-spin" /> : <WandSparkles className="mr-2 size-4" />} Preparar previsualización</Button> : null}
          {preview && !preview.writes_enabled && preview.publication_reason ? <p role="status" className="rounded-lg border bg-muted/40 p-3 text-sm">Previsualización disponible. Publicación bloqueada: {preview.publication_reason}</p> : null}
          {preview?.preview_token && !published ? <div className="space-y-3 rounded-xl border p-3">
            <label className="flex items-start gap-2 text-sm"><input type="checkbox" checked={confirmed} disabled={!!busy} onChange={e => setConfirmed(e.target.checked)} className="mt-1 size-4" /><span>Revisé el resultado y confirmo {preview.expected_sha ? "reemplazar la foto existente" : "subir una nueva foto"} de este producto.</span></label>
            <Button className="w-full" disabled={!confirmed || !!busy} onClick={() => void publish()}>{busy === "commit" ? <Loader2 className="mr-2 size-4 animate-spin" /> : <Upload className="mr-2 size-4" />} {preview.expected_sha ? "Confirmar reemplazo en GitHub" : "Confirmar publicación en GitHub"}</Button>
          </div> : null}
          {published ? <div role="status" className="space-y-2 rounded-xl border border-emerald-500/30 bg-emerald-500/10 p-4">
            <p className="flex items-center gap-2 font-semibold"><CheckCircle2 className="size-5 text-emerald-600" /> Archivo guardado en GitHub</p>
            {published.audit_pending ? <p role="alert" className="text-sm font-medium">GitHub confirmó la foto; falta reconciliar la auditoría interna. No vuelvas a publicarla. Referencia {published.audit_id}</p> : null}
            <p className="text-sm">La foto todavía debe verificarse en el catálogo publicado. Los archivos estáticos pueden requerir un nuevo despliegue y actualización de caché.</p>
            <div className="flex flex-wrap gap-4 text-sm"><a className="underline inline-flex items-center gap-1" href={published.commit_url} target="_blank" rel="noreferrer">Ver cambio <ExternalLink className="size-3" /></a><a className="underline inline-flex items-center gap-1" href={published.catalog_asset_url + "?v=" + published.new_sha} target="_blank" rel="noreferrer">Comprobar foto publicada <ExternalLink className="size-3" /></a></div>
          </div> : null}
          <p className="text-xs text-muted-foreground">No se modifican precios, stock, cantidades ni fotos de otros productos. Si el recorte no es correcto, elige conservar el original.</p>
        </> : null}
      </DialogContent>
    </Dialog>
  )
}
