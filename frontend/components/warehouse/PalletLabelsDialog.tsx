"use client"

import { useEffect, useState } from "react"
import { Download, Loader2, Printer, RefreshCw, Tags } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { previewPalletLabels, type PalletLabelBatch } from "@/lib/warehouse/api"
import { LABEL_FORMATS, labelPageLayout, MAX_LABEL_PALLETS, type LabelPerSheet } from "@/lib/warehouse/palletLabels"

type Preview = {url:string;format:LabelPerSheet;batch:PalletLabelBatch}
const message=(e:unknown)=>e instanceof Error?e.message:"No se pudo preparar las etiquetas."

export function PalletLabelsDialog({floorId,ids,onClose}:{floorId:number;ids:number[];onClose:()=>void}){
  const key=ids.join(",")
  const [format,setFormat]=useState<LabelPerSheet>(2)
  const [batch,setBatch]=useState<PalletLabelBatch|null>(null)
  const [preview,setPreview]=useState<Preview|null>(null)
  const [refresh,setRefresh]=useState(0)
  const [error,setError]=useState("")
  const [pdfError,setPdfError]=useState("")
  const [loading,setLoading]=useState(true)
  useEffect(()=>{
    let cancelled=false
    setBatch(null);setError("");setLoading(true)
    const requested=key?key.split(",").map(Number):[]
    if(!requested.length||requested.length>MAX_LABEL_PALLETS||new Set(requested).size!==requested.length){
      setError("Seleccione entre 1 y "+MAX_LABEL_PALLETS+" pallets distintos.");setLoading(false);return
    }
    previewPalletLabels(floorId,requested).then(data=>{
      if(cancelled)return
      if(data.items.length!==requested.length||data.items.some((p,i)=>p.id!==requested[i]))throw new Error("La respuesta no coincide con la selección.")
      setBatch(data)
    }).catch(e=>{if(!cancelled)setError(message(e))}).finally(()=>{if(!cancelled)setLoading(false)})
    return()=>{cancelled=true}
  },[key,floorId,refresh])
  useEffect(()=>{
    let cancelled=false,url:string|undefined
    setPreview(null);setPdfError("")
    if(batch){
      import("@/lib/warehouse/palletLabelsPdf").then(m=>m.createPalletLabelsPdf(batch,format)).then(doc=>{
        if(cancelled)return
        url=URL.createObjectURL(doc.output("blob"))
        setPreview({url,format,batch})
      }).catch(e=>{if(!cancelled)setPdfError(message(e))})
    }
    return()=>{cancelled=true;if(url)URL.revokeObjectURL(url)}
  },[batch,format])
  const ready=preview?.batch===batch&&preview?.format===format?preview:null
  const count=batch?.items.length??ids.length
  const layout=labelPageLayout(format)
  const dimension=(n:number)=>n.toLocaleString("es-CL",{maximumFractionDigits:1})
  return <Dialog open onOpenChange={open=>{if(!open)onClose()}}>
    <DialogContent className="flex max-h-[94vh] w-[96vw] flex-col gap-0 overflow-hidden p-0 sm:max-w-[1200px]">
      <DialogHeader className="shrink-0 border-b bg-card px-5 py-4 pr-14">
        <DialogTitle className="flex items-center gap-2"><Tags className="h-5 w-5 text-primary"/>Etiquetas de pallets</DialogTitle>
        <DialogDescription>QR permanente · {count} identidades · Sin ubicación impresa</DialogDescription>
      </DialogHeader>
      <div className="grid min-h-0 flex-1 gap-3 overflow-y-auto bg-background p-4 lg:grid-cols-[280px_minmax(0,1fr)]">
        <aside className="space-y-3">
          <section className="space-y-2 rounded-xl border bg-card p-3">
            <p className="text-xs font-medium text-muted-foreground">FORMATO A4</p>
            {LABEL_FORMATS.map(n=><Button key={n} type="button" className="w-full justify-between" variant={format===n?"default":"outline"}
              aria-pressed={format===n} onClick={()=>setFormat(n)}>{n===1?"1 etiqueta · Grande":n===2?"2 etiquetas · Estándar":"4 etiquetas · Compacto"}<span className="text-xs">{n}/hoja</span></Button>)}
            <div className="grid grid-cols-2 gap-2 rounded-lg border bg-muted p-2">
              <div><p className="text-xs text-muted-foreground">Hojas</p><p className="text-lg font-semibold tabular-nums">{Math.ceil(count/format)}</p></div>
              <div><p className="text-xs text-muted-foreground">Etiquetas</p><p className="text-lg font-semibold tabular-nums">{count}</p></div>
            </div>
            <p className="text-xs text-muted-foreground">{dimension(layout.width)} × {dimension(layout.height)} mm por etiqueta. Papel blanco, tinta negra y borde para recorte.</p>
          </section>
          <section className="space-y-2 rounded-xl border bg-card p-3">
            <p className="text-xs font-medium">Datos de impresión</p>
            <p className="text-xs text-muted-foreground">{batch?"Actualizados "+new Date(batch.generated_at).toLocaleString("es-CL",{timeZone:"America/Santiago"}):"Cargando ficha actual."}</p>
            <Button type="button" variant="outline" size="sm" className="w-full" disabled={loading} onClick={()=>setRefresh(x=>x+1)}><RefreshCw className="mr-1 h-4 w-4"/>Actualizar datos</Button>
            <p className="text-xs text-muted-foreground">Cada QR contiene sólo el código PAL. Reimprime si corrigieron los productos, cantidades, lotes o vencimientos; los traslados no requieren una nueva etiqueta.</p>
          </section>
          {batch?<section className="space-y-1 rounded-xl border bg-card p-3">
            <p className="text-xs font-medium">Identidades incluidas</p>
            <div className="max-h-32 overflow-y-auto text-xs text-muted-foreground">{batch.items.map(p=><p key={p.id} className="py-0.5 font-mono">{p.code}</p>)}</div>
          </section>:null}
          <p className="rounded-lg border border-lq-warning/30 bg-lq-warning/10 p-3 text-xs text-lq-warning">En el visor PDF, imprime A4 a tamaño real (100%). No pegues etiquetas de pallets anulados ni duplicadas en otro pallet.</p>
        </aside>
        <section className="min-h-[50vh] min-w-0 overflow-hidden rounded-xl border bg-muted">
          {error||pdfError?<p role="alert" className="p-4 text-sm text-destructive">{error||pdfError}</p>:
            ready?<iframe title="Vista previa PDF de etiquetas A4" src={ready.url} className="h-[61vh] min-h-[360px] w-full bg-white"/>:
            <div className="flex min-h-[48vh] items-center justify-center gap-2 text-sm text-muted-foreground"><Loader2 className="h-5 w-5 animate-spin"/>{loading?"Leyendo pallets activos…":"Preparando PDF y QR…"}</div>}
        </section>
      </div>
      <DialogFooter className="shrink-0 flex-wrap border-t bg-card px-5 py-3">
        <Button type="button" variant="outline" onClick={onClose}>Cerrar</Button>
        {ready?<><Button asChild variant="outline"><a href={ready.url} download="etiquetas-pallets-A4.pdf"><Download className="mr-1 h-4 w-4"/>Descargar PDF</a></Button>
          <Button asChild><a href={ready.url} target="_blank" rel="noopener noreferrer"><Printer className="mr-1 h-4 w-4"/>Abrir PDF e imprimir</a></Button></>:
          <Button type="button" disabled><Printer className="mr-1 h-4 w-4"/>Abrir PDF e imprimir</Button>}
      </DialogFooter>
    </DialogContent>
  </Dialog>
}
