"use client"

import { useCallback, useEffect, useRef, useState } from "react"
import { Camera, CameraOff, Loader2, QrCode, Search } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { getWarehousePallet, type PalletRecord } from "@/lib/warehouse/api"
import { formatLabelDate, palletIdFromCode } from "@/lib/warehouse/palletLabels"

type Controls={stop:()=>void}
const num=(n:number)=>n.toLocaleString("es-CL")
const kinds={complete:"Completo",partial:"Molido",mixed:"Mixto"}

export function PalletLookupDialog({onClose,onPrint}:{onClose:()=>void;onPrint:(p:PalletRecord)=>void}){
  const [code,setCode]=useState("")
  const [data,setData]=useState<PalletRecord|null>(null)
  const [loading,setLoading]=useState(false)
  const [error,setError]=useState("")
  const [cameraError,setCameraError]=useState("")
  const [camera,setCamera]=useState<"idle"|"starting"|"active">("idle")
  const videoRef=useRef<HTMLVideoElement|null>(null)
  const controlsRef=useRef<Controls|null>(null)
  const cameraEpoch=useRef(0)
  const requestEpoch=useRef(0)
  const alive=useRef(true)
  const stop=useCallback(()=>{
    cameraEpoch.current++
    try{controlsRef.current?.stop()}catch{/* release tracks below */}
    controlsRef.current=null
    const video=videoRef.current
    const stream=video?.srcObject as MediaStream|null|undefined
    stream?.getTracks().forEach(track=>track.stop())
    if(video)video.srcObject=null
    if(alive.current)setCamera("idle")
  },[])
  useEffect(()=>{
    alive.current=true
    return()=>{alive.current=false;requestEpoch.current++;stop()}
  },[stop])
  const lookup=useCallback(async(value:string)=>{
    const id=palletIdFromCode(value)
    if(id===null){setError("Ingresa un código PAL válido, por ejemplo PAL-000001.");return}
    const seq=++requestEpoch.current
    setCode(value.trim().toUpperCase());setData(null);setError("");setLoading(true)
    try{
      const p=await getWarehousePallet(id)
      if(p.id!==id||p.code!==value.trim().toUpperCase())throw new Error("La identidad no coincide.")
      if(alive.current&&seq===requestEpoch.current)setData(p)
    }catch(e){if(alive.current&&seq===requestEpoch.current)setError(e instanceof Error?e.message:"No se pudo consultar el pallet.")}
    finally{if(alive.current&&seq===requestEpoch.current)setLoading(false)}
  },[])
  const start=async()=>{
    const epoch=++cameraEpoch.current
    setCamera("starting");setCameraError("")
    try{
      if(!navigator.mediaDevices?.getUserMedia)throw new Error("Cámara no disponible. Usa un lector externo o escribe el código.")
      const {BrowserQRCodeReader}=await import("@zxing/browser")
      if(!alive.current||epoch!==cameraEpoch.current||!videoRef.current)return
      const reader=new BrowserQRCodeReader()
      const controls=await reader.decodeFromConstraints({video:{facingMode:{ideal:"environment"}},audio:false},videoRef.current,(result)=>{
        if(!result||!alive.current||epoch!==cameraEpoch.current)return
        const value=result.getText()
        if(palletIdFromCode(value)===null){setCameraError("El QR no corresponde a un código PAL de bodega.");return}
        stop();setCameraError("");void lookup(value)
      })
      if(!alive.current||epoch!==cameraEpoch.current){controls.stop();return}
      controlsRef.current=controls
      setCamera("active")
    }catch(e){
      if(!alive.current||epoch!==cameraEpoch.current)return
      stop();setCameraError(e instanceof Error?e.message:"No se pudo iniciar la cámara.")
    }
  }
  return <Dialog open onOpenChange={v=>{if(!v)onClose()}}>
    <DialogContent className="flex max-h-[94vh] w-[96vw] flex-col gap-0 overflow-hidden p-0 sm:max-w-[820px]">
      <DialogHeader className="border-b bg-card px-5 py-4 pr-14">
        <DialogTitle className="flex items-center gap-2"><QrCode className="h-5 w-5 text-primary"/>Consultar pallet por QR</DialogTitle>
        <DialogDescription>Lee el identificador permanente o escribe su código. La consulta requiere acceso ERP.</DialogDescription>
      </DialogHeader>
      <div className="min-h-0 flex-1 space-y-3 overflow-y-auto bg-background p-4">
        <form className="flex flex-wrap gap-2" onSubmit={e=>{e.preventDefault();stop();void lookup(code)}}>
          <Input className="min-w-[180px] flex-1 font-mono" aria-label="Código PAL" value={code} maxLength={20} autoComplete="off"
            placeholder="PAL-000001" onChange={e=>{setCode(e.target.value);requestEpoch.current++;setData(null);setLoading(false);setError("")}}/>
          <Button type="submit" disabled={loading||!code.trim()}><Search className="mr-1 h-4 w-4"/>Consultar</Button>
        </form>
        <section className="space-y-2 rounded-xl border bg-card p-3">
          <div className="flex items-center justify-between gap-2">
            <p className="text-xs text-muted-foreground">Cámara del dispositivo</p>
            {camera==="idle"?<Button type="button" variant="outline" size="sm" onClick={()=>void start()}><Camera className="mr-1 h-4 w-4"/>Escanear QR</Button>:
              <Button type="button" variant="outline" size="sm" onClick={stop}><CameraOff className="mr-1 h-4 w-4"/>{camera==="starting"?"Cancelar inicio":"Detener cámara"}</Button>}
          </div>
          <video ref={videoRef} muted playsInline aria-label="Cámara para lectura de QR" className={camera==="idle"?"hidden":"mx-auto aspect-video max-h-56 w-full rounded-lg bg-black object-contain"}/>
          {cameraError?<p role="alert" className="text-xs text-lq-warning">{cameraError}</p>:null}
        </section>
        {loading?<p className="flex items-center gap-2 text-sm text-muted-foreground"><Loader2 className="h-4 w-4 animate-spin"/>Consultando ficha…</p>:null}
        {error?<p role="alert" className="rounded-lg border border-destructive/30 bg-destructive/10 p-3 text-sm text-destructive">{error}</p>:null}
        {data?<section className="space-y-3 rounded-xl border bg-card p-4">
          <div className="flex flex-wrap items-start justify-between gap-2"><div><h3 className="text-xl font-semibold">{data.code}</h3><p className="text-xs text-muted-foreground">{kinds[data.pallet_kind]} · revisión de datos {data.row_version}</p></div>
            {data.status==="voided"?<span className="rounded border border-destructive/30 bg-destructive/10 px-2 py-1 text-sm text-destructive">ANULADO · no usar</span>:<span className="text-xs text-lq-success">Activo</span>}</div>
          <p className="text-sm">Última ubicación observada: <strong>{data.position_code_snap??"Sin verificar"}{data.observed_level?" · N"+data.observed_level:""}</strong></p>
          <p className="text-xs text-muted-foreground">Ingreso: {formatLabelDate(data.received_on)}</p>
          {data.lines.map(l=><div className="space-y-1 rounded-lg border bg-background p-3" key={l.id}>
            <p className="text-sm font-semibold">{l.product_name_snap} · {l.variant_name_snap||"Variante sin nombre"}</p>
            <p className="text-xs text-muted-foreground">{l.product_type_snap||"Sin tipo"} · {l.barcode_snap||l.sku_snap||"ID "+l.variant_id}</p>
            <p className="text-xs">{l.total_units===null?"Cantidad pendiente":num(l.total_units)+" un. · "+num(l.boxes??0)+" cajas + "+num(l.loose_units??0)+" sueltas"}</p>
            <p className="text-xs">Lote: {l.lot||"Sin verificar"} · Vence: {l.expiry_status==="not_applicable"?"No aplica":formatLabelDate(l.expires_on)}</p>
          </div>)}
          {data.notes?<p className="text-xs text-muted-foreground">{data.notes}</p>:null}
        </section>:null}
      </div>
      <DialogFooter className="shrink-0 border-t bg-card px-5 py-3">
        {data?.status==="active"?<Button type="button" variant="outline" onClick={()=>{stop();onPrint(data)}}>Reimprimir etiqueta</Button>:null}
        <Button type="button" onClick={onClose}>Cerrar</Button>
      </DialogFooter>
    </DialogContent>
  </Dialog>
}
