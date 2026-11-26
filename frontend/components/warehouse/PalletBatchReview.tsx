"use client"

import { useEffect, useRef, useState } from "react"
import { AlertTriangle, ArrowLeft, CheckCircle2, Layers, Loader2, Plus, X } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { registerWarehousePalletBatch, type PalletBatchInput, type PalletBatchResult, type PalletLocations } from "@/lib/warehouse/api"
import { WarehouseApiError } from "@/lib/warehouse/types"
import { validatePalletContents, type Line, type PalletKind } from "@/lib/warehouse/palletForms"
import { MAX_BATCH_LINES, MAX_BATCH_PALLETS, batchSelectable, slotsAtLevel } from "@/lib/warehouse/palletMap"
import type { BatchLocationForm } from "@/components/warehouse/PalletLocationMap"

type Common = {kind:PalletKind;received:string;notes:string;lines:Line[]}
type Row = Common & {id:number;code:string}
const select="h-9 w-full rounded-md border border-input bg-card px-2 text-xs text-foreground"
const err=(e:unknown)=>e instanceof Error?e.message:"Error inesperado"
const kindLabels={complete:"Completo",partial:"Molido",mixed:"Mixto"}
const dateText=(s:string|null)=>s?s.slice(0,10).split("-").reverse().join("/"):"Pendiente"

export function PalletBatchReview({floorId,locations,selection,common,onBack,onFinished}:{
  floorId:number;locations:PalletLocations;selection:BatchLocationForm;common:Common
  onBack:()=>void;onFinished:(result:PalletBatchResult)=>void
}){
  const [rows,setRows]=useState<Row[]>(()=>selection.ids.map(id=>({
    id,code:locations.items.find(p=>p.id===id)?.code??String(id),
    kind:common.kind,received:common.received,notes:common.notes,
    lines:common.lines.map(l=>({...l,key:crypto.randomUUID()}))
  })).sort((a,b)=>a.code.localeCompare(b.code,"es",{numeric:true})))
  const [confirmed,setConfirmed]=useState(false)
  const [busy,setBusy]=useState(false)
  const [error,setError]=useState("")
  const [uncertain,setUncertain]=useState(false)
  const [receipt,setReceipt]=useState<PalletBatchResult|null>(null)
  const inFlight=useRef(false)
  // Después de una respuesta perdida, congelar el cuerpo completo y conservar
  // su UUID. Nunca crear otra clave automáticamente durante el reintento.
  const frozen=useRef<PalletBatchInput|null>(null)
  useEffect(()=>{
    if(!uncertain&&!busy)return
    const handler=(e:BeforeUnloadEvent)=>{e.preventDefault();e.returnValue=""}
    window.addEventListener("beforeunload",handler)
    return()=>window.removeEventListener("beforeunload",handler)
  },[uncertain,busy])
  const changeRow=(id:number,patch:Partial<Common>)=>{
    setConfirmed(false);setRows(old=>old.map(r=>r.id===id?{...r,...patch}:r))
  }
  const changeLine=(id:number,key:string,patch:Partial<Line>)=>{
    setConfirmed(false);setRows(old=>old.map(r=>r.id===id?{...r,lines:r.lines.map(l=>l.key===key?{...l,...patch}:l)}:r))
  }
  const removeLine=(id:number,key:string)=>{
    setConfirmed(false);setRows(old=>old.map(r=>r.id===id?{...r,lines:r.lines.filter(l=>l.key!==key)}:r))
  }
  const totalLines=rows.reduce((n,r)=>n+r.lines.length,0)
  const addLot=(id:number,source:Line)=>{
    if(totalLines>=MAX_BATCH_LINES)return
    setConfirmed(false)
    setRows(old=>old.map(r=>r.id===id&&r.lines.length<100?{...r,lines:[...r.lines,{
      ...source,key:crypto.randomUUID(),verified:false,boxes:"0",units:"0",expiry:"unknown",expires:"",lot:""
    }]}:r))
  }
  const pendingQty=rows.filter(r=>r.lines.some(l=>!l.verified)).length
  const pendingExpiry=rows.filter(r=>r.lines.some(l=>l.expiry==="unknown")).length
  const pendingLot=rows.filter(r=>r.lines.some(l=>!l.lot.trim())).length
  const back=()=>{
    if(busy||uncertain)return
    if(window.confirm("¿Volver a los datos comunes? Los cambios individuales de esta revisión se descartarán. Aún no se ha registrado ningún pallet."))onBack()
  }
  const submit=async()=>{
    if(inFlight.current||receipt)return
    setError("")
    try{
      if(!frozen.current){
        if(!confirmed)throw new Error("Confirma la observación física de todos los pallets.")
        if(!locations.revision||rows.length<2||rows.length>MAX_BATCH_PALLETS||totalLines>MAX_BATCH_LINES)throw new Error("Selección o número de líneas inválido.")
        const level=Number(selection.level)
        if(!Number.isInteger(level)||level<1||level>100)throw new Error("Nivel inválido.")
        const slots=slotsAtLevel(locations.observed_slots,level)
        const items=rows.map(r=>{
          const p=locations.items.find(p=>p.id===r.id)
          if(!p||!batchSelectable(p,slots))throw new Error(r.code+": posición ocupada o restringida.")
          try{
            return {position_id:r.id,pallet_kind:r.kind,received_on:r.received||null,notes:r.notes.trim()||null,lines:validatePalletContents(r.kind,r.lines)}
          }catch(e){throw new Error(r.code+": "+err(e))}
        })
        frozen.current={batch_key:crypto.randomUUID(),floor_id:floorId,layout_revision_id:locations.revision.id,
          layout_row_version:locations.revision.row_version,observed_level:level,observed_ack:true,items}
      }
      const payload=frozen.current
      inFlight.current=true;setBusy(true)
      const result=await registerWarehousePalletBatch(payload)
      if(result.batch_key!==payload.batch_key||result.items.length!==payload.items.length)throw new Error("Respuesta incompleta. Reintenta la misma operación.")
      setReceipt(result);setUncertain(false)
    }catch(e){
      setError(err(e))
      if(frozen.current){
        // 4xx explícito antes de una respuesta ambigua => rollback/no escritura.
        // Después de una respuesta ambigua no cambiar datos aunque falle el retry.
        if(!uncertain&&e instanceof WarehouseApiError&&[400,401,403,404,409,422].includes(e.status)&&e.code!=="batch_key_reused"){
          frozen.current=null;setConfirmed(false)
        }else setUncertain(true)
      }
    }finally{inFlight.current=false;setBusy(false)}
  }
  const locked=busy||uncertain
  return <Dialog open onOpenChange={v=>{if(!v){if(receipt)onFinished(receipt);else back()}}}>
    <DialogContent showCloseButton={!locked} className="flex max-h-[94vh] w-[97vw] flex-col gap-0 overflow-hidden p-0 sm:max-w-[1420px]">
      <DialogHeader className="shrink-0 border-b bg-card px-5 py-4 pr-14">
        <DialogTitle className="flex items-center gap-2">{receipt?<CheckCircle2 className="h-5 w-5 text-lq-success"/>:<Layers className="h-5 w-5 text-primary"/>}{receipt?"Registro masivo confirmado":"Revisar "+rows.length+" pallets"}</DialogTitle>
        <DialogDescription>{locations.floor.facility_code} · {locations.floor.floor_code} · Nivel {selection.level} · Plano v{locations.revision?.row_version} · Un ID PAL por posición</DialogDescription>
      </DialogHeader>
      <div className="min-h-0 flex-1 space-y-3 overflow-y-auto bg-background p-4">
        {receipt?<div className="space-y-3">
          <div className="rounded-xl border border-lq-success/30 bg-lq-success/10 p-4"><p className="text-xl font-semibold text-lq-success">{receipt.items.length} pallets {receipt.replayed?"recuperados":"registrados"}</p><p className="mt-1 text-xs text-muted-foreground">Operación {receipt.batch_key}{receipt.replayed?" · Reintento sin duplicados. Se muestran los datos actuales.":""}</p></div>
          <div className="overflow-x-auto rounded-xl border bg-card">
            <table className="w-full text-left text-xs"><thead className="bg-muted text-muted-foreground"><tr>{["Pallet","Ubicación actual","Productos","Vencimiento más próximo"].map(h=><th key={h} className="p-3 font-medium">{h}</th>)}</tr></thead>
              <tbody>{receipt.items.map(p=><tr key={p.id} className="border-t">
                <td className="p-3 font-semibold">{p.code}{p.status==="voided"?<span className="ml-1 text-destructive">Anulado</span>:null}</td>
                <td className="p-3">{p.position_code_snap??"Sin verificar"}{p.observed_level?" · N"+p.observed_level:""}</td>
                <td className="p-3">{p.lines.map(l=><p key={l.id}>{l.product_name_snap} · {l.variant_name_snap||"Variante sin nombre"} <span className="text-muted-foreground">({l.product_type_snap||"Sin tipo"})</span></p>)}</td>
                <td className="p-3">{p.lines.some(l=>l.expires_on)?dateText(p.lines.flatMap(l=>l.expires_on?[l.expires_on]:[]).sort()[0]):p.lines.some(l=>l.expiry_status==="unknown")?"Sin verificar":"No aplica"}{p.lines.some(l=>l.expiry_status==="unknown"&&p.lines.some(x=>x.expires_on))?<p className="text-lq-warning">Otras fechas pendientes</p>:null}</td>
              </tr>)}</tbody></table>
          </div>
        </div>:<>
          <div className="grid grid-cols-2 gap-2 md:grid-cols-4">
            {[["Pallets",rows.length],["Conteos pendientes",pendingQty],["Vencimientos pendientes",pendingExpiry],["Lotes pendientes",pendingLot]].map(([label,value])=><div className="rounded-lg border bg-card p-3" key={label}><p className="text-xs text-muted-foreground">{label}</p><p className="text-xl font-semibold tabular-nums">{value}</p></div>)}
          </div>
          <div className="rounded-lg border border-lq-warning/30 bg-lq-warning/10 p-3 text-xs text-lq-warning"><AlertTriangle className="mr-1 inline h-4 w-4"/>Los datos comunes son por pallet. Revisa las excepciones; los campos pendientes no son cero. No se autoriza apilamiento ni se modifica Bsale.</div>
          <fieldset disabled={locked} className="min-w-0 overflow-x-auto rounded-xl border bg-card">
            <table className="w-full min-w-[1130px] text-left text-xs">
              <thead className="bg-muted text-muted-foreground"><tr>{["Ubicación / notas","Tipo / ingreso","Producto y variante","Cantidad por pallet","Lote","Vencimiento","Líneas"].map(h=><th key={h} className="p-3 font-medium">{h}</th>)}</tr></thead>
              {rows.map(r=><tbody key={r.id} className="border-t">
                {r.lines.map((l,i)=><tr key={l.key} className="align-top">
                  {i===0?<td rowSpan={r.lines.length} className="w-32 border-r p-2"><p className="font-mono text-sm font-semibold">{r.code} · N{selection.level}</p>
                    <textarea aria-label={"Observaciones "+r.code} rows={2} maxLength={2000} placeholder="Notas" value={r.notes} onChange={e=>changeRow(r.id,{notes:e.target.value})} className="mt-2 w-full rounded-md border border-input bg-card p-1.5 text-xs"/>
                  </td>:null}
                  {i===0?<td rowSpan={r.lines.length} className="w-36 border-r p-2">
                    <label className="block space-y-1"><span className="sr-only">Tipo {r.code}</span><select className={select} value={r.kind} onChange={e=>changeRow(r.id,{kind:e.target.value as PalletKind})}>{Object.entries(kindLabels).map(([k,v])=><option key={k} value={k}>{v}</option>)}</select></label>
                    <label className="mt-2 block space-y-1 text-muted-foreground">Ingreso <Input aria-label={"Ingreso "+r.code} type="date" value={r.received} onChange={e=>changeRow(r.id,{received:e.target.value})}/></label>
                  </td>:null}
                  <td className="min-w-[180px] p-2"><p className="font-semibold">{l.product.product_name} · {l.product.variant_name||"Variante sin nombre"}</p><p className="text-[11px] text-muted-foreground">{l.product.product_type||"Sin tipo"} · {l.product.barcode||l.product.sku||"ID "+l.product.variant_id}</p></td>
                  <td className="min-w-[200px] p-2"><label className="flex items-center gap-1.5"><input type="checkbox" checked={l.verified} onChange={e=>changeLine(r.id,l.key,{verified:e.target.checked})}/> Conteo verificado</label>
                    {l.verified?<div className="mt-1 grid grid-cols-2 gap-1.5"><label>Cajas {l.product.units_per_box?"×"+l.product.units_per_box:"(sin conversión)"}<Input inputMode="numeric" aria-label={"Cajas "+r.code+" línea "+(i+1)} disabled={!l.product.units_per_box} value={l.boxes} onChange={e=>changeLine(r.id,l.key,{boxes:e.target.value})}/></label><label>Sueltas<Input inputMode="numeric" aria-label={"Unidades sueltas "+r.code+" línea "+(i+1)} value={l.units} onChange={e=>changeLine(r.id,l.key,{units:e.target.value})}/></label></div>:<p className="mt-1 text-lq-warning">Pendiente</p>}</td>
                  <td className="min-w-[125px] p-2"><Input aria-label={"Lote "+r.code+" línea "+(i+1)} maxLength={200} placeholder="Sin verificar" value={l.lot} onChange={e=>changeLine(r.id,l.key,{lot:e.target.value})}/></td>
                  <td className="min-w-[160px] p-2"><select aria-label={"Estado vencimiento "+r.code+" línea "+(i+1)} className={select} value={l.expiry} onChange={e=>changeLine(r.id,l.key,{expiry:e.target.value as Line["expiry"]})}><option value="unknown">Sin verificar</option><option value="dated">Fecha verificada</option><option value="not_applicable">No aplica (confirmado)</option></select>
                    {l.expiry==="dated"?<Input className="mt-1" aria-label={"Vencimiento "+r.code+" línea "+(i+1)} type="date" value={l.expires} onChange={e=>changeLine(r.id,l.key,{expires:e.target.value})}/>:null}</td>
                  <td className="p-2"><div className="flex gap-1"><Button type="button" variant="outline" size="icon" title="Otro lote de esta variante" aria-label={"Añadir lote "+r.code} disabled={totalLines>=MAX_BATCH_LINES||r.lines.length>=100} onClick={()=>addLot(r.id,l)}><Plus className="h-4 w-4"/></Button><Button type="button" variant="ghost" size="icon" title="Quitar línea" aria-label={"Quitar línea "+r.code} disabled={r.lines.length<=1} onClick={()=>removeLine(r.id,l.key)}><X className="h-4 w-4"/></Button></div></td>
                </tr>)}
              </tbody>)}
            </table>
          </fieldset>
          <label className="flex items-start gap-2 rounded-xl border bg-card p-3 text-sm"><input type="checkbox" className="mt-1" disabled={locked} checked={confirmed} onChange={e=>setConfirmed(e.target.checked)}/><span><strong>Confirmo los {rows.length} pallets observados físicamente en las posiciones y nivel indicados.</strong> Revisé los datos comunes y las excepciones; lo desconocido se mantiene pendiente. El registro no es una autorización de almacenamiento ni un movimiento de stock.</span></label>
          {error?<div role="alert" className="rounded-lg border border-destructive/30 bg-destructive/10 p-3 text-sm text-destructive">{error}<p className="mt-1 text-xs">{uncertain?"Resultado incierto. Los datos y la clave quedaron congelados: reintenta exactamente la misma operación, sin iniciar otro registro.":"No se guardó el grupo. Corrige los datos o vuelve al mapa y actualiza las posiciones."}</p></div>:null}
        </>}
      </div>
      <DialogFooter className="shrink-0 border-t bg-card px-5 py-3">
        {receipt?<Button type="button" onClick={()=>onFinished(receipt)}>Cerrar y actualizar listado</Button>:<>
          <Button type="button" variant="outline" disabled={locked} onClick={back}><ArrowLeft className="mr-1 h-4 w-4"/>Volver a datos comunes</Button>
          <Button type="button" disabled={busy||(!confirmed&&!uncertain)} onClick={()=>void submit()}>{busy?<Loader2 className="mr-2 h-4 w-4 animate-spin"/>:null}{uncertain?"Reintentar misma operación":"Registrar "+rows.length+" pallets juntos"}</Button>
        </>}
      </DialogFooter>
    </DialogContent>
  </Dialog>
}
