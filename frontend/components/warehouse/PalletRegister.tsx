"use client"

import { useCallback, useEffect, useRef, useState } from "react"
import { Box, ClipboardList, Loader2, Plus, Search, X, ArrowLeftRight, AlertTriangle, History, MapPin, Pencil } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import {
  listWarehouseFacilities, listWarehouseFloors, listPalletLocations, listWarehousePallets,
  searchPalletProducts, registerWarehousePallet, getWarehousePallet, observeWarehousePalletLocation,
  voidWarehousePallet, updateWarehousePalletDetails,
  type PalletProduct, type PalletLocations, type PalletRecord, type PalletLineInput, type ObservedLocationInput,
} from "@/lib/warehouse/api"
import { PalletLocationMap, type ObservedLocationForm } from "@/components/warehouse/PalletLocationMap"
import type { WarehouseFacility, WarehouseFloor } from "@/lib/warehouse/types"

const kindLabels = {complete:"Completo",partial:"Molido",mixed:"Mixto"} as const
const num = (n: number) => n.toLocaleString("es-CL")
const err = (e: unknown) => e instanceof Error ? e.message : "Error inesperado"
const dateText = (s: string | null) => s ? s.slice(0,10).split("-").reverse().join("/") : "Sin fecha verificada"
const selectClass = "h-9 w-full rounded-md border border-input bg-card px-2 text-sm text-foreground"
const textAreaClass = "w-full rounded-md border border-input bg-card p-2 text-sm text-foreground"
type LocationForm = ObservedLocationForm
type Line = { key: string; lineId?: number; product: PalletProduct; verified: boolean; boxes: string; units: string; expiry: PalletLineInput["expiry_status"]; expires: string; lot: string }

function LocationFields({ locations, value, onChange, onLocationsChange, exclude, disabled }: {
  locations: PalletLocations; value: LocationForm; onChange:(v:LocationForm)=>void
  onLocationsChange:(locations:PalletLocations)=>void
  exclude?: PalletRecord; disabled?: boolean
}) {
  const [mapOpen,setMapOpen]=useState(false)
  const pos = locations.items.find(p=>p.id===Number(value.id))
  const occupied = locations.observed_slots.some(s=>s.position_id===Number(value.id)&&s.observed_level===Number(value.level)&&
    !(exclude?.position_id===s.position_id&&exclude?.observed_level===s.observed_level))
  const special = pos && ({...pos.physical_meta_snap,...pos.constraints_json,...pos.meta_json}.under_stair===true || pos.code.startsWith("BAE-"))
  return <div className="space-y-2">
    <Button type="button" variant="outline" className="h-10 w-full justify-start gap-2 border-primary/40 text-primary hover:bg-primary/10 hover:text-primary" disabled={disabled||!locations.revision} onClick={()=>setMapOpen(true)}>
      <MapPin className="h-4 w-4"/> Seleccionar en el mapa
      {pos?<span className="ml-auto font-mono text-xs">{pos.code}</span>:null}
    </Button>
    {mapOpen?<PalletLocationMap locations={locations} value={value} exclude={exclude} onClose={()=>setMapOpen(false)}
      onSelect={(next,fresh)=>{onLocationsChange(fresh);onChange(next);setMapOpen(false)}}/>:null}
    <div className="grid grid-cols-[minmax(0,1fr)_100px] gap-2">
      <label className="space-y-1 text-xs font-medium">Ubicación observada
        <select className={selectClass} value={value.id} disabled={disabled} onChange={e=>onChange({id:e.target.value,level:"1",ack:false})}>
          <option value="">Sin ubicación verificada</option>
          {locations.items.map(p=><option key={p.id} value={p.id}>{p.code}</option>)}
        </select>
      </label>
      <label className="space-y-1 text-xs font-medium">Nivel observado
        <Input type="number" min={1} max={100} value={value.level} disabled={disabled||!value.id}
          onChange={e=>onChange({...value,level:e.target.value,ack:false})}/>
      </label>
    </div>
    {locations.revision ? <p className="text-xs text-muted-foreground">Referencia guardada: {locations.revision.status} · v{locations.revision.row_version}. Nivel 1 = piso.</p> :
      <p className="text-xs text-lq-warning">No hay plano guardado de referencia. Puedes registrar sin ubicación.</p>}
    {occupied ? <p className="rounded bg-destructive/10 p-2 text-xs text-destructive">Ya hay un pallet activo registrado en este nivel de la posición.</p>:null}
    {value.id ? <div className="rounded-md bg-lq-warning/10 p-2 text-xs text-lq-warning">
      {special ? <p className="mb-1 font-semibold">Espacio bajo escalera: sólo documentar una observación existente. No autoriza almacenar aquí.</p>:null}
      {Number(value.level)>1 ? <p className="mb-1 font-semibold">Nivel superior observado: no implica autorización de apilamiento.</p>:null}
      <label className="flex items-start gap-2"><input type="checkbox" checked={value.ack} disabled={disabled} onChange={e=>onChange({...value,ack:e.target.checked})}/>
        Confirmo la ubicación y el nivel observados físicamente. No es una instrucción de movimiento ni una aprobación de seguridad.</label>
    </div>:null}
  </div>
}

function locationPayload(loc:LocationForm, locations:PalletLocations, exclude?:PalletRecord):ObservedLocationInput {
  if(!loc.id) return {position_id:null,observed_level:null,layout_revision_id:null,layout_row_version:null,observed_ack:false}
  const id=Number(loc.id),level=Number(loc.level)
  if(!locations.revision||!locations.items.some(p=>p.id===id)||!Number.isInteger(level)||level<1||level>100||!loc.ack) throw new Error("Confirma la posición y el nivel físico observado.")
  if(locations.observed_slots.some(s=>s.position_id===id&&s.observed_level===level&&!(exclude?.position_id===id&&exclude?.observed_level===level))) throw new Error("La ubicación/nivel ya tiene un pallet registrado.")
  return {position_id:id,observed_level:level,layout_revision_id:locations.revision.id,layout_row_version:locations.revision.row_version,observed_ack:true}
}

function linePayload(l:Line):PalletLineInput {
  const boxes=l.verified?Number(l.boxes):null, units=l.verified?Number(l.units):null
  if(l.verified&&(!l.boxes.trim()||!l.units.trim()||![boxes,units].every(v=>v!==null&&Number.isInteger(v)&&v>=0&&v<=100000000))) throw new Error("Completa las cajas y unidades verificadas de cada línea.")
  if(l.verified&&(boxes??0)>0&&!l.product.units_per_box) throw new Error("La variante no tiene unidades por caja. Cuenta unidades o deja cantidad pendiente.")
  if(l.verified&&((boxes??0)*(l.product.units_per_box??0)+(units??0)<=0)) throw new Error("Cantidad verificada debe ser mayor que cero.")
  if(l.expiry==="dated"&&!l.expires) throw new Error("Ingresa la fecha de vencimiento de la línea.")
  return {variant_id:l.product.variant_id,quantity_status:l.verified?"verified":"unknown",boxes,loose_units:units,
    expected_units_per_box:l.product.units_per_box,expiry_status:l.expiry,expires_on:l.expiry==="dated"?l.expires:null,lot:l.lot.trim()||null}
}

function RegisterDialog({floorId, locations: initialLocations, onClose, onSaved, initial}:{
  floorId:number;locations:PalletLocations;onClose:()=>void;onSaved:(p:PalletRecord)=>void;initial?:PalletRecord
}) {
  const [locations,setLocations]=useState(initialLocations)
  const [kind,setKind]=useState<keyof typeof kindLabels>(initial?.pallet_kind??"complete")
  const [received,setReceived]=useState(initial?.received_on??"")
  const [notes,setNotes]=useState(initial?.notes??"")
  const [reason,setReason]=useState("")
  const [loc,setLoc]=useState<LocationForm>({id:"",level:"1",ack:false})
  const [query,setQuery]=useState("")
  const [products,setProducts]=useState<PalletProduct[]>([])
  const [lines,setLines]=useState<Line[]>(()=>initial?.lines.map(l=>({
    key:"line-"+l.id,lineId:l.id,
    product:{variant_id:l.variant_id,product_id:l.product_id,product_name:l.product_name_snap,
      variant_name:l.variant_name_snap,product_type:l.product_type_snap,barcode:l.barcode_snap,sku:l.sku_snap,
      units_per_box:l.units_per_box_snap},
    verified:l.quantity_status==="verified",boxes:String(l.boxes??0),units:String(l.loose_units??0),
    expiry:l.expiry_status,expires:l.expires_on??"",lot:l.lot??""
  }))??[])
  const [busy,setBusy]=useState(false)
  const [searching,setSearching]=useState(false)
  const [error,setError]=useState("")
  const [searchError,setSearchError]=useState("")
  const inFlight=useRef(false)
  const registrationKey=useRef<string|null>(null)
  useEffect(()=>{
    let cancelled=false
    if(query.trim().length<2){setProducts([]);setSearching(false);setSearchError("");return}
    setSearching(true);setSearchError("");setProducts([])
    const timer=setTimeout(()=>{
      searchPalletProducts(floorId,query.trim()).then(v=>{if(!cancelled)setProducts(v)})
        .catch(e=>{if(!cancelled)setSearchError(err(e))}).finally(()=>{if(!cancelled)setSearching(false)})
    },250)
    return()=>{cancelled=true;clearTimeout(timer)}
  },[query,floorId])
  const add=(p:PalletProduct)=>{
    if(lines.length>=100)return
    setLines(old=>[...old,{key:crypto.randomUUID(),product:p,verified:false,boxes:"0",units:"0",expiry:"unknown",expires:"",lot:""}])
    setQuery("");setProducts([])
  }
  const patch=(key:string, next:Partial<Line>)=>setLines(old=>old.map(l=>l.key===key?{...l,...next}:l))
  const submit=async()=>{
    if(inFlight.current)return
    setError("")
    try{
      if(!lines.length)throw new Error("Selecciona al menos una variante Bsale.")
      const distinct=new Set(lines.map(l=>l.product.variant_id)).size
      if((kind==="mixed"&&distinct<2)||(kind!=="mixed"&&distinct!==1))throw new Error("Mixto requiere distintas variantes; completo/molido permite varias líneas del mismo producto.")
      if(initial&&reason.trim().length<3)throw new Error("Indica el motivo de la corrección.")
      const fields={pallet_kind:kind,received_on:received||null,notes:notes.trim()||null}
      if(initial){
        const kept=new Set(lines.flatMap(l=>l.lineId===undefined?[]:[l.lineId]))
        const body={...fields,expected_row_version:initial.row_version,reason:reason.trim(),
          lines:lines.map(l=>({...linePayload(l),line_id:l.lineId??null})),
          remove_line_ids:initial.lines.filter(l=>!kept.has(l.id)).map(l=>l.id)}
        inFlight.current=true;setBusy(true)
        onSaved(await updateWarehousePalletDetails(initial.id,body))
      }else{
        const body={...fields,...locationPayload(loc,locations),registration_key:registrationKey.current??crypto.randomUUID(),
          floor_id:floorId,lines:lines.map(linePayload)}
        registrationKey.current=body.registration_key
        inFlight.current=true;setBusy(true)
        onSaved(await registerWarehousePallet(body))
      }
    }catch(e){setError(err(e))}
    finally{inFlight.current=false;setBusy(false)}
  }
  return <Dialog open onOpenChange={open=>{if(!open&&!busy)onClose()}}>
    <DialogContent className="flex max-h-[94vh] w-[96vw] flex-col gap-0 overflow-hidden p-0 sm:max-w-[1040px]">
      <DialogHeader className="shrink-0 border-b px-5 py-4">
        <DialogTitle>{initial?"Editar "+initial.code:"Registrar pallet físico"}</DialogTitle>
        <p className="text-xs text-muted-foreground">{locations.floor.facility_code} · {locations.floor.floor_code} · {initial?"Corrección observada · v"+initial.row_version+" · sin modificar identidad ni stock":"ID PAL asignado al guardar, sin stock Bsale."}</p>
      </DialogHeader>
      <div className="min-h-0 flex-1 overflow-y-auto bg-muted p-4">
        <fieldset disabled={busy} className="grid gap-4 md:grid-cols-[minmax(250px,.75fr)_minmax(0,1.25fr)]">
          <div className="space-y-3">
            <section className="space-y-3 rounded-xl border bg-card p-4">
              <h3 className="text-sm font-semibold">1. Identificación</h3>
              <div className="grid grid-cols-3 gap-1">
                {(Object.keys(kindLabels) as (keyof typeof kindLabels)[]).map(k=><button type="button" key={k} onClick={()=>setKind(k)}
                  className={`rounded-md border p-2 text-xs font-medium ${kind===k?"border-primary bg-primary/10 text-primary":"bg-card text-muted-foreground"}`}>{kindLabels[k]}</button>)}
              </div>
              <label className="block space-y-1 text-xs font-medium">Fecha de ingreso (si se conoce)
                <Input type="date" value={received} onChange={e=>setReceived(e.target.value)}/>
              </label>
              <p className="text-xs text-muted-foreground">Dejar vacía si no existe respaldo. No usar hoy como fecha ficticia de ingreso.</p>
              <label className="block space-y-1 text-xs font-medium">Observaciones
                <textarea className={textAreaClass} rows={3} maxLength={2000} value={notes} onChange={e=>setNotes(e.target.value)}/>
              </label>
            </section>
            {initial?<section className="space-y-3 rounded-xl border bg-card p-4">
              <h3 className="text-sm font-semibold">2. Motivo de corrección</h3>
              <p className="text-xs text-muted-foreground">La ubicación se mantiene: {initial.position_code_snap??"Sin verificar"}{initial.observed_level?" · N"+initial.observed_level:""}. Para corregirla usa Ubicación observada.</p>
              <label className="block space-y-1 text-xs font-medium">Motivo obligatorio
                <textarea className={textAreaClass} value={reason} maxLength={1000} onChange={e=>setReason(e.target.value)} rows={3} placeholder="Ej. Conteo físico y lote verificados"/>
              </label>
              <p className="text-xs text-muted-foreground">Se conservará el registro anterior en el historial. No es una salida ni un ajuste comercial.</p>
              {initial.lines.some(l=>!lines.some(x=>x.lineId===l.id))?<p className="rounded bg-lq-warning/10 p-2 text-xs text-lq-warning">Las líneas retiradas se conservarán en la auditoría al guardar esta corrección.</p>:null}
            </section>:<section className="space-y-3 rounded-xl border bg-card p-4">
              <h3 className="text-sm font-semibold">2. Ubicación física</h3>
              <LocationFields locations={locations} value={loc} onChange={setLoc} onLocationsChange={setLocations} disabled={busy}/>
            </section>}
          </div>
          <section className="min-w-0 space-y-3 rounded-xl border bg-card p-4">
            <h3 className="text-sm font-semibold">3. Productos, lotes y vencimientos</h3>
            <div className="relative">
              <Search className="absolute left-2 top-2.5 h-4 w-4 text-muted-foreground"/>
              <Input value={query} onChange={e=>setQuery(e.target.value)} placeholder="Nombre, variante, código de barras o SKU" className="pl-8"/>
            </div>
            {searching?<p className="text-xs text-muted-foreground">Buscando variantes Bsale…</p>:null}
            {searchError?<p className="text-xs text-destructive">{searchError}</p>:null}
            {products.length?<div className="max-h-48 overflow-y-auto rounded-lg border">
              {products.map(p=><button type="button" key={p.variant_id} onClick={()=>add(p)} className="flex w-full items-start justify-between gap-2 border-b p-2 text-left hover:bg-muted">
                <span className="min-w-0"><strong className="block text-xs">{p.product_name} · {p.variant_name||"Variante sin nombre"}</strong><span className="block text-[11px] text-muted-foreground">{p.product_type||"Sin tipo"} · {p.barcode||p.sku||"Sin código"} · ID {p.variant_id} · {p.units_per_box?p.units_per_box+" un/caja":"Conversión no disponible"}</span></span><Plus className="h-4 w-4 shrink-0"/>
              </button>)}
            </div>:query.trim().length>=2&&!searching&&!searchError?<p className="text-xs text-muted-foreground">Sin coincidencias.</p>:null}
            {!lines.length?<div className="rounded-lg border border-dashed p-6 text-center text-xs text-muted-foreground">Busca y agrega productos reales. Puedes agregar la misma variante nuevamente para otro lote.</div>:null}
            {lines.map((l,i)=><div key={l.key} className="space-y-2 rounded-lg border bg-muted p-3">
              <div className="flex items-start justify-between gap-2">
                <div className="min-w-0"><p className="text-xs font-semibold">{i+1}. {l.product.product_name} · {l.product.variant_name||"Variante sin nombre"}</p><p className="text-[11px] text-muted-foreground">{l.product.product_type||"Sin tipo"} · {l.product.barcode||l.product.sku||"Sin código"} · ID {l.product.variant_id}</p></div>
                <Button type="button" size="icon" variant="ghost" className="h-7 w-7" aria-label="Quitar línea" onClick={()=>setLines(old=>old.filter(x=>x.key!==l.key))}><X className="h-4 w-4"/></Button>
              </div>
              <label className="flex items-center gap-2 text-xs"><input type="checkbox" checked={l.verified} onChange={e=>patch(l.key,{verified:e.target.checked})}/> Cantidad contada y verificada</label>
              {l.verified?<div className="grid grid-cols-2 gap-2">
                <label className="space-y-1 text-xs">Cajas ({l.product.units_per_box?l.product.units_per_box+" un/caja":"no disponible"})<Input inputMode="numeric" disabled={!l.product.units_per_box} value={l.boxes} onChange={e=>patch(l.key,{boxes:e.target.value})}/></label>
                <label className="space-y-1 text-xs">Unidades contadas aparte<Input inputMode="numeric" value={l.units} onChange={e=>patch(l.key,{units:e.target.value})}/></label>
                <p className="col-span-2 text-xs text-muted-foreground">Total calculado: {num((Number(l.boxes)||0)*(l.product.units_per_box??0)+(Number(l.units)||0))} unidades</p>
              </div>:<p className="text-xs text-lq-warning">Cantidad pendiente de conteo (no equivale a cero).</p>}
              <div className="grid grid-cols-2 gap-2">
                <label className="space-y-1 text-xs">Vencimiento<select className={selectClass} value={l.expiry} onChange={e=>patch(l.key,{expiry:e.target.value as Line["expiry"]})}>
                  <option value="unknown">Sin verificar</option><option value="dated">Fecha verificada</option><option value="not_applicable">No aplica (confirmado)</option>
                </select></label>
                <label className="space-y-1 text-xs">Lote (si se conoce)<Input maxLength={200} value={l.lot} onChange={e=>patch(l.key,{lot:e.target.value})}/></label>
              </div>
              {l.expiry==="dated"?<label className="block space-y-1 text-xs">Fecha verificada<Input type="date" value={l.expires} onChange={e=>patch(l.key,{expires:e.target.value})}/></label>:null}
            </div>)}
          </section>
        </fieldset>
        {error?<div className="mt-3 rounded-md border border-destructive/30 bg-destructive/10 p-3 text-sm text-destructive">{error}<p className="mt-1 text-xs">{initial?"Si la versión cambió, cierra y vuelve a consultar el pallet antes de corregirlo.":"Si se perdió la respuesta al guardar, reintenta sin cambiar los datos o revisa el listado antes de registrar nuevamente."}</p></div>:null}
      </div>
      <DialogFooter className="shrink-0 border-t px-5 py-3">
        <Button type="button" variant="outline" disabled={busy} onClick={onClose}>Cancelar</Button>
        <Button type="button" disabled={busy||!lines.length||!!initial&&reason.trim().length<3} onClick={()=>void submit()}>{busy?<Loader2 className="mr-2 h-4 w-4 animate-spin"/>:null}{initial?"Guardar corrección":"Registrar pallet"}</Button>
      </DialogFooter>
    </DialogContent>
  </Dialog>
}

function DetailDialog({pallet, locations: initialLocations, onClose, onChanged, onEdit}:{
  pallet:PalletRecord; locations:PalletLocations; onClose:()=>void; onChanged:()=>void;onEdit:(p:PalletRecord)=>void
}) {
  const [locations,setLocations]=useState(initialLocations)
  const [data,setData]=useState(pallet)
  const [mode,setMode]=useState<"view"|"location"|"void">("view")
  const [loc,setLoc]=useState<LocationForm>({id:pallet.position_id?String(pallet.position_id):"",level:String(pallet.observed_level??1),ack:false})
  const [reason,setReason]=useState("")
  const [busy,setBusy]=useState(false)
  const [error,setError]=useState("")
  const guard=useRef(false)
  useEffect(()=>{
    let cancelled=false
    getWarehousePallet(pallet.id).then(p=>{if(!cancelled){setData(p);setLoc({id:p.position_id?String(p.position_id):"",level:String(p.observed_level??1),ack:false})}}).catch(e=>{if(!cancelled)setError(err(e))})
    return()=>{cancelled=true}
  },[pallet.id])
  const change=async()=>{
    if(guard.current)return
    setError("")
    try{
      if(reason.trim().length<3)throw new Error("Indica un motivo.")
      guard.current=true;setBusy(true)
      if(mode==="location") await observeWarehousePalletLocation(data.id,{...locationPayload(loc,locations,data),expected_row_version:data.row_version,reason:reason.trim()})
      else if(mode==="void") await voidWarehousePallet(data.id,{expected_row_version:data.row_version,reason:reason.trim()})
      onChanged()
    }catch(e){setError(err(e))}finally{guard.current=false;setBusy(false)}
  }
  return <Dialog open onOpenChange={v=>{if(!v&&!busy)onClose()}}>
    <DialogContent className="flex max-h-[92vh] w-[96vw] flex-col gap-0 overflow-hidden p-0 sm:max-w-[760px]">
      <DialogHeader className="border-b px-5 py-4"><DialogTitle>{data.code} · {kindLabels[data.pallet_kind]}</DialogTitle><p className="text-sm text-muted-foreground">{data.status==="voided"?"Registro anulado":data.position_code_snap?data.position_code_snap+" · nivel observado "+data.observed_level:"Sin ubicación verificada"} · v{data.row_version}</p></DialogHeader>
      <div className="min-h-0 flex-1 space-y-4 overflow-y-auto p-5">
        <p className="text-xs text-muted-foreground">Ingreso: {dateText(data.received_on)} · registrado: {dateText(data.created_at)}</p>
        {data.notes?<p className="rounded bg-muted p-2 text-sm">{data.notes}</p>:null}
        <div className="space-y-2">{data.lines.map(l=><div key={l.id} className="rounded-lg border p-3">
          <p className="text-sm font-semibold">{l.product_name_snap} · {l.variant_name_snap||"Variante sin nombre"}</p>
          <p className="text-xs text-muted-foreground">{l.product_type_snap||"Sin tipo"} · {l.barcode_snap||l.sku_snap||"Sin código"} · variante {l.variant_id}</p>
          <p className="mt-2 text-xs">{l.total_units==null?"Cantidad pendiente":num(l.total_units)+" unidades · "+num(l.boxes??0)+" cajas + "+num(l.loose_units??0)+" unidades"}</p>
          <p className="text-xs">Lote: {l.lot||"Sin verificar"} · Vence: {l.expiry_status==="not_applicable"?"No aplica":dateText(l.expires_on)}</p>
        </div>)}</div>
        {mode==="location"?<div className="space-y-2 rounded-lg border p-3"><h3 className="text-sm font-semibold">Registrar ubicación observada</h3><LocationFields value={loc} onChange={setLoc} locations={locations} onLocationsChange={setLocations} exclude={data} disabled={busy}/></div>:null}
        {mode==="void"?<p className="rounded bg-destructive/10 p-3 text-xs text-destructive">Anular sólo un registro erróneo o duplicado. No elimina el historial, no descuenta stock ni representa una salida de mercadería.</p>:null}
        {mode!=="view"?<label className="block space-y-1 text-xs font-medium">Motivo obligatorio<textarea className={textAreaClass} disabled={busy} value={reason} maxLength={1000} onChange={e=>setReason(e.target.value)} rows={2}/></label>:null}
        <div><h3 className="mb-2 flex items-center gap-2 text-sm font-semibold"><History className="h-4 w-4"/>Historial</h3>
          {data.events?.map(e=><div key={e.id} className="border-b py-2 text-xs"><strong>{e.action==="registered"?"Registro inicial":e.action==="voided"?"Anulación":e.action==="details_updated"?"Corrección de datos":"Cambio de ubicación observado"}</strong><p className="text-muted-foreground">{dateText(e.created_at)} · {e.actor_label}</p>{typeof e.detail_json.reason==="string"?<p>{e.detail_json.reason}</p>:null}{e.action==="location_observed"||e.action==="details_updated"?<details><summary className="cursor-pointer text-muted-foreground">Detalle antes/después</summary><pre className="overflow-auto whitespace-pre-wrap text-[10px]">{JSON.stringify(e.detail_json,null,2)}</pre></details>:null}</div>)}
        </div>
        {error?<p className="rounded bg-destructive/10 p-2 text-xs text-destructive">{error}</p>:null}
      </div>
      <DialogFooter className="border-t px-5 py-3">
        {mode==="view"?<>{data.status==="active"&&data.events?<><Button type="button" variant="outline" onClick={()=>onEdit(data)}><Pencil className="mr-1 h-4 w-4"/>Editar datos</Button><Button type="button" variant="outline" onClick={()=>{setReason("");setMode("location")}}><ArrowLeftRight className="mr-1 h-4 w-4"/>Ubicación observada</Button><Button type="button" variant="outline" className="text-destructive" onClick={()=>{setReason("");setMode("void")}}>Anular registro</Button></>:null}<Button type="button" onClick={onClose}>Cerrar</Button></>:
          <><Button type="button" variant="outline" disabled={busy} onClick={()=>setMode("view")}>Volver</Button><Button type="button" disabled={busy||reason.trim().length<3} onClick={()=>void change()}>{busy?"Guardando…":mode==="void"?"Confirmar anulación":"Guardar observación"}</Button></>}
      </DialogFooter>
    </DialogContent>
  </Dialog>
}

export function PalletRegister(){
  const [facilities,setFacilities]=useState<WarehouseFacility[]>([])
  const [floors,setFloors]=useState<WarehouseFloor[]>([])
  const [facility,setFacility]=useState<number|null>(null)
  const [floor,setFloor]=useState<number|null>(null)
  const [locations,setLocations]=useState<PalletLocations|null>(null)
  const [records,setRecords]=useState<PalletRecord[]>([])
  const [query,setQuery]=useState("")
  const [filter,setFilter]=useState("")
  const [offset,setOffset]=useState(0)
  const [more,setMore]=useState(false)
  const [loading,setLoading]=useState(true)
  const [error,setError]=useState("")
  const [notice,setNotice]=useState("")
  const [open,setOpen]=useState(false)
  const [selected,setSelected]=useState<PalletRecord|null>(null)
  const [editing,setEditing]=useState<PalletRecord|null>(null)
  const [reload,setReload]=useState(0)
  useEffect(()=>{
    let cancelled=false
    listWarehouseFacilities().then(f=>{if(!cancelled){setFacilities(f);setFacility((f.find(x=>x.code==="BC")||f[0])?.id??null);if(!f.length)setLoading(false)}})
      .catch(e=>{if(!cancelled){setError(err(e));setLoading(false)}})
    return()=>{cancelled=true}
  },[])
  useEffect(()=>{
    if(facility==null)return
    let cancelled=false
    setFloor(null);setFloors([]);setLocations(null);setRecords([]);setLoading(true);setError("")
    listWarehouseFloors(facility).then(f=>{if(!cancelled){setFloors(f);setFloor((f.find(x=>x.code==="PISO1")||f[0])?.id??null);if(!f.length)setLoading(false)}})
      .catch(e=>{if(!cancelled){setError(err(e));setLoading(false)}})
    return()=>{cancelled=true}
  },[facility])
  useEffect(()=>{
    if(floor==null)return
    let cancelled=false
    setLoading(true);setError("");setLocations(null);setRecords([])
    Promise.all([listPalletLocations(floor),listWarehousePallets(floor,filter,offset)])
      .then(([loc,res])=>{if(!cancelled){setLocations(loc);setRecords(res.items);setMore(res.has_more)}})
      .catch(e=>{if(!cancelled)setError(err(e))}).finally(()=>{if(!cancelled)setLoading(false)})
    return()=>{cancelled=true}
  },[floor,filter,offset,reload])
  const refresh=useCallback(()=>setReload(x=>x+1),[])
  return <div className="space-y-4">
    <div className="flex flex-wrap items-center justify-between gap-3">
      <div><h1 className="text-xl font-semibold tracking-tight">Registro de pallets</h1><p className="text-sm text-muted-foreground">Levantamiento físico · Identidades persistentes, sin movimientos de stock comercial</p></div>
      <Button type="button" disabled={!locations||locations.floor.floor_id!==floor||loading} onClick={()=>{setNotice("");setOpen(true)}}><Plus className="mr-2 h-4 w-4"/>Registrar pallet</Button>
    </div>
    <div className="flex items-start gap-2 rounded-lg border border-lq-warning/30 bg-lq-warning/10 p-3 text-xs text-lq-warning"><AlertTriangle className="h-4 w-4 shrink-0"/>Las ubicaciones y niveles registran observaciones. No autorizan uso de circulación, espacios bajo escalera ni apilamiento. No se calculan existencias de Bsale ni capacidad disponible a partir de registros parciales.</div>
    <div className="flex flex-wrap items-end gap-2 rounded-lg border bg-card p-3">
      <label className="min-w-[180px] space-y-1 text-xs font-medium">Bodega<select className={selectClass} value={facility??""} disabled={open||!!selected||!!editing} onChange={e=>{setOffset(0);setFacility(Number(e.target.value)||null)}}><option value="" disabled>Seleccionar</option>{facilities.map(f=><option key={f.id} value={f.id}>{f.code} · {f.name}</option>)}</select></label>
      <label className="min-w-[160px] space-y-1 text-xs font-medium">Piso<select className={selectClass} value={floor??""} disabled={open||!!selected||!!editing} onChange={e=>{setOffset(0);setFloor(Number(e.target.value)||null)}}><option value="" disabled>Seleccionar</option>{floors.map(f=><option key={f.id} value={f.id}>{f.code} · {f.name}</option>)}</select></label>
      <form className="flex min-w-[250px] flex-1 gap-2" onSubmit={e=>{e.preventDefault();setOffset(0);setFilter(query.trim())}}>
        <Input placeholder="Buscar PAL, producto, lote, ubicación..." value={query} maxLength={120} onChange={e=>setQuery(e.target.value)}/>
        <Button variant="outline" type="submit"><Search className="h-4 w-4"/></Button>
      </form>
      <Button type="button" variant="outline" disabled={loading} onClick={refresh}>Actualizar</Button>
    </div>
    {notice?<p className="rounded-md border border-lq-success/30 bg-lq-success/10 p-3 text-sm text-lq-success">{notice}</p>:null}
    {error?<p className="rounded-md border border-destructive/30 bg-destructive/10 p-3 text-sm text-destructive">{error}</p>:null}
    {locations?<div className="grid gap-2 sm:grid-cols-3">
      <div className="rounded-lg border bg-card p-3"><p className="text-xs text-muted-foreground">Plano de referencia</p><p className="text-lg font-semibold">{locations.revision?locations.revision.status+" · v"+locations.revision.row_version:"No disponible"}</p></div>
      <div className="rounded-lg border bg-card p-3"><p className="text-xs text-muted-foreground">Posiciones geométricas de referencia</p><p className="text-lg font-semibold">{locations.items.length}</p><p className="text-[11px] text-muted-foreground">Incluye posiciones provisionales; no es capacidad autorizada.</p></div>
      <div className="rounded-lg border bg-card p-3"><p className="text-xs text-muted-foreground">Ubicaciones/niveles registrados activos</p><p className="text-lg font-semibold">{locations.observed_slots.length}</p><p className="text-[11px] text-muted-foreground">Levantamiento parcial, no ocupación certificada.</p></div>
    </div>:null}
    <section className="overflow-hidden rounded-lg border bg-card">
      <div className="flex items-center justify-between border-b p-3"><h2 className="flex items-center gap-2 text-sm font-semibold"><ClipboardList className="h-4 w-4"/>Pallets registrados</h2><span className="text-xs text-muted-foreground">{records.length?offset+1+"–"+(offset+records.length):"Sin filas"}</span></div>
      {loading?<div className="flex items-center justify-center gap-2 p-10 text-sm text-muted-foreground"><Loader2 className="h-4 w-4 animate-spin"/>Cargando…</div>:!records.length?<div className="p-10 text-center text-sm text-muted-foreground"><Box className="mx-auto mb-2 h-8 w-8"/>No hay registros para esta búsqueda.</div>:<div className="overflow-x-auto">
        <table className="w-full min-w-[850px] text-left text-xs"><thead className="bg-muted text-muted-foreground"><tr>{["Pallet","Ubicación observada","Productos / variantes","Cantidad","Vencimiento","Estado"].map(x=><th className="p-3 font-medium" key={x}>{x}</th>)}</tr></thead>
          <tbody>{records.map(p=>{
            const known=p.lines.every(l=>l.total_units!==null)
            const expiry=p.lines.map(l=>l.expires_on).filter((x):x is string=>!!x).sort()[0]
            const unknownExpiry=p.lines.some(l=>l.expiry_status==="unknown")
            return <tr key={p.id} className="border-t hover:bg-muted">
              <td className="p-3"><button type="button" className="font-semibold text-destructive underline-offset-2 hover:underline" onClick={()=>setSelected(p)}>{p.code}</button><p className="text-muted-foreground">{kindLabels[p.pallet_kind]}</p></td>
              <td className="p-3">{p.position_code_snap?p.position_code_snap+" · N"+p.observed_level:"Sin verificar"}{p.location_in_current_layout===false?<p className="text-lq-warning">No figura en plano vigente</p>:p.current_position_code&&p.current_position_code!==p.position_code_snap?<p className="text-lq-warning">Código actual: {p.current_position_code}</p>:null}</td>
              <td className="max-w-[360px] p-3">{p.lines.slice(0,2).map(l=><div key={l.id}><strong>{l.product_name_snap}</strong> · {l.variant_name_snap||"Variante sin nombre"}<p className="text-[11px] text-muted-foreground">{l.product_type_snap||"Sin tipo"} · {l.barcode_snap||"ID "+l.variant_id}</p></div>)}{p.lines.length>2?<p className="text-muted-foreground">+{p.lines.length-2} líneas</p>:null}</td>
              <td className="p-3">{p.pallet_kind==="mixed"?(known?"Ver cantidades por variante":"Conteo parcial · ver líneas"):known?num(p.lines.reduce((n,l)=>n+(l.total_units??0),0))+" un.":"Pendiente"}</td>
              <td className="p-3">{expiry?dateText(expiry):unknownExpiry?"Sin verificar":"No aplica"}{expiry&&unknownExpiry?<p className="text-lq-warning">Otras fechas pendientes</p>:null}</td>
              <td className="p-3">{p.status==="voided"?<span className="text-muted-foreground">Anulado</span>:<span className="text-foreground">Registrado</span>}</td>
            </tr>
          })}</tbody>
        </table>
      </div>}
      <div className="flex justify-end gap-2 border-t p-3"><Button size="sm" variant="outline" disabled={loading||offset===0} onClick={()=>setOffset(v=>Math.max(0,v-50))}>Anterior</Button><Button size="sm" variant="outline" disabled={loading||!more} onClick={()=>setOffset(v=>v+50)}>Siguiente</Button></div>
    </section>
    {open&&locations&&floor!=null?<RegisterDialog floorId={floor} locations={locations} onClose={()=>setOpen(false)} onSaved={p=>{setOpen(false);setNotice(p.code+" registrado. No se modificó stock Bsale.");setOffset(0);setFilter("");setQuery("");refresh()}}/>:null}
    {selected&&locations?<DetailDialog pallet={selected} locations={locations} onClose={()=>setSelected(null)}
      onEdit={p=>{setSelected(null);setEditing(p)}} onChanged={()=>{setSelected(null);setNotice("Observación registrada con historial.");refresh()}}/>:null}
    {editing&&locations&&floor!=null?<RegisterDialog key={"edit-"+editing.id} initial={editing} floorId={floor} locations={locations}
      onClose={()=>setEditing(null)} onSaved={p=>{setEditing(null);setNotice(p.code+" actualizado. Identidad e historial conservados.");refresh()}}/>:null}
  </div>
}
