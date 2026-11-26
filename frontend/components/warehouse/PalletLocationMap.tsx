"use client"

import { useEffect, useMemo, useState } from "react"
import { AlertTriangle, MapPin, Loader2, Minus, Plus, RotateCcw, Search } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { getWarehouseRevision, getWarehousePallet, listPalletLocations, type PalletLocations, type PalletRecord } from "@/lib/warehouse/api"
import { metricToScreen, type ViewportTransform } from "@/lib/warehouse/coords"
import type { RevisionBundle } from "@/lib/warehouse/types"
import { isUnderStairMeta } from "@/lib/warehouse/style"

export type ObservedLocationForm = {id:string;level:string;ack:boolean}
type Props = {
  locations:PalletLocations; value:ObservedLocationForm; exclude?:PalletRecord
  onClose:()=>void
  onSelect:(value:ObservedLocationForm, fresh:PalletLocations)=>void
}
const message=(e:unknown)=>e instanceof Error?e.message:"No se pudo cargar el mapa."
const number=(n:number)=>n.toLocaleString("es-CL")
const dateLabel=(d:string|null)=>d?d.slice(0,10).split("-").reverse().join("/"):"Sin verificar"

export function PalletLocationMap({locations,value,exclude,onClose,onSelect}:Props){
  const [fresh,setFresh]=useState<PalletLocations|null>(null)
  const [bundle,setBundle]=useState<RevisionBundle|null>(null)
  const [level,setLevel]=useState(value.level||"1")
  const [picked,setPicked]=useState(value.id)
  const [search,setSearch]=useState("")
  const [zoom,setZoom]=useState(1)
  const [refresh,setRefresh]=useState(0)
  const [error,setError]=useState("")
  const [detail,setDetail]=useState<PalletRecord|null>(null)
  const [detailError,setDetailError]=useState("")
  const [detailLoading,setDetailLoading]=useState(false)
  const floorId=locations.floor.floor_id
  const revisionId=locations.revision?.id
  const revisionVersion=locations.revision?.row_version
  useEffect(()=>{
    let cancelled=false
    setFresh(null);setBundle(null);setError("")
    ;(async()=>{
      const loc=await listPalletLocations(floorId)
      if(!loc.revision||loc.revision.id!==revisionId||loc.revision.row_version!==revisionVersion) {
        throw new Error("El plano guardado cambió. Cierra el registro y vuelve a abrirlo para actualizar la referencia.")
      }
      const plan=await getWarehouseRevision(loc.revision.id)
      if(plan.revision.id!==loc.revision.id||plan.revision.row_version!==loc.revision.row_version||
         plan.revision.floor_id!==floorId||plan.floor?.id!==floorId) {
        throw new Error("La geometría cambió durante la consulta. Actualiza el registro antes de seleccionar.")
      }
      if(!(Number(plan.floor?.length_m)>0&&Number(plan.floor?.width_m)>0)) throw new Error("El plano no tiene dimensiones métricas válidas.")
      const geometryIds=new Set(plan.elements.filter(e=>e.physical.element_type==="position").map(e=>e.physical.id))
      if(loc.items.some(p=>!geometryIds.has(p.id)))throw new Error("Las ubicaciones no coinciden con el plano de referencia.")
      if(!cancelled){setFresh(loc);setBundle(plan)}
    })().catch(e=>{if(!cancelled)setError(message(e))})
    return()=>{cancelled=true}
  },[floorId,revisionId,revisionVersion,refresh])
  const validLevel=Number.isInteger(Number(level))&&Number(level)>=1&&Number(level)<=100
  const slotByPosition=useMemo(()=>new Map((fresh?.observed_slots??[]).filter(s=>s.observed_level===Number(level)).map(s=>[s.position_id,s])),[fresh,level])
  const chosen=fresh?.items.find(p=>p.id===Number(picked))
  const slot=chosen?slotByPosition.get(chosen.id):undefined
  const own=!!slot&&!!exclude&&slot.pallet_id===exclude.id
  // Compatibilidad con API 2A: si no hay pallet_id, no asumir que el slot está libre.
  const blocked=!!slot&&!own
  const palletId=slot?.pallet_id
  useEffect(()=>{
    let cancelled=false
    setDetail(null);setDetailError("");setDetailLoading(false)
    if(!palletId||!validLevel)return
    setDetailLoading(true)
    getWarehousePallet(palletId).then(p=>{
      if(cancelled)return
      if(p.status!=="active"||p.position_id!==Number(picked)||p.observed_level!==Number(level)){
        setDetailError("La ocupación cambió desde la consulta. Actualiza el mapa.")
      }else setDetail(p)
    }).catch(e=>{if(!cancelled)setDetailError(message(e))}).finally(()=>{if(!cancelled)setDetailLoading(false)})
    return()=>{cancelled=true}
  },[palletId,picked,level,validLevel,refresh])
  const length=Number(bundle?.floor?.length_m)||0
  const width=Number(bundle?.floor?.width_m)||0
  const scale=40
  const viewport:ViewportTransform={scale,offsetX:24,offsetY:24,floorLengthM:length,floorWidthM:width,viewOrientationDeg:90}
  const elements=useMemo(()=>{
    const order:Record<string,number>={facility_boundary:0,sector:1,circulation:2,aisle:3,office:4,stair:4,gate:4,loose_storage:4,position:5}
    return [...(bundle?.elements??[])].sort((a,b)=>(order[a.physical.element_type]??4)-(order[b.physical.element_type]??4))
  },[bundle])
  const known=new Map((fresh?.items??[]).map(p=>[p.id,p]))
  const matches=(code:string)=>!search.trim()||code.toLowerCase().includes(search.trim().toLowerCase())
  const levels=[...new Set([1,...(fresh?.observed_slots??[]).map(s=>s.observed_level),...(validLevel?[Number(level)]:[])])].sort((a,b)=>a-b)
  const numberOccupied=validLevel?slotByPosition.size:0
  return <Dialog open onOpenChange={v=>{if(!v)onClose()}}>
    <DialogContent className="flex max-h-[94vh] w-[97vw] flex-col gap-0 overflow-hidden p-0 sm:max-w-[1320px]">
      <DialogHeader className="shrink-0 border-b bg-card px-5 py-4 pr-14">
        <DialogTitle className="flex items-center gap-2"><MapPin className="h-5 w-5 text-primary"/>Mapa de ubicaciones</DialogTitle>
        <DialogDescription>{locations.floor.facility_code} · {locations.floor.floor_code} · Plano {locations.revision?.status} v{revisionVersion} · orientación horizontal</DialogDescription>
      </DialogHeader>
      <div className="min-h-0 flex-1 space-y-3 overflow-y-auto bg-background p-4">
        <div className="flex flex-wrap items-end justify-between gap-3 rounded-xl border bg-card p-3">
          <div className="flex flex-wrap items-end gap-2">
            <label className="space-y-1 text-xs font-medium">Nivel observado
              <select value={validLevel?level:""} onChange={e=>setLevel(e.target.value)} className="block h-9 min-w-[120px] rounded-md border bg-card px-2 text-sm text-foreground">
                {!validLevel?<option value="">Seleccionar nivel</option>:null}
                {levels.map(n=><option key={n} value={n}>N{n}{n===1?" · Piso":""}</option>)}
              </select>
            </label>
            <label className="w-24 space-y-1 text-xs font-medium">Otro nivel
              <Input type="number" min={1} max={100} value={level} onChange={e=>setLevel(e.target.value)}/>
            </label>
            <label className="relative space-y-1 text-xs font-medium">Buscar posición
              <span className="relative block"><Search className="absolute left-2 top-2.5 h-4 w-4 text-muted-foreground"/><Input className="w-40 pl-8" placeholder="P3-04, IZ…" value={search} onChange={e=>setSearch(e.target.value)}/></span>
            </label>
          </div>
          <div className="flex items-center gap-1">
            <Button type="button" variant="outline" size="icon" aria-label="Alejar mapa" disabled={zoom<=1} onClick={()=>setZoom(z=>Math.max(1,z-.5))}><Minus className="h-4 w-4"/></Button>
            <span className="min-w-12 text-center text-xs tabular-nums text-muted-foreground">{Math.round(zoom*100)}%</span>
            <Button type="button" variant="outline" size="icon" aria-label="Acercar mapa" disabled={zoom>=3} onClick={()=>setZoom(z=>Math.min(3,z+.5))}><Plus className="h-4 w-4"/></Button>
            <Button type="button" size="sm" variant="outline" onClick={()=>setRefresh(v=>v+1)}><RotateCcw className="mr-1 h-4 w-4"/>Actualizar</Button>
          </div>
        </div>
        <div className="rounded-lg border border-lq-warning/30 bg-lq-warning/10 p-2 text-xs text-lq-warning">
          <AlertTriangle className="mr-1 inline h-4 w-4"/>Sin registro no significa vacío. Confirma físicamente el nivel; el mapa no autoriza apilamiento ni almacenamiento en zonas restringidas.
        </div>
        {error?<p role="alert" className="rounded-lg border border-destructive/30 bg-destructive/10 p-3 text-sm text-destructive">{error}</p>:!fresh||!bundle?<div className="flex items-center justify-center gap-2 p-12 text-sm text-muted-foreground"><Loader2 className="h-4 w-4 animate-spin"/>Cargando plano y ocupación…</div>:
          <div className="grid min-w-0 gap-3 xl:grid-cols-[minmax(0,1fr)_285px]">
            <div className="min-w-0 rounded-xl border bg-card p-2">
              <div className="mb-2 flex flex-wrap items-center justify-between gap-2 px-1 text-xs text-muted-foreground"><span>Fondo ←</span><span>{validLevel?numberOccupied+" ubicaciones registradas en N"+level:"Nivel inválido"}</span><span>→ Entrada</span></div>
              <div className="max-h-[56vh] overflow-auto rounded-lg border bg-background">
                <svg role="group" aria-label="Plano métrico interactivo: selecciona una posición" viewBox={`0 0 ${length*scale+48} ${width*scale+48}`} width="100%" className="block" style={{width:`${zoom*100}%`,minWidth:760*zoom}}>
                  {elements.map(e=>{
                    const p=e.physical,g=e.geometry,b=metricToScreen(g,viewport),cx=b.x+b.width/2,cy=b.y+b.height/2
                    const pos=known.get(p.id),isPosition=p.element_type==="position"
                    const selected=!!pos&&String(p.id)===picked
                    const occ=slotByPosition.get(p.id)
                    const restricted=!!pos&&(isUnderStairMeta({...pos.physical_meta_snap,...pos.constraints_json,...pos.meta_json})||pos.code.startsWith("BAE-"))
                    const active=!!pos&&validLevel
                    const faded=isPosition&&!matches(p.code)
                    const fill=selected?"var(--chart-3)":occ?"var(--lq-warning)":isPosition?"var(--muted)":p.element_type==="stair"?"var(--chart-5)":p.element_type==="office"?"var(--chart-4)":"var(--card)"
                    const stroke=selected?"var(--lq-info)":restricted?"var(--chart-5)":occ?"var(--lq-warning)":"var(--lq-line)"
                    const shapeLabel=pos?p.code+(occ?" · "+(occ.pallet_code??"Pallet registrado"):" · Sin registro en este nivel")+(restricted?" · Restringida":""):p.code
                    const choose=()=>{if(active)setPicked(String(p.id))}
                    return <g key={p.id} role={active?"button":undefined} tabIndex={active?0:undefined} aria-label={active?shapeLabel:undefined}
                      aria-pressed={active?selected:undefined} onClick={active?choose:undefined} onKeyDown={active?ev=>{if(ev.key==="Enter"||ev.key===" "){ev.preventDefault();choose()}}:undefined}
                      className={active?"cursor-pointer focus-visible:outline-none focus-visible:[&_rect]:stroke-[var(--primary)]":undefined} opacity={faded ? .25 : 1}>
                      <title>{shapeLabel}</title>
                      <rect x={b.x} y={b.y} width={b.width} height={b.height} transform={`rotate(${b.rotation} ${cx} ${cy})`}
                        fill={fill} fillOpacity={selected ? .72 : occ ? .36 : isPosition ? 1 : p.element_type==="stair"||p.element_type==="office" ? .18 : .45}
                        stroke={stroke} strokeWidth={selected?2.6:restricted?1.8:1} strokeDasharray={restricted?"4 2":p.element_type==="aisle"?"4 3":undefined}/>
                      {(isPosition||["office","gate","stair","circulation","loose_storage"].includes(p.element_type))?
                        <text x={cx} y={cy} textAnchor="middle" dominantBaseline="central" pointerEvents="none"
                          fontSize={isPosition?Math.max(6,Math.min(9,Math.min(b.width,b.height)*.21)):10} fontWeight={selected||isPosition?600:400}
                          fill="var(--foreground)">{p.code}</text>:null}
                      {occ&&isPosition?<circle cx={cx+Math.min(b.width,b.height)*.32} cy={cy-Math.min(b.width,b.height)*.32} r={2.8} fill="var(--lq-warning)" pointerEvents="none"/>:null}
                    </g>
                  })}
                </svg>
              </div>
              <div className="mt-2 flex flex-wrap gap-x-4 gap-y-2 text-xs text-muted-foreground">
                {[["var(--muted)","Sin registro"],["var(--lq-warning)","Pallet registrado"],["var(--chart-3)","Seleccionada"],["var(--chart-5)","Restringida"]].map(([color,label])=><span key={label} className="inline-flex items-center gap-1.5"><span className="h-3 w-3 rounded border" style={{backgroundColor:color}}/>{label}</span>)}
              </div>
            </div>
            <aside className="min-w-0 space-y-3 rounded-xl border bg-card p-4">
              <p className="text-xs font-medium text-muted-foreground">UBICACIÓN SELECCIONADA</p>
              {chosen?<><h3 className="text-2xl font-semibold tracking-tight">{chosen.code}</h3><p className="text-xs text-muted-foreground">{chosen.name} · N{level}</p>
                <div className={`rounded-md border p-2 text-xs ${slot?"border-lq-warning/30 bg-lq-warning/10 text-lq-warning":"border-border bg-muted text-foreground"}`}>{slot?(own?"Pallet actual · "+(slot.pallet_code??"Registrado"):(slot.pallet_code??"Pallet registrado")+" · Ocupado en este nivel"):"Sin pallet registrado en este nivel"}</div>
                {chosen.code.startsWith("BAE-")||isUnderStairMeta({...chosen.physical_meta_snap,...chosen.constraints_json,...chosen.meta_json})?<p className="text-xs text-lq-warning">Bajo escalera / restringida. Sólo observaciones existentes.</p>:null}
                {detailLoading?<p className="text-xs text-muted-foreground">Consultando pallet…</p>:null}
                {detailError?<p className="text-xs text-destructive">{detailError}</p>:null}
                {detail?<div className="space-y-2">{detail.lines.map(l=><div key={l.id} className="rounded-lg border bg-background p-2">
                  <p className="text-xs font-semibold">{l.product_name_snap} · {l.variant_name_snap||"Variante sin nombre"}</p>
                  <p className="text-[11px] text-muted-foreground">{l.product_type_snap||"Sin tipo"} · {l.barcode_snap||l.sku_snap||"ID "+l.variant_id}</p>
                  <p className="mt-1 text-xs">{l.total_units===null?"Cantidad pendiente":number(l.total_units)+" un."} · Lote {l.lot||"Sin verificar"}</p>
                  <p className="text-xs">Vence: {l.expiry_status==="not_applicable"?"No aplica":dateLabel(l.expires_on)}</p>
                </div>)}</div>:null}
              </>:<p className="text-sm text-muted-foreground">Haz clic en una posición para ver su código y ocupación.</p>}
              {chosen&&fresh?<div className="border-t pt-2 text-xs text-muted-foreground">Niveles registrados aquí: {fresh.observed_slots.filter(s=>s.position_id===chosen.id).map(s=>"N"+s.observed_level).join(", ")||"Ninguno"}</div>:null}
            </aside>
          </div>}
      </div>
      <DialogFooter className="shrink-0 border-t bg-card px-5 py-3">
        <Button type="button" variant="outline" onClick={onClose}>Cancelar</Button>
        <Button type="button" disabled={!fresh||!chosen||!validLevel||blocked||!!error} onClick={()=>{if(fresh&&chosen&&!blocked&&validLevel)onSelect({id:String(chosen.id),level,ack:false},fresh)}}>Usar ubicación seleccionada</Button>
      </DialogFooter>
    </DialogContent>
  </Dialog>
}
