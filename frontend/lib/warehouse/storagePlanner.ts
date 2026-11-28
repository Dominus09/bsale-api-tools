import type { PalletLocations, PalletProduct } from "./api"
import type { RevisionBundle } from "./types"
import { metricPointToScreen, type ViewportTransform } from "./coords"

export type Kind = "complete" | "partial"
export type Method = "floor" | "direct" | "rack"
export type Count = { variant_id: number; source: string; kind: Kind; count: number }
export type Slot = { floor_id: number; revision_id: number; revision_version: number; position_id: number; level: number; variant_id: number; kind: Kind; method: Method }
export type PlanDocument = { counts: Count[]; slots: Slot[]; products: PalletProduct[] }
export type PlanInfo = { id: string; name: string; row_version: number; updated_at: string; updated_by: string }
export type SavedPlan = PlanInfo & { document: PlanDocument }
export type PlanMap = { bundle: RevisionBundle; locations: PalletLocations }
export type Group = { variant_id: number; kind: Kind; total: number; assigned: number; remaining: number; product: PalletProduct | undefined }
export const emptyPlan = (): PlanDocument => ({counts:[],slots:[],products:[]})
export const kindLabel = (k:Kind) => k==="complete"?"Completo":"Molido"
export const methodLabel = (m:Method) => m==="rack"?"Rack proyectado":m==="direct"?"Apilamiento directo":"Piso"
export const productLabel = (p?:PalletProduct) => p ? [p.product_name,p.variant_name].filter(Boolean).join(" · ") : "Variante no disponible"
export function compactFormat(p?:PalletProduct){
  if(!p)return "SKU pendiente"
  const s=p.variant_name??""
  const m=s.match(/\d+(?:[.,]\d+)?\s*(?:CC|ML|LITROS?|LTS?|L|KG|GR|G)\b/i)
  return m?.[0]??s.slice(0,16)
}
export const groupKey = (v:{variant_id:number;kind:Kind}) => v.variant_id+":"+v.kind
export const slotKey = (p:{floor_id:number;position_id:number;level:number}) => p.floor_id+":"+p.position_id+":"+p.level
export const palette = ["#C6A465","#88B2D6","#83C0A0","#E2A5AA","#B3A1D2","#91C7CB","#D9BA8D","#A7AEE3","#C1C985","#E3AE82","#9DBAD2","#C5B0C9"]
export const productColor = (id:number) => palette[Math.abs(id)%palette.length]
export function groups(doc:PlanDocument):Group[]{
  const totals=new Map<string,Group>()
  for(const row of doc.counts){
    const key=groupKey(row)
    const g=totals.get(key)??{variant_id:row.variant_id,kind:row.kind,total:0,assigned:0,remaining:0,product:doc.products.find(p=>p.variant_id===row.variant_id)}
    g.total+=row.count;totals.set(key,g)
  }
  for(const slot of doc.slots){const g=totals.get(groupKey(slot));if(g)g.assigned++}
  return [...totals.values()].map(g=>({...g,remaining:g.total-g.assigned})).sort((a,b)=>b.total-a.total||productLabel(a.product).localeCompare(productLabel(b.product),"es")||a.kind.localeCompare(b.kind))
}
export function changeCount(doc:PlanDocument, product:PalletProduct, source:string, complete:number, partial:number):PlanDocument{
  source=source.trim()
  if(!source||source.length>100||![complete,partial].every(n=>Number.isInteger(n)&&n>=0&&n<=1000))throw Error("Indique origen y cantidades enteras entre 0 y 1000.")
  const same=(r:Count)=>r.variant_id===product.variant_id&&r.source.toLowerCase()===source.toLowerCase()
  const counts=doc.counts.filter(r=>!same(r))
  if(complete)counts.push({variant_id:product.variant_id,source,kind:"complete",count:complete})
  if(partial)counts.push({variant_id:product.variant_id,source,kind:"partial",count:partial})
  const next={...doc,counts,products:[...doc.products.filter(p=>p.variant_id!==product.variant_id),product]}
  if(groups(next).some(g=>g.remaining<0)||next.slots.some(s=>!counts.some(r=>groupKey(r)===groupKey(s))))throw Error("Retire primero asignaciones que superan el nuevo conteo.")
  return next
}
export function restrictedPosition(p:PalletLocations["items"][number]){
  const meta={...p.physical_meta_snap,...p.constraints_json,...p.meta_json}
  return p.code.startsWith("BAE-")||meta.under_stair===true||meta.storage_kind==="special_restricted"||meta.storage_forbidden_assumed===true||meta.not_storage===true
}
export function isCurrent(slot:Slot,maps:PlanMap[]){
  const map=maps.find(m=>m.locations.floor.floor_id===slot.floor_id)
  const p=map?.locations.items.find(x=>x.id===slot.position_id)
  return !!map&&!!p&&!restrictedPosition(p)&&slot.revision_id===map.locations.revision?.id&&slot.revision_version===map.locations.revision?.row_version&&(p.stack_levels_max==null||slot.level<=p.stack_levels_max)
}
export function orderedPositions(map:PlanMap, group:string){
  const elements=map.bundle.elements
  const byId=new Map(elements.map(e=>[e.physical.id,e]))
  const parent=map.bundle.facility?.code
  return map.locations.items.filter(p=>!restrictedPosition(p)&&(!group||String(byId.get(p.id)?.physical.parent_id)===group)).sort((a,b)=>{
    const ea=byId.get(a.id),eb=byId.get(b.id)
    const ga=ea?.geometry,gb=eb?.geometry
    if(parent==="BC"&&a.code.startsWith("IZ-")&&b.code.startsWith("IZ-"))return b.code.localeCompare(a.code,"es",{numeric:true})
    if(parent==="BL"&&ga&&gb)return ga.y_m-gb.y_m||gb.x_m-ga.x_m
    return a.code.localeCompare(b.code,"es",{numeric:true})
  })
}
export function assignSlots(doc:PlanDocument,map:PlanMap,ids:number[],level:number,method:Method,variantId:number,kind:Kind):PlanDocument{
  if(level!==1&&level!==2)throw Error("Nivel inválido.")
  const rev=map.locations.revision
  if(!rev)throw Error("No hay plano vigente.")
  const g=groups(doc).find(x=>x.variant_id===variantId&&x.kind===kind)
  if(!g||g.remaining<1)throw Error("No quedan pallets de esta variante/condición por asignar.")
  if(new Set(ids).size>g.remaining)throw Error("La selección supera los pallets pendientes.")
  const floor=map.locations.floor.floor_id
  const slots=[...doc.slots]
  for(const id of [...new Set(ids)]){
    const p=map.locations.items.find(x=>x.id===id)
    if(!p||restrictedPosition(p)||(p.stack_levels_max!=null&&level>p.stack_levels_max))throw Error("Posición restringida o nivel no permitido.")
    if(slots.some(s=>s.floor_id===floor&&s.position_id===id&&s.level===level))throw Error("Una posición seleccionada ya está asignada.")
    if(method==="direct"&&kind!=="complete")throw Error("No apilar directamente pallets molidos.")
    if(level===2){
      const lower=slots.find(s=>s.floor_id===floor&&s.position_id===id&&s.level===1)
      if(!lower||lower.method!==method||method==="floor")throw Error("Nivel 2 requiere una base del mismo sistema.")
      if(method==="direct"&&(lower.kind!=="complete"||lower.variant_id!==variantId))throw Error("Apilamiento directo: misma variante y ambos completos.")
    }
    slots.push({floor_id:floor,revision_id:rev.id,revision_version:rev.row_version,position_id:id,level,variant_id:variantId,kind,method})
  }
  return {...doc,slots}
}
export function clearSlots(doc:PlanDocument,floorId:number,ids:number[],level:number):PlanDocument{
  const matches=(s:Slot)=>s.floor_id===floorId&&ids.includes(s.position_id)
  if(level===1&&doc.slots.some(s=>matches(s)&&s.level===2))throw Error("Retire primero las asignaciones del nivel 2.")
  return {...doc,slots:doc.slots.filter(s=>!(matches(s)&&s.level===level))}
}
/** Draft suggestion: volume first, selected group only; no stacking without explicit method. */
export function suggest(doc:PlanDocument,map:PlanMap,group:string,method:Method,maxLevel:number){
  let next=doc
  const ids=orderedPositions(map,group).map(p=>p.id)
  const floor=map.locations.floor.floor_id
  for(const g of groups(doc)){
    if(method==="direct"&&g.kind==="partial")continue
    for(const id of ids){
      for(let level=1;level<=maxLevel;level++){
        if(!groups(next).find(x=>groupKey(x)===groupKey(g))?.remaining)break
        if(next.slots.some(s=>s.floor_id===floor&&s.position_id===id&&s.level===level))continue
        try{next=assignSlots(next,map,[id],level,method,g.variant_id,g.kind)}catch{/* incompatible position; leave unassigned */}
      }
    }
  }
  return next
}
export function mapViewport(map:PlanMap,scale:number,offset=12):ViewportTransform{
  return {scale,offsetX:offset,offsetY:offset,floorLengthM:Number(map.bundle.floor?.length_m),floorWidthM:Number(map.bundle.floor?.width_m),viewOrientationDeg:map.bundle.facility?.code==="BC"?90:0}
}
export function projectedGeometry(g:RevisionBundle["elements"][number]["geometry"],vp:ViewportTransform){
  const cx=g.x_m+g.width_m/2,cy=g.y_m+g.depth_m/2
  const r=g.rotation_deg*Math.PI/180
  const points=[[g.x_m,g.y_m],[g.x_m+g.width_m,g.y_m],[g.x_m+g.width_m,g.y_m+g.depth_m],[g.x_m,g.y_m+g.depth_m]].map(([x,y])=>metricPointToScreen(cx+(x-cx)*Math.cos(r)-(y-cy)*Math.sin(r),cy+(x-cx)*Math.sin(r)+(y-cy)*Math.cos(r),vp))
  const xs=points.map(p=>p.x),ys=points.map(p=>p.y)
  return {points,x:Math.min(...xs),y:Math.min(...ys),w:Math.max(...xs)-Math.min(...xs),h:Math.max(...ys)-Math.min(...ys),center:metricPointToScreen(cx,cy,vp)}
}
export function structureFill(type:string){
  return type==="facility_boundary"?"#F5F6F8":type==="position"?"#FFFFFF":type==="circulation"?"#DDE3EA":type==="unusable"?"#E5CCCC":type==="aisle"||type==="sector"?"#E9EDF1":"#CED2D7"
}
export function sortedElements(map:PlanMap){
  const rank:Record<string,number>={facility_boundary:0,sector:1,aisle:2,circulation:3,position:5}
  return [...map.bundle.elements].sort((a,b)=>(rank[a.physical.element_type]??4)-(rank[b.physical.element_type]??4))
}
