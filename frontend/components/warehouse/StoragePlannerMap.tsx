"use client"
import {Button} from "@/components/ui/button"
import {type PlanMap,type PlanDocument,type Method,type Group,orderedPositions,restrictedPosition,mapViewport,projectedGeometry,sortedElements,structureFill,compactFormat,productColor,productLabel,kindLabel,methodLabel} from "@/lib/warehouse/storagePlanner"
import {useState} from "react"

type Props={maps:PlanMap[];doc:PlanDocument;chosen:Group|undefined;onAssign:(map:PlanMap,ids:number[],level:number,method:Method)=>void;onRemove:(map:PlanMap,ids:number[],level:number)=>void;onSuggest:(map:PlanMap,area:string,method:Method)=>void}
const selectClass="h-9 rounded-md border border-input bg-background px-2 text-sm"
export function StoragePlannerMap({maps,doc,chosen,onAssign,onRemove,onSuggest}:Props){
  const [floor,setFloor]=useState(0)
  const [area,setArea]=useState("")
  const [level,setLevel]=useState(1)
  const [method,setMethod]=useState<Method>("floor")
  const [selected,setSelected]=useState<number[]>([])
  const map=maps.find(m=>m.locations.floor.floor_id===floor)??maps[0]
  if(!map)return <p>Sin planos disponibles.</p>
  const fid=map.locations.floor.floor_id
  const bc=map.bundle.facility?.code==="BC"
  const vp=mapViewport(map,56,16)
  const w=(bc?vp.floorLengthM:vp.floorWidthM)*vp.scale+32
  const h=(bc?vp.floorWidthM:vp.floorLengthM)*vp.scale+32
  const positions=orderedPositions(map,area)
  const eligible=new Set(positions.map(p=>p.id))
  const areas=map.bundle.elements.filter(e=>["sector","aisle"].includes(e.physical.element_type)&&map.bundle.elements.some(x=>x.physical.parent_id===e.physical.id&&x.physical.element_type==="position"))
  const slots=doc.slots.filter(s=>s.floor_id===fid&&s.level===level)
  const occupied=new Map(slots.map(s=>[s.position_id,s]))
  const toggle=(id:number)=>setSelected(old=>old.includes(id)?old.filter(x=>x!==id):[...old,id])
  return <section className="min-w-0 space-y-3 rounded-lg border bg-card p-3">
    <h2 className="font-semibold">3. Mapa objetivo editable</h2>
    <div className="flex flex-wrap gap-2">
      <select aria-label="Bodega" className={selectClass} value={fid} onChange={e=>{setFloor(Number(e.target.value));setArea("");setSelected([])}}>{maps.map(m=><option value={m.locations.floor.floor_id} key={m.locations.floor.floor_id}>{m.bundle.facility?.name}</option>)}</select>
      <select aria-label="Sector" className={selectClass} value={area} onChange={e=>{setArea(e.target.value);setSelected([])}}><option value="">Todos los sectores</option>{areas.map(e=><option key={e.physical.id} value={e.physical.id}>{e.physical.code}</option>)}</select>
      <select aria-label="Nivel" className={selectClass} value={level} onChange={e=>{setLevel(Number(e.target.value));setSelected([])}}><option value={1}>Nivel 1</option><option value={2}>Nivel 2 (simulación)</option></select>
      <select aria-label="Sistema" className={selectClass} value={method} onChange={e=>setMethod(e.target.value as Method)}><option value="floor">Piso</option><option value="direct">Directo simulado</option><option value="rack">Rack proyectado</option></select>
    </div>
    <p className="text-xs text-muted-foreground">{bc?"BC: fondo izquierda, entrada derecha.":"BL: fila 1 izquierda, patio abajo; geometría provisional."} Nivel 2 no autorizado por el sistema. Fechas individuales pendientes.</p>
    <div className="max-h-[600px] overflow-auto rounded border bg-white">
      <svg aria-label="Plano objetivo" viewBox={"0 0 "+w+" "+h} width={w} height={h} style={{display:"block",minWidth:w}}>
        {sortedElements(map).map(e=>{
          const g=projectedGeometry(e.geometry,vp),typ=e.physical.element_type,p=map.locations.items.find(x=>x.id===e.physical.id),slot=occupied.get(e.physical.id),prod=doc.products.find(x=>x.variant_id===slot?.variant_id)
          const usable=typ==="position"&&!!p&&!restrictedPosition(p)&&(p.stack_levels_max==null||level<=p.stack_levels_max)
          const selectable=usable&&eligible.has(e.physical.id)
          const sel=selected.includes(e.physical.id)
          const fill=slot?productColor(slot.variant_id):typ==="position"&&!usable?"#DFB8B8":structureFill(typ)
          const title=e.physical.code+(slot?" | "+productLabel(prod)+" | "+kindLabel(slot.kind)+" | "+methodLabel(slot.method):" | Sin asignar en este escenario")
          return <g key={e.physical.id} style={{cursor:selectable?"pointer":"default",opacity:typ==="position"&&area&&!eligible.has(e.physical.id)?0.5:1}} onClick={selectable?()=>toggle(e.physical.id):undefined} role={selectable?"button":undefined} tabIndex={selectable?0:undefined} onKeyDown={selectable?ev=>{if(ev.key==="Enter"||ev.key===" "){ev.preventDefault();toggle(e.physical.id)}}:undefined} aria-label={selectable?title:undefined}>
            <title>{title}</title><polygon points={g.points.map(p=>p.x+","+p.y).join(" ")} fill={fill} stroke={sel?"#B91C1C":"#84909E"} strokeWidth={sel?3:typ==="position"?1:0.7}/>
            {typ==="position"&&<><text x={g.center.x} y={g.center.y-12} fontSize={Math.min(11,g.w/Math.max(4,e.physical.code.length)*1.6)} fontWeight="bold" textAnchor="middle" fill="#182432">{e.physical.code}</text>{slot&&<><text x={g.center.x} y={g.center.y} fontSize="8" textAnchor="middle" fill="#182432">{(prod?.product_name??"SKU").slice(0,Math.max(6,Math.floor(g.w/4.4)))}</text><text x={g.center.x} y={g.center.y+10} fontSize="8" textAnchor="middle" fill="#182432">{compactFormat(prod).slice(0,Math.max(6,Math.floor(g.w/4.4)))}</text><text x={g.center.x} y={g.center.y+20} fontSize="8" textAnchor="middle" fill="#182432">{slot.kind==="partial"?"M":"C"} · {slot.method==="rack"?"R":slot.method==="direct"?"D":"P"}</text></>}</>}
            {!["facility_boundary","sector","aisle","position"].includes(typ)&&g.w>28&&g.h>18&&<text x={g.center.x} y={g.center.y} fontSize="9" textAnchor="middle" fill="#394655">{e.physical.code.slice(0,18)}</text>}
          </g>
        })}
      </svg>
    </div>
    <div className="flex flex-wrap items-center gap-2">
      <span className="text-xs text-muted-foreground">{selected.length} seleccionadas</span>
      <Button variant="outline" size="sm" disabled={!chosen||!area} onClick={()=>setSelected(positions.filter(p=>!occupied.has(p.id)&&(p.stack_levels_max==null||level<=p.stack_levels_max)).map(p=>p.id).slice(0,chosen?.remaining??0))}>Seleccionar bloque</Button>
      <Button variant="outline" size="sm" onClick={()=>setSelected([])}>Deseleccionar</Button>
      <Button size="sm" disabled={!selected.length||!chosen} onClick={()=>{onAssign(map,selected,level,method);setSelected([])}}>Asignar</Button>
      <Button variant="outline" size="sm" disabled={!selected.length} onClick={()=>{onRemove(map,selected,level);setSelected([])}}>Retirar</Button>
      <Button variant="outline" size="sm" disabled={!area} onClick={()=>{onSuggest(map,area,method);setSelected([])}}>Sugerir por volumen</Button>
    </div>
    <p className="text-xs text-muted-foreground">Seleccione posiciones y asigne; retire y reasigne para cambiar destino. Blanco: sin asignar, no necesariamente libre. C/M: completo/molido; P/D/R: piso/directo/rack.</p>
    {!!slots.length&&<details><summary className="cursor-pointer text-sm">Destinos nivel {level} ({slots.length})</summary><div className="max-h-64 overflow-auto">{slots.map(s=><div className="border-b py-1 text-xs" key={s.position_id}><strong>{map.locations.items.find(p=>p.id===s.position_id)?.code??"Posición anterior"}</strong> · {productLabel(doc.products.find(p=>p.variant_id===s.variant_id))} · {kindLabel(s.kind)} · {methodLabel(s.method)}</div>)}</div></details>}
  </section>
}
