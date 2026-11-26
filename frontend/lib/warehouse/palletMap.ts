/** Estado de ocupación observada; sin registro nunca equivale a libre. */
export type ObservedSlot = {position_id:number;observed_level:number;pallet_id?:number;pallet_code?:string}
export function slotsAtLevel(slots:ObservedSlot[],level:number):Map<number,ObservedSlot>{
  if(!Number.isInteger(level)||level<1||level>100)return new Map()
  return new Map(slots.filter(s=>s.observed_level===level).map(s=>[s.position_id,s]))
}
export function observedSlotState(slot:ObservedSlot|undefined,excludedPalletId?:number):"unrecorded"|"own"|"occupied"{
  if(!slot)return "unrecorded"
  return excludedPalletId!==undefined&&slot.pallet_id===excludedPalletId?"own":"occupied"
}


export const MAX_BATCH_PALLETS = 120
export const MAX_BATCH_LINES = 500

export type BatchLocation = {id:number;code:string;physical_meta_snap:Record<string,unknown>;constraints_json:Record<string,unknown>;meta_json:Record<string,unknown>}
export function restrictedBatchLocation(p:BatchLocation):boolean{
  const meta={...p.physical_meta_snap,...p.constraints_json,...p.meta_json}
  return p.code.startsWith("BAE-")||meta.under_stair===true||meta.storage_kind==="special_restricted"
}
export function batchSelectable(p:BatchLocation,slots:Map<number,ObservedSlot>):boolean{
  return !restrictedBatchLocation(p)&&!slots.has(p.id)
}
export function toggleBatchGroup(selected:number[],eligible:number[],limit=MAX_BATCH_PALLETS):number[]{
  const unique=[...new Set(eligible)]
  if(!unique.length)return selected
  if(unique.every(id=>selected.includes(id)))return selected.filter(id=>!unique.includes(id))
  return [...new Set([...selected,...unique])].slice(0,limit)
}


export type BatchGroupElement = {physical:{id:number;parent_id:number|null;element_type:string;code:string}}
export function groupBatchPositions(elements:BatchGroupElement[],positionIds:ReadonlySet<number>){
  const groups=new Map<number,{id:number;code:string;ids:number[]}>()
  const parents=new Map(elements.filter(e=>e.physical.element_type==="aisle"||e.physical.element_type==="sector").map(e=>[e.physical.id,e.physical]))
  for(const e of elements){
    if(e.physical.element_type!=="position"||!positionIds.has(e.physical.id))continue
    const parent=e.physical.parent_id==null?undefined:parents.get(e.physical.parent_id)
    if(!parent)continue
    const g=groups.get(parent.id)??{id:parent.id,code:parent.code,ids:[]}
    g.ids.push(e.physical.id);groups.set(parent.id,g)
  }
  return [...groups.values()].sort((a,b)=>a.code.localeCompare(b.code,"es",{numeric:true}))
}
