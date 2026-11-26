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
