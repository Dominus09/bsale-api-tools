import type { PalletProduct, PalletLineInput, PalletRegisterInput } from "@/lib/warehouse/api"

export type Line = { key: string; lineId?: number; product: PalletProduct; verified: boolean; boxes: string; units: string; expiry: PalletLineInput["expiry_status"]; expires: string; lot: string }
export type PalletKind = PalletRegisterInput["pallet_kind"]

export function linePayload(l:Line):PalletLineInput {
  const boxes=l.verified?Number(l.boxes):null, units=l.verified?Number(l.units):null
  if(l.verified&&(!l.boxes.trim()||!l.units.trim()||![boxes,units].every(v=>v!==null&&Number.isInteger(v)&&v>=0&&v<=100000000))) throw new Error("Completa las cajas y unidades verificadas de cada línea.")
  if(l.verified&&(boxes??0)>0&&!l.product.units_per_box) throw new Error("La variante no tiene unidades por caja. Cuenta unidades o deja cantidad pendiente.")
  if(l.verified&&((boxes??0)*(l.product.units_per_box??0)+(units??0)<=0)) throw new Error("Cantidad verificada debe ser mayor que cero.")
  if(l.expiry==="dated"&&!l.expires) throw new Error("Ingresa la fecha de vencimiento de la línea.")
  return {variant_id:l.product.variant_id,quantity_status:l.verified?"verified":"unknown",boxes,loose_units:units,
    expected_units_per_box:l.product.units_per_box,expiry_status:l.expiry,expires_on:l.expiry==="dated"?l.expires:null,lot:l.lot.trim()||null}
}


export function validatePalletContents(kind:PalletKind,lines:Line[]):PalletLineInput[]{
  if(!lines.length||lines.length>100)throw new Error("Selecciona entre 1 y 100 líneas de productos reales.")
  const distinct=new Set(lines.map(l=>l.product.variant_id)).size
  if((kind==="mixed"&&distinct<2)||(kind!=="mixed"&&distinct!==1))throw new Error("Mixto requiere distintas variantes; completo/molido admite varios lotes de una variante.")
  return lines.map(linePayload)
}
