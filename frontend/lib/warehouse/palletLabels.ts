import type { PalletLabelData, PalletLocations } from "@/lib/warehouse/api"

export const MAX_LABEL_PALLETS=120
export type LabelPerSheet=1|2|4
export const LABEL_FORMATS:LabelPerSheet[]=[1,2,4]
export function palletIdFromCode(value:string):number|null{
  const code=value.trim().toUpperCase()
  if(!/^PAL-\d{6,16}$/.test(code))return null
  const id=Number(code.slice(4))
  return Number.isSafeInteger(id)&&id>0&&code==="PAL-"+String(id).padStart(6,"0")?id:null
}
export const formatLabelDate=(value:string|null)=>value?value.slice(0,10).split("-").reverse().join("/"):"Sin verificar"
const num=(n:number)=>n.toLocaleString("es-CL")
const kindLabels={complete:"Completo",partial:"Molido",mixed:"Mixto"}

export function summarizePalletLabel(label:PalletLabelData){
  const lines=label.lines
  if(!lines.length)throw new Error(label.code+": no contiene líneas.")
  const variants=new Set(lines.map(l=>l.variant_id))
  const first=lines[0]
  const mixed=variants.size>1||label.pallet_kind==="mixed"
  const dates=[...new Set(lines.filter(l=>l.expiry_status==="dated"&&l.expires_on).map(l=>l.expires_on!))].sort()
  const pendingExpiry=lines.some(l=>l.expiry_status==="unknown"||(l.expiry_status==="dated"&&!l.expires_on))
  const expiry=dates.length?formatLabelDate(dates[0]):pendingExpiry?"SIN VERIFICAR":"NO APLICA"
  const expiryNote=dates.length?(pendingExpiry?"OTRAS FECHAS PENDIENTES":dates.length>1?"Fecha más próxima · "+dates.length+" fechas":"Fecha registrada"):
    pendingExpiry?"Revisar envase y actualizar":"No aplica confirmado"
  const lots=[...new Set(lines.map(l=>l.lot?.trim()).filter((l):l is string=>!!l))]
  const pendingLot=lines.some(l=>!l.lot?.trim())
  const lotText=!lots.length?"Sin verificar":lots.length===1&&!pendingLot?lots[0]:lots.length+" lote(s)"+(pendingLot?" + pendientes":" · ver ficha")
  const knownQty=lines.every(l=>l.quantity_status==="verified"&&l.total_units!==null)
  const total=knownQty&&!mixed?lines.reduce((n,l)=>n+(l.total_units??0),0):null
  const safeTotal=total!==null&&Number.isSafeInteger(total)
  const packs=new Set(lines.map(l=>l.units_per_box_snap))
  const boxTotal=lines.reduce((n,l)=>n+(l.boxes??0),0)
  const looseTotal=lines.reduce((n,l)=>n+(l.loose_units??0),0)
  const qtyText=mixed?"Por variante · ver QR":!knownQty?"Pendiente de conteo":safeTotal?num(total!)+" un.":"Consultar ficha"
  const qtyNote=mixed?(knownQty?"Conteos registrados":"Conteos pendientes"):!knownQty?(lines.some(l=>l.total_units!==null)?"Conteo parcial":"Sin cantidad verificada"):
    safeTotal&&packs.size===1&&first.units_per_box_snap&&Number.isSafeInteger(boxTotal)&&Number.isSafeInteger(looseTotal)?
      num(boxTotal)+" cajas + "+num(looseTotal)+" sueltas":""
  return {
    kind:kindLabels[label.pallet_kind],
    product:mixed?"PALLET MIXTO":first.product_name_snap,
    variant:mixed?variants.size+" variantes · ver detalle QR":first.variant_name_snap||"Variante sin nombre",
    type:mixed?"Diferentes productos / variantes":first.product_type_snap||"Sin tipo",
    expiry,expiryNote,pendingExpiry,qtyText,qtyNote,lotText,lotPending:pendingLot,
  }
}

// Grupo derivado exclusivamente de la relación padre de la revisión guardada.
// Incluye todos los niveles activos de ese pasillo/sector; no depende de paginación.
export function labelGroups(locations:PalletLocations){
  const parents=new Map(locations.items.filter(p=>p.parent_id!=null&&p.parent_code).map(p=>[p.id,{id:p.parent_id!,code:p.parent_code!}]))
  const groups=new Map<number,{id:number;code:string;ids:number[]}>()
  for(const slot of locations.observed_slots){
    if(!slot.pallet_id)continue
    const parent=parents.get(slot.position_id)
    if(!parent)continue
    const g=groups.get(parent.id)??{...parent,ids:[]}
    if(!g.ids.includes(slot.pallet_id))g.ids.push(slot.pallet_id)
    groups.set(g.id,g)
  }
  return [...groups.values()].sort((a,b)=>a.code.localeCompare(b.code,"es",{numeric:true}))
}

export function labelPageLayout(perSheet:LabelPerSheet){
  if(!LABEL_FORMATS.includes(perSheet))throw new Error("Formato de hoja inválido.")
  const cols=perSheet===4?2:1,rows=perSheet===1?1:2
  const margin=8,gap=4,pageWidth=210,pageHeight=297
  return {cols,rows,margin,gap,pageWidth,pageHeight,
    width:(pageWidth-margin*2-gap*(cols-1))/cols,
    height:(pageHeight-margin*2-gap*(rows-1))/rows}
}
export function labelRect(index:number,perSheet:LabelPerSheet){
  const l=labelPageLayout(perSheet),slot=index%perSheet
  return {page:Math.floor(index/perSheet),x:l.margin+(slot%l.cols)*(l.width+l.gap),
    y:l.margin+Math.floor(slot/l.cols)*(l.height+l.gap),width:l.width,height:l.height}
}

// Validar estructura y zona de silencio de 4 módulos. El servidor genera el QR
// con qrcode; no renderizar matrices incompletas ni etiquetas mal asociadas.
export function qrRuns(label:PalletLabelData){
  if(palletIdFromCode(label.code)!==label.id||label.qr_value!==label.code)throw new Error("Identidad QR no coincide.")
  const m=label.qr_matrix,n=m.length
  if(n<29||n>185||(n-29)%4!==0||m.some(row=>row.length!==n||!/^[01]+$/.test(row)))throw new Error("Matriz QR inválida.")
  const runs:{x:number;y:number;length:number}[]=[]
  for(let y=0;y<n;y++){
    for(let x=0;x<n;x++){
      if(m[y][x]!=="1")continue
      if(x<4||y<4||x>=n-4||y>=n-4)throw new Error("QR sin margen de seguridad.")
      const start=x
      while(x+1<n&&m[y][x+1]==="1"){
        x++
        if(x>=n-4)throw new Error("QR sin margen de seguridad.")
      }
      runs.push({x:start,y,length:x-start+1})
    }
  }
  if(!runs.length)throw new Error("QR vacío.")
  return {size:n,runs}
}
