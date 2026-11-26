import type { jsPDF } from "jspdf"
import type { PalletLabelBatch,PalletLabelData } from "@/lib/warehouse/api"
import { labelRect, summarizePalletLabel, qrRuns, type LabelPerSheet } from "./palletLabels"

const clean=(text:string)=>text.replace(/[\x00-\x1f\x7f]/g," ").replace(/\s+/g," ").trim()
function fit(doc:jsPDF,value:string,x:number,y:number,width:number,preferred:number,min:number,bold=false,align:"left"|"center"="center"){
  let text=clean(value)
  doc.setFont("helvetica",bold?"bold":"normal")
  let size=preferred
  doc.setFontSize(size)
  while(size>min&&doc.getTextWidth(text)>width){size=Math.max(min,size-.5);doc.setFontSize(size)}
  if(doc.getTextWidth(text)>width){
    while(text.length&&doc.getTextWidth(text+"...")>width)text=text.slice(0,-1)
    text=text.trimEnd()+"..."
  }
  doc.text(text,x,y,{align,baseline:"middle"})
}

function drawLabel(doc:jsPDF,label:PalletLabelData,index:number,format:LabelPerSheet,generatedAt:string){
  const {x,y,width:w,height:h}=labelRect(index,format),cx=x+w/2
  const s=summarizePalletLabel(label)
  const narrow=format===4,large=format===1
  const fontScale=large?1.65:narrow?1:1.15
  const pad=narrow?4:6
  const usable=w-pad*2
  const center=(text:string,fy:number,size:number,min:number,bold=false)=>fit(doc,text,cx,y+h*fy,usable,size*fontScale,min,bold)
  doc.setFillColor(255,255,255);doc.setDrawColor(0,0,0);doc.setTextColor(0,0,0)
  doc.setLineWidth(.35);doc.rect(x,y,w,h,"FD")
  center("GRUPO LA QUILLOTANA",.055,11,8,true)
  doc.setLineWidth(.25);doc.line(x+pad,y+h*.09,x+w-pad,y+h*.09)
  center(label.code,.162,25,16,true)
  center(s.product,.254,15,9,true)
  center(s.variant,.309,12.5,8,true)
  center(s.type,.36,10,7)
  // Vencimiento grande, con aviso explícito cuando existen fechas pendientes.
  const bx=x+pad,by=y+h*.404,bw=w-pad*2,bh=h*.232
  doc.setLineWidth(.6);doc.rect(bx,by,bw,bh)
  center("VENCIMIENTO",.439,10.5,8,true)
  center(s.expiry,.513,28,15,true)
  center(s.expiryNote,.598,9,7,true)
  // QR vectorial, negro sobre blanco, con sus cuatro módulos de margen.
  const q=qrRuns(label)
  const qrSize=Math.min(h*.263,w*.43,large?74:45)
  const qx=x+pad,qy=y+h*.654
  const module=qrSize/q.size
  doc.setFillColor(255,255,255);doc.rect(qx,qy,qrSize,qrSize,"F")
  doc.setFillColor(0,0,0)
  for(const run of q.runs)doc.rect(qx+run.x*module,qy+run.y*module,run.length*module,module,"F")
  const tx=qx+qrSize+pad*.6,tw=x+w-pad-tx
  const left=(text:string,fy:number,size:number,min:number,bold=false)=>fit(doc,text,tx,y+h*fy,tw,size*fontScale,min,bold,"left")
  left("PALLET "+s.kind.toUpperCase(),.681,10,7,true)
  left(s.qtyText,.725,10,7,true)
  if(s.qtyNote)left(s.qtyNote,.762,8,6.5)
  left("Lote: "+s.lotText,.807,9,7)
  left("Ingreso: "+(label.received_on?label.received_on.slice(0,10).split("-").reverse().join("/"):"Pendiente"),.845,8,6.5)
  left("Escanear en ERP",.889,9,7,true)
  doc.setLineWidth(.2);doc.line(x+pad,y+h*.935,x+w-pad,y+h*.935)
  const generated=new Intl.DateTimeFormat("es-CL",{timeZone:"America/Santiago",day:"2-digit",month:"2-digit",year:"numeric"}).format(new Date(generatedAt))
  center("Datos al "+generated+" · Actualizar etiqueta si cambian los datos",.963,7.7,6.5)
}

export async function createPalletLabelsPdf(batch:PalletLabelBatch,perSheet:LabelPerSheet):Promise<jsPDF>{
  if(!batch.items.length)throw new Error("No hay etiquetas para generar.")
  if(new Set(batch.items.map(p=>p.id)).size!==batch.items.length)throw new Error("Selección duplicada.")
  const {jsPDF}=await import("jspdf")
  const doc=new jsPDF({orientation:"portrait",unit:"mm",format:"a4",compress:true,precision:5})
  doc.setProperties({title:"Etiquetas de pallets - Grupo La Quillotana",subject:"Identificación física PAL"})
  batch.items.forEach((label,index)=>{
    if(index>0&&index%perSheet===0)doc.addPage("a4","portrait")
    drawLabel(doc,label,index,perSheet,batch.generated_at)
  })
  return doc
}
