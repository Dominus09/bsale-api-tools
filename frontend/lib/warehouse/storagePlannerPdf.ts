import {jsPDF} from "jspdf"
import {type PlanDocument,type PlanMap,groups,isCurrent,restrictedPosition,compactFormat,kindLabel,methodLabel,mapViewport,productColor,productLabel,projectedGeometry,sortedElements,structureFill,baseKey,baseAssignments,summarizeAssignments,baseBadge,groupKey} from "./storagePlanner"

export function storagePlanPdf(name:string,version:number,doc:PlanDocument,maps:PlanMap[]){
  if(!maps.length||doc.slots.some(s=>!isCurrent(s,maps)))throw Error("Hay referencias de plano pendientes o desactualizadas.")
  const pdf=new jsPDF({orientation:"landscape",unit:"mm",format:"a3"})
  const allGroups=groups(doc)
  const pending=allGroups.reduce((n,g)=>n+g.remaining,0)
  let first=true
  const clean=(s:string)=>s.replace(/[\u0000-\u001f]/g," ")
  const header=(title:string,sub:string)=>{
    pdf.setTextColor(25);pdf.setFont("helvetica","bold");pdf.setFontSize(18);pdf.text(clean(title),12,14)
    pdf.setFont("helvetica","normal");pdf.setFontSize(10);pdf.text(clean(sub),12,21)
    pdf.setFontSize(9);pdf.text("MAPA OBJETIVO · NO INVENTARIO REAL · FEFO pendiente de verificar por pallet · No autoriza apilamiento",12,27)
  }
  const page=()=>{if(first)first=false;else pdf.addPage("a3","landscape")}
  const date=new Intl.DateTimeFormat("es-CL",{timeZone:"America/Santiago",dateStyle:"short"}).format(new Date())
  const textFit=(text:string,x:number,y:number,w:number,maxSize:number)=>{
    const size=Math.max(5,Math.min(maxSize,maxSize*w/Math.max(w,pdf.getTextWidth(text))))
    pdf.setFontSize(size)
    const lines=pdf.splitTextToSize(clean(text),Math.max(1,w)) as string[]
    pdf.text(lines[0]??"",x,y,{align:"center"})
  }
  for(const map of maps){
    const facility=map.bundle.facility?.name??"Bodega"
    const floorId=map.locations.floor.floor_id
    const floorSlots=doc.slots.filter(s=>s.floor_id===floorId)
    const bases=baseAssignments(floorSlots)
    const stats=summarizeAssignments(floorSlots)
    const levels=stats.n2>0?[1,2]:[1]
    for(const level of levels){
      page()
      header("Grupo La Quillotana | "+facility+" | Nivel "+level,name+" · v"+version+" · "+date+" · Plano "+map.bundle.revision.id+"/"+map.bundle.revision.row_version+" · Por asignar: "+pending)
      pdf.setFontSize(9);pdf.setFont("helvetica","bold")
      pdf.text("Ambos niveles: "+stats.pallets+" PAL | "+stats.bases+" bases | x1: "+stats.singleBases+" | x2: "+stats.doubleBases+" | N1: "+stats.n1+" | N2: "+stats.n2,14,32)
      const l=Number(map.bundle.floor?.length_m),w=Number(map.bundle.floor?.width_m)
      const bc=map.bundle.facility?.code==="BC"
      const scale=Math.min(392/(bc?l:w),220/(bc?w:l))
      const vp=mapViewport(map,scale,0);vp.offsetX=14;vp.offsetY=35
      const selected=floorSlots.filter(s=>s.level===level)
      const byPosition=new Map(selected.map(s=>[s.position_id,s]))
      for(const e of sortedElements(map)){
        const g=projectedGeometry(e.geometry,vp),type=e.physical.element_type
        const slot=byPosition.get(e.physical.id)
        const base=bases.get(baseKey({floor_id:floorId,position_id:e.physical.id}))??[]
        const position=map.locations.items.find(p=>p.id===e.physical.id)
        const blocked=type==="position"&&(!position||restrictedPosition(position)||(position.stack_levels_max!=null&&level>position.stack_levels_max))
        const fill=slot?productColor(slot.variant_id):blocked?"#DFB8B8":structureFill(type)
        pdf.setFillColor(fill);pdf.setDrawColor("#84909E");pdf.setLineWidth(type==="position"?0.25:0.35)
        const p=g.points
        pdf.lines(p.slice(1).map((q,i)=>[q.x-p[i].x,q.y-p[i].y]),p[0].x,p[0].y,[1,1],"FD",true)
        if(type==="position"){
          pdf.setTextColor(25);pdf.setFont("helvetica","bold");pdf.setFontSize(8)
          textFit(e.physical.code,g.center.x,g.y+3.4,g.w-1,8)
          if(slot){
            const prod=doc.products.find(x=>x.variant_id===slot.variant_id)
            pdf.setFont("helvetica","normal");pdf.setFontSize(7)
            const short=prod?.product_name??"SKU "+slot.variant_id
            textFit(short,g.center.x,g.y+6.8,g.w-1,7)
            textFit(compactFormat(prod),g.center.x,g.y+9.8,g.w-1,7)
          }else if(base.length){
            pdf.setFont("helvetica","normal");pdf.setFontSize(7)
            textFit("N"+level+" sin asignar",g.center.x,g.y+7.5,g.w-1,7)
          }
          if(base.length){
            const bw=Math.min(g.w-1.6,12),bh=4
            const badgeY=g.y+g.h-bh-0.8
            if(slot&&g.h>=19.8){
              pdf.setFont("helvetica","normal");pdf.setFontSize(6)
              textFit("N"+level+" "+(slot.kind==="partial"?"M":"C")+"-"+(slot.method==="rack"?"R":slot.method==="direct"?"D":"P"),g.center.x,g.y+12.5,g.w-1,6)
            }
            pdf.setFillColor(base.length===2?"#163B60":"#475569")
            pdf.roundedRect(g.center.x-bw/2,badgeY,bw,bh,0.8,0.8,"F")
            pdf.setFont("helvetica","bold");pdf.setFontSize(7);pdf.setTextColor(255)
            pdf.text(baseBadge(base),g.center.x,badgeY+2.9,{align:"center"})
          }
        }else if(!["facility_boundary","sector","aisle"].includes(type)&&g.w>8&&g.h>5){
          pdf.setFont("helvetica","normal");pdf.setFontSize(7);pdf.setTextColor(45)
          textFit(e.physical.code,g.center.x,g.center.y,g.w-1,7)
        }
      }
      pdf.setFont("helvetica","normal");pdf.setFontSize(9);pdf.setTextColor(30)
      const bottom=35+(bc?w:l)*scale
      pdf.text(bc?"FONDO IZQUIERDA · ENTRADA DERECHA":"FONDO ARRIBA · PATIO ABAJO · FILA 1 IZQUIERDA",14,bottom+7)
      pdf.text("x1 PAL / x2 PAL: total de la base (N1 + N2). No sumar distintivos entre los planos de niveles.",14,bottom+13)
      pdf.text("Blanco: sin asignar en el nivel visible, no necesariamente libre | Rosa: restringido | Ver detalle de productos y métodos en listado.",14,bottom+19)
      if(!bc)pdf.text("BL provisional: confirmar postes, banda de sensores, acceso y dimensiones en terreno antes de ejecutar.",14,bottom+25)
      if(bc)pdf.text("IZ: doble altura prevista para completos compatibles. El distintivo indica asignación, no autorización de apilamiento.",14,bottom+25)
    }
  }
  // Detailed, readable location-by-location work sheet; never relies on truncated map labels.
  for(const map of maps){
    const fid=map.locations.floor.floor_id
    const names=new Map(map.locations.items.map(p=>[p.id,p.code]))
    const baseMap=baseAssignments(doc.slots.filter(s=>s.floor_id===fid))
    const rows=doc.slots.filter(s=>s.floor_id===fid).sort((a,b)=>(names.get(a.position_id)??"").localeCompare(names.get(b.position_id)??"","es",{numeric:true})||a.level-b.level)
    let y=0
    const newListPage=()=>{page();header("Plan de trabajo | "+(map.bundle.facility?.name??"Bodega"),name+" · v"+version+" · "+date+" · Ubicación objetivo, confirmar físicamente antes de mover");y=39}
    newListPage()
    for(const slot of rows){
      const product=doc.products.find(p=>p.variant_id===slot.variant_id)
      const label="Base "+baseBadge(baseMap.get(baseKey(slot))??[])+" | "+productLabel(product)+" | "+(product?.product_type??"Tipo pendiente")+" | "+kindLabel(slot.kind)+" | "+methodLabel(slot.method)
      pdf.setFontSize(11);const lines=pdf.splitTextToSize(clean(label),325) as string[]
      const height=Math.max(8,lines.length*5+3)
      if(y+height>283)newListPage()
      pdf.setFont("helvetica","bold");pdf.text((names.get(slot.position_id)??"?")+" N"+slot.level,12,y)
      pdf.setFont("helvetica","normal");pdf.text(lines,71,y)
      pdf.setDrawColor(210);pdf.line(12,y+height-3,408,y+height-3);y+=height
    }
    if(!rows.length){pdf.setFontSize(11);pdf.text("Sin asignaciones en este escenario.",12,y)}
  }
  page();header("Consolidado de planificación",name+" · v"+version+" · "+date+" · Conteo agregado por variante y condición")
  let y=39
  for(const g of allGroups){
    pdf.setFontSize(11);const lines=pdf.splitTextToSize(clean(productLabel(g.product)+" | "+(g.product?.product_type??"Tipo pendiente")+" | "+kindLabel(g.kind)),285) as string[]
    const h=Math.max(16,lines.length*5+3)
    if(y+h>283){page();header("Consolidado de planificación (continuación)",name+" · v"+version+" · "+date);y=39}
    pdf.setTextColor(25);pdf.setFont("helvetica","normal");pdf.text(lines,12,y)
    const stat=summarizeAssignments(doc.slots.filter(s=>groupKey(s)===groupKey(g)))
    pdf.setFontSize(9)
    pdf.text("Contados: "+g.total+" | Asig: "+g.assigned+" | Pend: "+g.remaining,305,y)
    pdf.text("Bases: "+stat.bases+" | N1: "+stat.n1+" | N2: "+stat.n2,305,y+5)
    pdf.text("De este grupo: x1 "+stat.singleBases+" | x2 "+stat.doubleBases,305,y+9)
    pdf.setDrawColor(210);pdf.line(12,y+h-3,408,y+h-3);y+=h
  }
  if(!allGroups.length)pdf.text("Sin conteos ingresados.",12,y)
  return pdf
}
