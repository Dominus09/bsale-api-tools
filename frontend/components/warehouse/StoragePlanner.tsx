"use client"
import {useEffect,useMemo,useState} from "react"
import {Button} from "@/components/ui/button"
import {Input} from "@/components/ui/input"
import {StoragePlannerMap} from "@/components/warehouse/StoragePlannerMap"
import {StoragePlannerCount} from "@/components/warehouse/StoragePlannerCount"
import {listWarehouseFacilities,listWarehouseFloors,listPalletLocations,getWarehouseRevision} from "@/lib/warehouse/api"
import {listStoragePlans,getStoragePlan,saveStoragePlan} from "@/lib/warehouse/storagePlannerApi"
import {type PlanDocument,type PlanMap,type PlanInfo,type Method,emptyPlan,groups,groupKey,kindLabel,productLabel,productColor,assignSlots,clearSlots,suggest,isCurrent} from "@/lib/warehouse/storagePlanner"

const message=(e:unknown)=>e instanceof Error?e.message:"No se pudo completar la operación."
async function loadMaps():Promise<PlanMap[]>{
  const facilities=(await listWarehouseFacilities()).filter(f=>["BC","BL"].includes(f.code))
  const floors=(await Promise.all(facilities.map(f=>listWarehouseFloors(f.id)))).flat().filter(f=>f.code==="PISO1")
  const maps=await Promise.all(floors.map(async floor=>{
    const locations=await listPalletLocations(floor.id)
    if(!locations.revision)throw Error("Falta guardar el plano de "+locations.floor.facility_code)
    const bundle=await getWarehouseRevision(locations.revision.id)
    if(bundle.revision.row_version!==locations.revision.row_version||bundle.floor?.id!==floor.id)throw Error("El plano cambió durante la consulta. Recargue.")
    if(!(Number(bundle.floor?.length_m)>0&&Number(bundle.floor?.width_m)>0))throw Error("Dimensiones de plano inválidas.")
    const ids=new Set(bundle.elements.filter(e=>e.physical.element_type==="position").map(e=>e.physical.id))
    if(locations.items.some(p=>!ids.has(p.id)))throw Error("Posiciones inconsistentes con plano.")
    return {bundle,locations}
  }))
  return maps.sort((a,b)=>(a.bundle.facility?.code??"").localeCompare(b.bundle.facility?.code??""))
}

export function StoragePlanner(){
  const [maps,setMaps]=useState<PlanMap[]>([])
  const [plans,setPlans]=useState<PlanInfo[]>([])
  const [doc,setDoc]=useState<PlanDocument>(emptyPlan)
  const [id,setId]=useState("")
  const [name,setName]=useState("Ordenamiento de bodegas")
  const [version,setVersion]=useState(0)
  const [dirty,setDirty]=useState(false)
  const [busy,setBusy]=useState(false)
  const [countBusy,setCountBusy]=useState(false)
  const [loading,setLoading]=useState(true)
  const [error,setError]=useState("")
  const [notice,setNotice]=useState("")
  const [selectedGroup,setSelectedGroup]=useState("")
  const catalogFloor=maps.find(m=>m.locations.floor.facility_code==="BC")?.locations.floor.floor_id??maps[0]?.locations.floor.floor_id
  const origins=maps.flatMap(m=>m.bundle.elements.filter(e=>["sector","aisle"].includes(e.physical.element_type)&&m.bundle.elements.some(x=>x.physical.parent_id===e.physical.id&&x.physical.element_type==="position")).map(e=>m.locations.floor.facility_code+" / "+e.physical.code.replace(/^BL-/,"")))
  const consolidated=useMemo(()=>groups(doc),[doc])
  const stale=doc.slots.filter(s=>!isCurrent(s,maps))
  const total=consolidated.reduce((n,g)=>n+g.total,0)
  const chosen=consolidated.find(g=>groupKey(g)===selectedGroup)
  const update=(next:PlanDocument)=>{setDoc(next);setDirty(true);setError("")}
  const run=async(fn:()=>Promise<void>)=>{setBusy(true);setError("");setNotice("");try{await fn()}catch(e){setError(message(e))}finally{setBusy(false)}}
  useEffect(()=>{
    let live=true
    Promise.all([loadMaps(),listStoragePlans()]).then(([m,p])=>{
      if(!live)return
      setMaps(m);setPlans(p);setId(crypto.randomUUID())
      if(!m.some(x=>x.locations.floor.facility_code==="BL"))setNotice("Bodega de Lata todavía no tiene plano: falta migración 011.")
    }).catch(e=>{if(live)setError(message(e))}).finally(()=>{if(live)setLoading(false)})
    return()=>{live=false}
  },[])
  useEffect(()=>{
    if(!dirty)return
    const fn=(e:BeforeUnloadEvent)=>{e.preventDefault();e.returnValue=""}
    window.addEventListener("beforeunload",fn)
    return()=>window.removeEventListener("beforeunload",fn)
  },[dirty])
  const open=(planId:string)=>run(async()=>{
    if(!planId||planId===id)return
    if(dirty&&!window.confirm("¿Descartar cambios sin guardar?"))return
    const p=await getStoragePlan(planId)
    setId(p.id);setName(p.name);setVersion(p.row_version);setDoc(p.document);setDirty(false);setSelectedGroup("")
  })
  const create=()=>{
    if(dirty&&!window.confirm("¿Descartar cambios sin guardar?"))return
    setId(crypto.randomUUID());setName("Ordenamiento de bodegas");setVersion(0);setDoc(emptyPlan());setDirty(false);setSelectedGroup("")
  }
  const save=(copy=false)=>run(async()=>{
    if(stale.length)throw Error("Retire las asignaciones desactualizadas y reasigne antes de guardar.")
    const p=await saveStoragePlan({id:copy?crypto.randomUUID():id||crypto.randomUUID(),name:copy?(name.slice(0,110)+" - copia"):name,expected_row_version:copy?0:version,counts:doc.counts,slots:doc.slots})
    setId(p.id);setName(p.name);setVersion(p.row_version);setDoc(p.document);setDirty(false)
    setPlans(old=>[{id:p.id,name:p.name,row_version:p.row_version,updated_at:p.updated_at,updated_by:p.updated_by},...old.filter(x=>x.id!==p.id)])
    setNotice("Escenario guardado en servidor · versión "+p.row_version)
  })
  const assign=(map:PlanMap,ids:number[],level:number,method:Method)=>{
    try{
      if(stale.length)throw Error("Primero resuelva las asignaciones desactualizadas.")
      if(!chosen||ids.length>chosen.remaining)throw Error("Seleccione producto y hasta la cantidad pendiente de posiciones.")
      update(assignSlots(doc,map,ids,level,method,chosen.variant_id,chosen.kind))
    }catch(e){setError(message(e))}
  }
  const remove=(map:PlanMap,ids:number[],level:number)=>{
    try{update(clearSlots(doc,map.locations.floor.floor_id,ids,level))}catch(e){setError(message(e))}
  }
  const auto=(map:PlanMap,area:string,method:Method)=>{
    if(stale.length){setError("Primero resuelva las asignaciones desactualizadas.");return}
    if(!window.confirm("Sugerir distribución por volumen en el sector elegido, sin reemplazar asignaciones. No verifica FEFO ni seguridad. ¿Continuar?"))return
    const next=suggest(doc,map,area,method,method==="floor"?1:2)
    update(next);setNotice((next.slots.length-doc.slots.length)+" pallets asignados en propuesta. Revise y ajuste.")
  }
  const print=()=>run(async()=>{
    if(dirty||!version)throw Error("Guarde el escenario antes de generar el PDF.")
    if(!maps.some(m=>m.locations.floor.facility_code==="BL"))throw Error("Falta el plano de Bodega de Lata.")
    const fresh=await loadMaps()
    if(doc.slots.some(s=>!isCurrent(s,fresh))){setMaps(fresh);throw Error("Cambió el plano. Revise el escenario.")}
    const {storagePlanPdf}=await import("@/lib/warehouse/storagePlannerPdf")
    storagePlanPdf(name,version,doc,fresh).save("Quillotana-mapa-objetivo-v"+version+".pdf")
  })
  if(loading)return <p className="p-8 text-muted-foreground">Cargando planos y escenarios…</p>
  return <div className="space-y-4">
    <header><h1 className="text-2xl font-semibold">Planificador de almacenamiento</h1><p className="text-sm text-muted-foreground">Conteo rápido y mapa objetivo de BC + BL. No altera pallets físicos, ubicaciones observadas ni Bsale.</p></header>
    {error&&<div role="alert" className="rounded-lg border border-destructive/40 bg-destructive/10 p-3 text-sm text-destructive">{error}</div>}
    {notice&&<div role="status" className="rounded-lg border bg-muted p-3 text-sm">{notice}</div>}
    {!maps.length&&<Button variant="outline" onClick={()=>window.location.reload()}>Recargar planos y escenarios</Button>}
    <fieldset disabled={busy||countBusy||!maps.length} className="space-y-4 disabled:opacity-70">
      <section className="space-y-2 rounded-lg border bg-card p-3">
        <div className="flex flex-wrap items-center gap-2">
          <select aria-label="Escenario guardado" className="h-9 rounded-md border border-input bg-background px-2 text-sm" value={version?id:""} onChange={e=>void open(e.target.value)}><option value="">Escenario nuevo</option>{plans.map(p=><option key={p.id} value={p.id}>{p.name} · v{p.row_version}</option>)}</select>
          <Input aria-label="Nombre del escenario" className="max-w-xs" value={name} maxLength={120} onChange={e=>{setName(e.target.value);setDirty(true)}}/>
          <Button variant="outline" onClick={create}>Nuevo</Button><Button onClick={()=>void save()}>{busy?"Procesando…":"Guardar escenario"}</Button><Button variant="outline" onClick={()=>void save(true)}>Guardar copia</Button><Button variant="outline" onClick={()=>void run(async()=>setMaps(await loadMaps()))}>Actualizar planos</Button><Button variant="outline" disabled={dirty||!version||!!stale.length} onClick={()=>void print()}>Descargar mapa PDF A3</Button>
          <span className="text-xs text-muted-foreground">{dirty?"Cambios sin guardar":version?"Guardado · v"+version:"Sin guardar"}</span>
        </div>
        <div className="grid grid-cols-3 gap-2">{[["Pallets contados",total],["Asignados",doc.slots.length],["Por asignar",total-doc.slots.length]].map(([l,n])=><div className="rounded-md border p-2" key={l}><div className="text-xs text-muted-foreground">{l}</div><div className="text-xl font-semibold">{n}</div></div>)}</div>
        {!!stale.length&&<div className="space-y-2 rounded-md border border-amber-500/50 p-2 text-sm"><p>{stale.length} asignaciones usan un plano anterior. No imprimir hasta revisar.</p><Button variant="outline" onClick={()=>{if(window.confirm("¿Retirar únicamente las asignaciones obsoletas?")){const invalid=new Set(stale.map(s=>s.floor_id+":"+s.position_id));update({...doc,slots:doc.slots.filter(s=>!invalid.has(s.floor_id+":"+s.position_id))})}}}>Retirar posiciones desactualizadas</Button><Button variant="outline" onClick={()=>void run(async()=>setMaps(await loadMaps()))}>Recargar planos</Button></div>}
      </section>
      {catalogFloor&&<StoragePlannerCount key={id} doc={doc} floorId={catalogFloor} onChange={update} onBusy={setCountBusy} origins={origins}/>}
      <section className="grid gap-3 xl:grid-cols-[minmax(240px,320px)_minmax(0,1fr)]">
        <div className="space-y-2 rounded-lg border bg-card p-3"><h2 className="font-semibold">2. Consolidado por volumen</h2><p className="text-xs text-muted-foreground">Seleccione variante y condición a asignar.</p><div className="max-h-[560px] space-y-1 overflow-auto">{consolidated.map(g=><button type="button" key={groupKey(g)} onClick={()=>setSelectedGroup(groupKey(g))} className={"w-full rounded-md border p-2 text-left text-sm "+(selectedGroup===groupKey(g)?"border-primary bg-muted":"hover:bg-muted/50")}><div className="flex items-start gap-2"><span className="mt-1 h-3 w-3 shrink-0 rounded" style={{backgroundColor:productColor(g.variant_id)}}/><span className="font-medium">{productLabel(g.product)}</span></div><div className="text-xs text-muted-foreground">{g.product?.product_type??"Tipo pendiente"} · {kindLabel(g.kind)}</div><div className="mt-1 flex justify-between text-xs"><span>{g.total} contados</span><strong>{g.remaining} pendientes</strong></div></button>)}</div>{!consolidated.length&&<p className="text-sm text-muted-foreground">Ingrese productos para comenzar.</p>}</div>
        <StoragePlannerMap key={id} maps={maps} doc={doc} chosen={chosen} onAssign={assign} onRemove={remove} onSuggest={auto}/>
      </section>
    </fieldset>
    <p className="text-xs text-muted-foreground">El plan impreso indica destinos previstos, no secuencia de traslados desde ubicaciones reales. Antes de ejecutar, verificar accesibilidad, estabilidad, carga, alturas y fechas individuales; aplicar FEFO, luego FIFO.</p>
  </div>
}
