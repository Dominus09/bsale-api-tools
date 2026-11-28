"use client"
import {useEffect,useRef,useState} from "react"
import {Button} from "@/components/ui/button"
import {Input} from "@/components/ui/input"
import {searchPalletProducts,type PalletProduct} from "@/lib/warehouse/api"
import {type PlanDocument,changeCount,groupKey,kindLabel,productLabel} from "@/lib/warehouse/storagePlanner"

type Props={doc:PlanDocument;floorId:number;onChange:(doc:PlanDocument)=>void}
const msg=(e:unknown)=>e instanceof Error?e.message:"Operación fallida."
export function StoragePlannerCount({doc,floorId,onChange}:Props){
  const [source,setSource]=useState("BC / IZ")
  const [query,setQuery]=useState("")
  const [results,setResults]=useState<PalletProduct[]>([])
  const [picked,setPicked]=useState<PalletProduct|null>(null)
  const [complete,setComplete]=useState("0")
  const [partial,setPartial]=useState("0")
  const [bulk,setBulk]=useState("")
  const [showBulk,setShowBulk]=useState(false)
  const [busy,setBusy]=useState(false)
  const [error,setError]=useState("")
  const [notice,setNotice]=useState("")
  const [camera,setCamera]=useState(false)
  const video=useRef<HTMLVideoElement>(null)
  const scanner=useRef<{stop:()=>void}|null>(null)
  const epoch=useRef(0)
  const input=useRef<HTMLInputElement>(null)
  const choose=(p:PalletProduct,origin=source)=>{
    setPicked(p);setResults([]);setQuery(p.barcode??p.sku??productLabel(p))
    const found=doc.counts.filter(r=>r.variant_id===p.variant_id&&r.source.toLowerCase()===origin.trim().toLowerCase())
    setComplete(String(found.find(r=>r.kind==="complete")?.count??0));setPartial(String(found.find(r=>r.kind==="partial")?.count??0))
  }
  const run=async(fn:()=>Promise<void>)=>{setBusy(true);setError("");setNotice("");try{await fn()}catch(e){setError(msg(e))}finally{setBusy(false)}}
  const search=()=>run(async()=>{
    if(query.trim().length<2)throw Error("Ingrese código o nombre.")
    const found=await searchPalletProducts(floorId,query.trim())
    setResults(found);setPicked(null)
    const exact=found.filter(p=>p.barcode===query.trim()||p.sku===query.trim())
    if(exact.length===1)choose(exact[0]);else if(found.length===1)choose(found[0])
    else if(!found.length)setNotice("No encontrado en catálogo.")
  })
  useEffect(()=>{
    if(!camera||!video.current)return
    const token=++epoch.current
    let consumed=false
    ;(async()=>{
      const {BrowserMultiFormatReader}=await import("@zxing/browser")
      if(epoch.current!==token||!video.current)return
      const controls=await new BrowserMultiFormatReader().decodeFromVideoDevice(undefined,video.current,(result,_err,control)=>{
        if(!result||consumed||epoch.current!==token)return
        consumed=true;control.stop();epoch.current++;scanner.current=null;setCamera(false);setQuery(result.getText());setPicked(null);setResults([])
      })
      if(epoch.current!==token)controls.stop();else scanner.current=controls
    })().catch(e=>{if(epoch.current===token){setError("Cámara no disponible: "+msg(e));setCamera(false)}})
    return()=>{epoch.current++;scanner.current?.stop();scanner.current=null}
  },[camera])
  const add=()=>{
    try{
      if(!picked)throw Error("Seleccione una variante.")
      if(!complete.trim()||!partial.trim())throw Error("Indique cantidades (0 si corresponde).")
      if(doc.counts.some(r=>r.variant_id===picked.variant_id&&r.source.toLowerCase()===source.trim().toLowerCase())&&!window.confirm("¿Reemplazar conteo del mismo producto/origen?"))return
      onChange(changeCount(doc,picked,source,Number(complete),Number(partial)))
      setPicked(null);setQuery("");setComplete("0");setPartial("0");setNotice("Conteo agregado. Guarde el escenario.");setError("");input.current?.focus()
    }catch(e){setError(msg(e))}
  }
  const importBulk=()=>run(async()=>{
    const rows=bulk.trim().split(/\r?\n/).filter(Boolean)
    if(!rows.length||rows.length>100)throw Error("Pegue entre 1 y 100 líneas.")
    let next=doc
    const keys=new Set<string>()
    for(const [i,line] of rows.entries()){
      const fields=line.split(";").map(x=>x.trim())
      if(fields.length<3||fields.length>4||!fields[0]||!fields[1]||!fields[2])throw Error("Línea "+(i+1)+": código;completos;molidos;origen opcional.")
      const [code,c,p,s]=fields
      const found=(await searchPalletProducts(floorId,code)).filter(x=>x.barcode===code||x.sku===code)
      if(found.length!==1)throw Error("Línea "+(i+1)+": código no encontrado o ambiguo: "+code)
      const origin=s||source,key=found[0].variant_id+":"+origin.toLowerCase()
      if(keys.has(key))throw Error("Línea "+(i+1)+": producto/origen duplicado.")
      keys.add(key);next=changeCount(next,found[0],origin,Number(c),Number(p))
    }
    if(!window.confirm("Aplicar "+rows.length+" conteos. Reemplaza los existentes del mismo producto/origen, no los suma."))return
    onChange(next);setBulk("");setNotice("Conteos agregados. Guarde el escenario.")
  })
  return <fieldset disabled={busy} className="space-y-3 rounded-lg border bg-card p-3">
    <h2 className="font-semibold">1. Conteo rápido por sector</h2>
    {error&&<p role="alert" className="text-sm text-destructive">{error}</p>}
    {notice&&<p role="status" className="text-sm text-muted-foreground">{notice}</p>}
    <label className="block text-sm">Origen (bodega / pasillo)<Input value={source} maxLength={100} onChange={e=>{setSource(e.target.value);setPicked(null);setResults([])}} placeholder="BC / IZ o BL / P2"/></label>
    <div className="flex flex-wrap gap-2">
      <Input ref={input} className="min-w-48 flex-1" aria-label="Código o producto" placeholder="Escanee o busque en catálogo Bsale" value={query} onChange={e=>{setQuery(e.target.value);setPicked(null);setResults([])}} onKeyDown={e=>{if(e.key==="Enter"){e.preventDefault();void search()}}}/>
      <Button onClick={()=>void search()}>Buscar</Button><Button variant="outline" onClick={()=>setCamera(v=>!v)}>{camera?"Cerrar cámara":"Cámara"}</Button><Button variant="outline" onClick={()=>setShowBulk(v=>!v)}>Pegar códigos</Button>
    </div>
    {camera&&<div><video ref={video} autoPlay muted playsInline className="max-h-56 w-full rounded bg-black"/><p className="text-xs text-muted-foreground">Una vez leído, presione Buscar.</p></div>}
    {!!results.length&&<div className="max-h-64 space-y-1 overflow-auto">{results.map(p=><button type="button" key={p.variant_id} className="block w-full rounded border p-2 text-left text-sm hover:bg-muted" onClick={()=>choose(p)}>{productLabel(p)} · {p.product_type??"Tipo pendiente"}<span className="block text-xs text-muted-foreground">{p.barcode??p.sku??"Sin código"}</span></button>)}</div>}
    {picked&&<div className="space-y-2 rounded border bg-muted/40 p-3"><p className="text-sm font-semibold">{productLabel(picked)} · {picked.product_type??"Tipo pendiente"}</p><div className="flex flex-wrap items-end gap-2"><label className="text-xs">Completos<Input type="number" min={0} max={1000} step={1} value={complete} onChange={e=>setComplete(e.target.value)}/></label><label className="text-xs">Molidos físicos<Input type="number" min={0} max={1000} step={1} value={partial} onChange={e=>setPartial(e.target.value)}/></label><Button onClick={add}>Registrar conteo</Button></div><p className="text-xs text-muted-foreground">Medio pallet ocupa una posición: ingresar 1 molido. Mismo origen y variante reemplaza el conteo.</p></div>}
    {showBulk&&<div className="space-y-2"><p className="text-xs text-muted-foreground">Código;completos;molidos;origen opcional. Máximo 100 líneas, códigos exactos, todo o nada.</p><textarea className="min-h-28 w-full rounded border bg-background p-2 font-mono text-sm" value={bulk} onChange={e=>setBulk(e.target.value)} placeholder="7790000000000;5;1;BL / P1"/><Button variant="outline" onClick={()=>void importBulk()}>Validar y aplicar</Button></div>}
    {!!doc.counts.length&&<details><summary className="cursor-pointer text-sm font-medium">Detalle por origen ({doc.counts.length} registros)</summary><div className="max-h-64 overflow-auto"><table className="w-full text-xs"><thead><tr className="border-b text-left"><th>Producto / variante</th><th>Origen</th><th>Condición</th><th>Cantidad</th><th>Editar</th></tr></thead><tbody>{doc.counts.map(r=><tr className="border-b" key={groupKey(r)+":"+r.source}><td className="p-1">{productLabel(doc.products.find(p=>p.variant_id===r.variant_id))}</td><td>{r.source}</td><td>{kindLabel(r.kind)}</td><td>{r.count}</td><td><Button size="sm" variant="ghost" onClick={()=>{const p=doc.products.find(x=>x.variant_id===r.variant_id);if(p){setSource(r.source);choose(p,r.source)}}}>Editar</Button></td></tr>)}</tbody></table></div></details>}
  </fieldset>
}
