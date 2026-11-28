import test from "node:test"
import assert from "node:assert/strict"
import {mkdtempSync,readFileSync,writeFileSync,rmSync} from "node:fs"
import {dirname,join} from "node:path"
import {fileURLToPath,pathToFileURL} from "node:url"
import ts from "typescript"

const root=dirname(fileURLToPath(import.meta.url))
const tmp=mkdtempSync(join(root,".planner-test-"))
let p,pdf
try{
  for(const file of ["coords","storagePlanner","storagePlannerPdf"]){
    const code=ts.transpileModule(readFileSync(join(root,file+".ts"),"utf8"),{compilerOptions:{module:ts.ModuleKind.ESNext,target:ts.ScriptTarget.ES2022}}).outputText.replaceAll('from "./coords"','from "./coords.mjs"').replaceAll('from "./storagePlanner"','from "./storagePlanner.mjs"')
    writeFileSync(join(tmp,file+".mjs"),code)
  }
  p=await import(pathToFileURL(join(tmp,"storagePlanner.mjs")).href)
  pdf=await import(pathToFileURL(join(tmp,"storagePlannerPdf.mjs")).href)
}finally{rmSync(tmp,{recursive:true,force:true})}
const prod=(id)=>({variant_id:id,product_id:id,product_name:"Corona",variant_name:id===1?"330 cc":"620 cc",product_type:"Cervezas",barcode:"000"+id,sku:null,units_per_box:24})
const geom=(x,y)=>({x_m:x,y_m:y,width_m:1.06,depth_m:1.26,rotation_deg:0})
const positions=[1,2,3,4].map(id=>({id,code:id===4?"BAE-01":"IZ-0"+id+"-D",physical_meta_snap:{},constraints_json:{},meta_json:{},stack_levels_max:null}))
const map={locations:{floor:{floor_id:1,facility_code:"BC"},revision:{id:1,row_version:6},items:positions},bundle:{facility:{code:"BC",name:"Bodega Central"},floor:{length_m:23.1,width_m:13.7},revision:{id:1,row_version:6},elements:positions.map((x,i)=>({physical:{id:x.id,code:x.code,parent_id:10,element_type:"position"},geometry:geom(i*1.1,0)}))}}
const doc=()=>p.changeCount(p.changeCount(p.emptyPlan(),prod(1),"BC / IZ",2,0),prod(2),"BL / P1",0,1)
test("count replaces same source; different source consolidates",()=>{
  const x=p.changeCount(p.changeCount(doc(),prod(1),"bc / iz",3,0),prod(1),"BL / P2",4,0)
  assert.equal(p.groups(x).find(g=>g.variant_id===1).total,7)
})
test("direct two levels same variant; cannot remove supporting base",()=>{
  let x=p.assignSlots(doc(),map,[1],1,"direct",1,"complete")
  x=p.assignSlots(x,map,[1],2,"direct",1,"complete")
  assert.equal(x.slots.length,2)
  assert.throws(()=>p.clearSlots(x,1,[1],1))
  assert.throws(()=>p.changeCount(x,prod(1),"BC / IZ",1,0))
})
test("rack accepts partial lower and different complete upper",()=>{
  let x=p.assignSlots(doc(),map,[1],1,"rack",2,"partial")
  x=p.assignSlots(x,map,[1],2,"rack",1,"complete")
  assert.equal(x.slots.length,2)
})
test("reject restricted, upper without base, and overassignment",()=>{
  assert.throws(()=>p.assignSlots(doc(),map,[4],1,"floor",1,"complete"))
  assert.throws(()=>p.assignSlots(doc(),map,[1],2,"rack",1,"complete"))
  assert.throws(()=>p.assignSlots(doc(),map,[1,2,3],1,"floor",1,"complete"))
})
test("suggest starts at highest IZ, preserves restricted and counts",()=>{
  const x=p.suggest(doc(),map,"10","direct",2)
  assert.equal(x.slots.length,2);assert.ok(x.slots.every(s=>s.position_id===3))
  assert.equal(p.groups(x).find(g=>g.variant_id===2).remaining,1)
})
test("layout version changed invalidates slot",()=>{
  const x=p.assignSlots(doc(),map,[1],1,"floor",1,"complete")
  assert.ok(p.isCurrent(x.slots[0],[map]))
  assert.ok(!p.isCurrent(x.slots[0],[{...map,locations:{...map.locations,revision:{id:1,row_version:7}}}]))
})
test("real PDF generation, one map + detailed list + consolidated",()=>{
  const x=p.assignSlots(doc(),map,[1],1,"floor",1,"complete")
  const out=pdf.storagePlanPdf("Escenario",1,x,[map])
  assert.equal(out.getNumberOfPages(),3)
  assert.ok(out.output("arraybuffer").byteLength>1000)
})
