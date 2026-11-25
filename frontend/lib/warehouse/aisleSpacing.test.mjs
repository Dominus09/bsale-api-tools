import assert from "node:assert/strict"
import test from "node:test"
import { inspectAisleSpacing, planAisleSpacing, newSpacingBlockers } from "./aisleSpacing.ts"

function element(id, code, type, parent, x, y, w, d, extra = {}) {
  return { clientKey: "pe-" + id, physical: { id, code, name: code, element_type: type, parent_id: parent, meta_json: {} },
    geometry: { x_m:x, y_m:y, width_m:w, depth_m:d, rotation_deg:0, geom_type:"rect",
      validation_status:"pending_validation", constraints_json:{stacking_authorized:false}, meta_json:{}, ...extra } }
}
function fixture() {
  const all = [
    element(1,"DER","sector",null,0,7.35,23.1,6),
    element(2,"CIRC","circulation",null,0,2.65,23.1,4.7),
    element(3,"OFC","office",1,0,7.35,2.8,6),
    element(4,"ESC","stair",2,21.58,4.05,1.52,3.2),
    element(5,"BAE-01","position",4,21.68,4.2,1.26,1.06),
    element(6,"IZ-SU","loose_storage",null,21.84,.1,1.26,2.12),
  ]
  for(let i=1;i<=7;i++) {
    const x=i*2.8,id=10+i
    all.push(element(id,"P"+i,"aisle",1,x,7.35,2.8,6))
    for(let r=0;r<2;r++) for(let j=0;j<5;j++) {
      const n=r*5+j+1
      all.push(element(100+i*10+n,"P"+i+"-"+String(n).padStart(2,"0"),"position",id,x+r*1.74,7.05+j*1.26,1.06,1.26))
    }
  }
  return all
}
const targets=(f,m)=>Object.fromEntries(inspectAisleSpacing(f,1).gaps.map(g=>[g.key,m]))
const close=(a,b)=>assert.ok(Math.abs(a-b)<.00011,`${a} != ${b}`)
test("uniform B 20 cm: keep 70 identities, all nominal aisle frames, and other structures",()=>{
  const f=fixture(),old=JSON.stringify(f)
  const p=planAisleSpacing(f,1,targets(f,.2),23.1,13.7)
  assert.deepEqual(p.errors,[])
  assert.equal(p.changedKeys.length,60)
  assert.equal(p.elements.length,f.length)
  assert.equal(JSON.stringify(f),old)
  p.gaps.forEach(g=>close(g.proposedM,.2))
  p.interior.forEach((a,i)=>close(a.proposedM,i===0||i===6?.58:.48))
  p.elements.forEach((e,i)=>{
    assert.deepEqual(e.physical,f[i].physical)
    assert.equal(e.geometry.y_m,f[i].geometry.y_m)
    assert.equal(e.geometry.width_m,f[i].geometry.width_m)
    assert.equal(e.geometry.depth_m,f[i].geometry.depth_m)
    if(!p.changedKeys.includes(e.clientKey)) assert.equal(e,f[i])
  })
  assert.ok(p.circulationWarnings.length)
})
test("one boundary changes only its two neighboring rows",()=>{
  const f=fixture(),g=inspectAisleSpacing(f,1).gaps[2]
  const p=planAisleSpacing(f,1,{[g.key]:.4},23.1,13.7)
  assert.deepEqual(p.errors,[])
  assert.equal(p.changedKeys.length,10)
  p.gaps.forEach((x,i)=>close(x.proposedM,i===2?.4:0))
})
test("large uniform gap blocks internal overlap",()=>{
  const f=fixture()
  const p=planAisleSpacing(f,1,targets(f,.8),23.1,13.7)
  assert.ok(p.errors.some(e=>e.includes("dos filas se superponen")))
})
test("zero-change preview preserves every object",()=>{
  const f=fixture()
  const p=planAisleSpacing(f,1,{},23.1,13.7)
  assert.equal(p.changedKeys.length,0)
  p.elements.forEach((e,i)=>assert.equal(e,f[i]))
})
test("can reduce a previously created separation to zero",()=>{
  const f=fixture(),p=planAisleSpacing(f,1,targets(f,.3),23.1,13.7)
  assert.deepEqual(p.errors,[])
  const q=planAisleSpacing(p.elements,1,targets(p.elements,0),23.1,13.7)
  assert.deepEqual(q.errors,[])
  q.gaps.forEach(g=>close(g.proposedM,0))
})
test("reject a moved validated position and a new obstacle collision",()=>{
  const f=fixture()
  f.find(e=>e.physical.code==="P1-06").geometry.validation_status="validated"
  f.push(element(999,"PILAR","pillar",null,4.35,7.2,.12,.12))
  const p=planAisleSpacing(f,1,targets(f,.4),23.1,13.7)
  assert.ok(p.errors.some(e=>e.includes("validada/autorizada")))
  assert.ok(p.errors.some(e=>e.includes("PILAR")))
})
test("reject non-uniform rows instead of guessing geometry",()=>{
  const f=fixture()
  f.find(e=>e.physical.code==="P1-01").geometry.x_m+=.05
  assert.throws(()=>inspectAisleSpacing(f,1),/dos filas alineadas/)
})
test("existing server issues are retained; new hard conflicts and circulation warnings block",()=>{
  const old={severity:"geom_conflict",code:"structure_collision",element_ref:"5",other_code:"ESC"}
  const fresh={severity:"warning",code:"circulation_invasion",element_ref:"100",other_code:"CIRC"}
  const pending={severity:"pending_validation",code:"pending_validation",element_ref:"100"}
  assert.deepEqual(newSpacingBlockers([old],[old,fresh,pending]),[fresh])
})
