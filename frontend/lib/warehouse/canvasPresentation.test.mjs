// node --experimental-strip-types --test frontend/lib/warehouse/canvasPresentation.test.mjs
import assert from "node:assert/strict"
import test from "node:test"
import { boxesOverlap, clippedHatchLines, compactPositionCode, placeCanvasLabels, screenBounds, stairDecoration } from "./canvasPresentation.ts"
import { metricToScreen, screenToMetric, fitScale, fitOffsets } from "./coords.ts"
import { validateElements } from "./validation.ts"

const L = 23.1, W = 13.7
const vp = (viewOrientationDeg = 0, scale = 28) => ({
  floorLengthM: L, floorWidthM: W, viewOrientationDeg, scale, offsetX: 48, offsetY: 48,
})
const rect = (x_m, y_m, width_m, depth_m, rotation_deg = 0) =>
  ({ x_m, y_m, width_m, depth_m, rotation_deg })
const close = (a, b) => assert.ok(Math.abs(a - b) < 1e-7, `${a} != ${b}`)

test("hatch never extends outside its own geometry", () => {
  for (const [w, h] of [[12, 63], [40, 66], [66, 40], [9, 2]]) {
    const lines = clippedHatchLines(w, h)
    assert.ok(lines.length)
    for (const [x1, y1, x2, y2] of lines) {
      for (const x of [x1, x2]) assert.ok(x >= -1e-9 && x <= w + 1e-9)
      for (const y of [y1, y2]) assert.ok(y >= -1e-9 && y <= h + 1e-9)
      close(x2 - x1, y2 - y1)
    }
  }
})

test("stairs: treads perpendicular to +Y, arrow towards DER in longitudinal view", () => {
  const s = metricToScreen(rect(21.58, 4.05, 1.52, 3.20), vp())
  const d = stairDecoration(s.width, s.height)
  assert.ok(d.steps.length >= 4)
  for (const [x1,y1,x2,y2] of d.steps) {
    close(y1,y2)
    assert.ok(x2 > x1)
  }
  assert.ok(d.arrow)
  close(d.arrow[0], d.arrow[2])
  assert.ok(d.arrow[3] > d.arrow[1])
  const angle = s.rotation * Math.PI / 180
  const dy = d.arrow[3] - d.arrow[1]
  assert.ok(-Math.sin(angle) * dy > 0)
  close(Math.cos(angle) * dy, 0)
})

test("rotate and drag round trip preserves the real unrotated origin", () => {
  for (const view of [0,90,180,270]) {
    for (const angle of [-45,0,30,90,179]) {
      const a = rect(11.2, 7.35, 1.26, 1.06, angle)
      const b = screenToMetric(metricToScreen(a, vp(view)), vp(view))
      for (const k of ["x_m","y_m","width_m","depth_m"]) close(a[k], b[k])
      close(Math.sin(a.rotation_deg*Math.PI/180), Math.sin(b.rotation_deg*Math.PI/180))
      close(Math.cos(a.rotation_deg*Math.PI/180), Math.cos(b.rotation_deg*Math.PI/180))
    }
  }
})

const seedLabels = [
  {key:"P7",code:"P7",type:"aisle",g:rect(19.6,7.35,2.8,6),dimensions:"2,80 × 6,00 m"},
  {key:"ESC",code:"ESC",type:"stair",g:rect(21.58,4.05,1.52,3.20)},
  {key:"BAE-01",code:"BAE-01",type:"position",underStair:true,g:rect(21.68,4.20,1.26,1.06)},
  {key:"BAE-02",code:"BAE-02",type:"position",underStair:true,g:rect(21.68,5.40,1.26,1.06)},
  {key:"IZ-SU",code:"IZ-SU",type:"loose_storage",g:rect(21.84,0.10,1.26,2.12)},
  {key:"DER",code:"DER",type:"sector",sectorWithAisles:true,g:rect(0,7.35,23.1,6)},
  {key:"CIRC",code:"CIRC",type:"circulation",g:rect(0,2.65,23.1,4.7)},
  {key:"IZ",code:"IZ",type:"sector",g:rect(0,0,23.1,2.65)},
]
test("readable non-overlapping labels; P7 centered; special labels remain visible", () => {
  for (const view of [0,90,180,270]) {
    for (const scale of [20,28,38]) {
      const inputs=seedLabels.map(({g,...label})=>({...label,rect:metricToScreen(g,vp(view,scale))}))
      const labels=placeCanvasLabels(inputs,1)
      for (const code of ["P7","ESC","BAE-01","BAE-02","IZ-SU"]) {
        assert.ok(labels.some(l=>l.key===code), `missing ${code} view=${view} scale=${scale}`)
      }
      for(let i=0;i<labels.length;i++) for(let j=i+1;j<labels.length;j++) {
        assert.ok(!boxesOverlap(labels[i],labels[j]), `${labels[i].key} vs ${labels[j].key}`)
      }
      const b=screenBounds(inputs.find(x=>x.key==="P7").rect)
      const p=labels.find(x=>x.key==="P7")
      close(b.x+b.width/2,p.x+p.width/2)
      close(b.y+b.height/2,p.y+p.height/2)
    }
  }
})

test("fit keeps the entire building inside the viewport in all views", () => {
  for(const view of [0,90,180,270]) {
    const s=fitScale(L,W,900,650,48,view)
    const off=fitOffsets(L,W,900,650,s,view)
    const b=screenBounds(metricToScreen(rect(0,0,L,W),{...vp(view,s),...off}))
    assert.ok(b.x>=47.9999 && b.y>=47.9999)
    assert.ok(b.x+b.width<=852.0001 && b.y+b.height<=602.0001)
  }
})

function aisle(id, code, x_m) {
  return {clientKey:code,physical:{id,code,name:code,element_type:"aisle",parent_id:null},
    geometry:rect(x_m,7.35,2.8,6)}
}
test("adjacent P3/P4 do not show false overlap; true overlap remains", () => {
  const floor={length_m:L,width_m:W}
  assert.equal(validateElements([aisle(3,"P3",8.4),aisle(4,"P4",11.2)],floor).filter(i=>i.code==="overlap").length,0)
  assert.equal(validateElements([aisle(3,"P3",8.4),aisle(4,"P4",11.19)],floor).filter(i=>i.code==="overlap").length,1)
})


// Layout representativo de v5: la geometría de producción no se escribe
// ni se presume idéntica a este fixture sintético de presentación.
const populatedLabels = [
  ...seedLabels.map(item => item.key === "ESC" ? {...item,parentKey:"CIRC"}
    : item.underStair ? {...item,parentKey:"ESC"} : item),
  ...Array.from({length:19},(_,i)=>["A","D"].map((side,j)=>({
    key:`IZ-${i+1}-${side}`,code:`IZ-${String(i+1).padStart(2,"0")}-${side}`,
    type:"position",parentKey:"IZ",g:rect(i*1.11,j*1.31,1.06,1.26),
  }))).flat(),
  ...Array.from({length:10},(_,i)=>({
    key:`P7-${i+1}`,code:`P7-${String(i+1).padStart(2,"0")}`,
    type:"position",parentKey:"P7",g:rect(19.6+Math.floor(i/5)*1.74,7.05+(i%5)*1.26,1.06,1.26),
  })),
]

function projected(items, view, scale) {
  return items.map(({g,...item})=>{
    const r=metricToScreen(g,vp(view,scale))
    const rad=r.rotation*Math.PI/180
    const cx=r.x+r.width/2,cy=r.y+r.height/2
    const entryPoint=item.type==="stair"?{x:cx-Math.cos(rad)*r.width/2,y:cy-Math.sin(rad)*r.width/2}
      : item.type==="aisle"?{x:cx+Math.sin(rad)*r.height/2,y:cy-Math.cos(rad)*r.height/2}:undefined
    return {...item,rect:r,entryPoint}
  })
}

test("compact codes preserve paired suffix and do not change physical identity",()=>{
  assert.equal(compactPositionCode("IZ-01-D"),"01D")
  assert.equal(compactPositionCode("IZ-01-A"),"01A")
  assert.equal(compactPositionCode("P7-10"),"10")
  assert.equal(compactPositionCode("BAE-02"),"02")
  assert.equal(compactPositionCode("custom"),"custom")
})

test("dense plan at fit: BAE codes inside their own footprints, aisle not over positions",()=>{
  for(const view of [0,90,180,270]) {
    for(const scale of [20,28,38]) {
      const inputs=projected(populatedLabels,view,scale)
      const original=JSON.stringify(inputs)
      const labels=placeCanvasLabels(inputs,1)
      const stair=labels.find(l=>l.key==="ESC")
      assert.ok(stair,`stair missing view=${view} scale=${scale}`)
      for(const code of ["BAE-01","BAE-02"]) {
        const b=screenBounds(inputs.find(x=>x.key===code).rect)
        const l=labels.find(x=>x.key===code)
        assert.ok(l,`missing ${code} view=${view} scale=${scale}`)
        assert.equal(l.callout,false)
        assert.ok(l.x>=b.x-1e-7 && l.y>=b.y-1e-7 &&
          l.x+l.width<=b.x+b.width+1e-7 && l.y+l.height<=b.y+b.height+1e-7)
      }
      const aisle=labels.find(x=>x.key==="P7")
      assert.ok(aisle,`missing P7 view=${view} scale=${scale}`)
      for(const p of inputs.filter(x=>x.parentKey==="P7")) {
        assert.ok(!boxesOverlap(aisle,screenBounds(p.rect)),`P7 label covers ${p.code}`)
      }
      for(let i=0;i<labels.length;i++) for(let j=i+1;j<labels.length;j++) {
        assert.ok(!boxesOverlap(labels[i],labels[j]),`${labels[i].key} vs ${labels[j].key}`)
      }
      assert.equal(JSON.stringify(inputs),original)
    }
  }
})

test("at closer zoom, compact positions show their full codes",()=>{
  const inputs=projected(populatedLabels,0,28)
  const labels=placeCanvasLabels(inputs,4)
  for(const key of ["BAE-01","BAE-02","IZ-1-D","IZ-1-A","P7-1","P7-10"]) {
    const item=inputs.find(p=>p.key===key)
    const label=labels.find(p=>p.key===key)
    assert.ok(label,`missing ${key}`)
    assert.equal(label.text,item.code)
    assert.equal(label.compact,false)
    assert.equal(label.callout,false)
  }
})


test("BL codes stay short and distinct in compact view",()=>{
  const codes=Array.from({length:12},(_,i)=>Array.from("ABCDE",d=>"BL"+String(i+1).padStart(2,"0")+d)).flat()
  assert.equal(new Set(codes.map(compactPositionCode)).size,60)
  for(const code of codes)assert.equal(compactPositionCode(code),code)
})
