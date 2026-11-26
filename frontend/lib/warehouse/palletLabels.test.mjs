import test from "node:test"
import assert from "node:assert/strict"
import { mkdtempSync, readFileSync, writeFileSync, rmSync } from "node:fs"
import { dirname, join } from "node:path"
import { fileURLToPath, pathToFileURL } from "node:url"
import ts from "typescript"
import { palletIdFromCode, summarizePalletLabel, labelGroups, labelPageLayout, labelRect, qrRuns } from "./palletLabels.ts"

// QR estándar generado por qrcode, nivel M, margen 4, para PAL-000001 a PAL-000003.
const matrices=[["00000000000000000000000000000","00000000000000000000000000000","00000000000000000000000000000","00000000000000000000000000000","00001111111010101011111110000","00001000001001110010000010000","00001011101001110010111010000","00001011101010010010111010000","00001011101011001010111010000","00001000001010010010000010000","00001111111010101011111110000","00000000000011011000000000000","00001000101110110111110010000","00000010100101011000011000000","00001111111010110011101110000","00001000100001100110111010000","00001110101100101110010010000","00000000000011001110011110000","00001111111011001101110100000","00001000001000111001110100000","00001011101011010010001010000","00001011101001111000001110000","00001011101000010011110000000","00001000001001100111110010000","00001111111010001110001110000","00000000000000000000000000000","00000000000000000000000000000","00000000000000000000000000000","00000000000000000000000000000"],["00000000000000000000000000000","00000000000000000000000000000","00000000000000000000000000000","00000000000000000000000000000","00001111111010000011111110000","00001000001001100010000010000","00001011101000111010111010000","00001011101011011010111010000","00001011101011101010111010000","00001000001011010010000010000","00001111111010101011111110000","00000000000010011000000000000","00001000101111010111110010000","00000000110011011000011000000","00000111011000010011101110000","00000001010111100111011010000","00000010111111001110010010000","00000000000011001110011110000","00001111111011101101110100000","00001000001000111001110100000","00001011101011010010001010000","00001011101001011000001110000","00001011101001010011110000000","00001000001000100111110010000","00001111111010001110001110000","00000000000000000000000000000","00000000000000000000000000000","00000000000000000000000000000","00000000000000000000000000000"],["00000000000000000000000000000","00000000000000000000000000000","00000000000000000000000000000","00000000000000000000000000000","00001111111001011011111110000","00001000001011100010000010000","00001011101010101010111010000","00001011101010010010111010000","00001011101000001010111010000","00001000001001010010000010000","00001111111010101011111110000","00000000000010100000000000000","00001000001010110110011100000","00000111000010011100111100000","00001100101010001011010110000","00000110100100111111100010000","00001011101010011111100010000","00000000000010101000011000000","00001111111001110101001100000","00001000001001000010101110000","00001011101001110101001100000","00001011101001111110001000000","00001011101001011101111110000","00001000001000011110001010000","00001111111010101001001000000","00000000000000000000000000000","00000000000000000000000000000","00000000000000000000000000000","00000000000000000000000000000"]]
const line={variant_id:100,product_name_snap:"SIDRA REAL",variant_name_snap:"BLANCA 750 CC (SEC 6)",
  product_type_snap:"ESPUMANTES",units_per_box_snap:6,quantity_status:"verified",boxes:80,
  loose_units:0,total_units:480,expiry_status:"dated",expires_on:"2029-02-20",lot:"L001"}
const label=(id=1,lines=[line],kind="complete")=>({
  id,code:"PAL-"+String(id).padStart(6,"0"),qr_value:"PAL-"+String(id).padStart(6,"0"),
  qr_matrix:matrices[id-1],pallet_kind:kind,row_version:1,received_on:null,lines
})

test("parse canonical PAL, no URL or arbitrary numeric ID",()=>{
  for(const [code,id] of [["PAL-000001",1],[" pal-000010 ",10],["PAL-1000001",1000001]])assert.equal(palletIdFromCode(code),id)
  for(const invalid of ["1","PAL-1","PAL-000000","PAL-0000001","https://example.com/PAL-000001","PAL-9007199254740992"])
    assert.equal(palletIdFromCode(invalid),null)
})
test("earliest verified expiry with pending dates and quantities stays explicit",()=>{
  const s=summarizePalletLabel(label(1,[line,{...line,expires_on:"2028-01-01",lot:"L002"},
    {...line,quantity_status:"unknown",boxes:null,loose_units:null,total_units:null,expiry_status:"unknown",expires_on:null,lot:null}]))
  assert.equal(s.expiry,"01/01/2028")
  assert.equal(s.expiryNote,"OTRAS FECHAS PENDIENTES")
  assert.equal(s.qtyText,"Pendiente de conteo")
  assert.equal(s.qtyNote,"Conteo parcial")
  assert.equal(s.lotText,"2 lote(s) + pendientes")
})
test("no expiry is only shown if explicitly not applicable; no mixed totals",()=>{
  const pending={...line,expiry_status:"unknown",expires_on:null}
  const na={...line,expiry_status:"not_applicable",expires_on:null}
  assert.equal(summarizePalletLabel(label(1,[pending])).expiry,"SIN VERIFICAR")
  assert.equal(summarizePalletLabel(label(1,[na])).expiry,"NO APLICA")
  const mixed=summarizePalletLabel(label(1,[na,{...line,variant_id:200}],"mixed"))
  assert.equal(mixed.product,"PALLET MIXTO")
  assert.equal(mixed.qtyText,"Por variante · ver QR")
  assert.equal(summarizePalletLabel(label()).qtyText,(480).toLocaleString("es-CL")+" un.")
})
test("aisle labels use saved parent identity and all observed levels, not visible page",()=>{
  const items=[{id:1,code:"P7-01",parent_id:7,parent_code:"P7"},{id:2,code:"P7-02",parent_id:7,parent_code:"P7"},
    {id:3,code:"P7-SIMILAR",parent_id:6,parent_code:"P6"},{id:4,code:"BAE-01"}]
  const observed_slots=[{position_id:2,observed_level:1,pallet_id:20},{position_id:1,observed_level:2,pallet_id:12},
    {position_id:1,observed_level:1,pallet_id:11},{position_id:3,observed_level:1,pallet_id:31},{position_id:4,observed_level:1,pallet_id:40}]
  assert.deepEqual(labelGroups({items,observed_slots}),[{id:6,code:"P6",ids:[31]},{id:7,code:"P7",ids:[11,12,20]}])
})
test("A4 layout covers 10 independent labels with no overflow, 1/2/4 per sheet",()=>{
  for(const n of [1,2,4]){
    const layout=labelPageLayout(n)
    const rects=Array.from({length:10},(_,i)=>labelRect(i,n))
    assert.equal(rects.at(-1).page+1,Math.ceil(10/n))
    for(const r of rects){
      assert.ok(r.x>=layout.margin&&r.y>=layout.margin)
      assert.ok(r.x+r.width<=layout.pageWidth-layout.margin+.001)
      assert.ok(r.y+r.height<=layout.pageHeight-layout.margin+.001)
    }
    for(const [i,a] of rects.entries())for(const b of rects.slice(i+1)){
      if(a.page===b.page)assert.ok(a.x+a.width<=b.x||b.x+b.width<=a.x||a.y+a.height<=b.y||b.y+b.height<=a.y)
    }
  }
})
test("QR vector runs reproduce the matrix and reject invalid identity or quiet zone",()=>{
  const a=label(),q=qrRuns(a)
  const rows=Array.from({length:q.size},()=>Array(q.size).fill("0"))
  for(const r of q.runs)for(let x=r.x;x<r.x+r.length;x++)rows[r.y][x]="1"
  assert.deepEqual(rows.map(r=>r.join("")),a.qr_matrix)
  assert.throws(()=>qrRuns({...a,qr_value:"PAL-000002"}))
  const invalid=[...a.qr_matrix];invalid[0]="1"+invalid[0].slice(1)
  assert.throws(()=>qrRuns({...a,qr_matrix:invalid}))
})
test("real jsPDF generation A4, page count, no duplicate identities",async()=>{
  // Node strip-types does not resolve extensionless TS imports used by Next.
  // Transpile the two exact source modules into an isolated temp directory
  // beside node_modules resolution; do not change application sources.
  const here=dirname(fileURLToPath(import.meta.url))
  const tmp=mkdtempSync(join(here,".pallet-labels-test-"))
  try{
    for(const name of ["palletLabels","palletLabelsPdf"]){
      const source=readFileSync(join(here,name+".ts"),"utf8")
      const js=ts.transpileModule(source,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.ESNext}}).outputText
        .replace('from "./palletLabels"','from "./palletLabels.mjs"')
      writeFileSync(join(tmp,name+".mjs"),js)
    }
    const {createPalletLabelsPdf}=await import(pathToFileURL(join(tmp,"palletLabelsPdf.mjs")).href)
    const batch={generated_at:"2026-11-26T19:00:00Z",items:[label(1),label(2,[{...line,expiry_status:"unknown",expires_on:null}]),label(3,[line,{...line,variant_id:200}],"mixed")]}
    for(const n of [1,2,4]){
      const doc=await createPalletLabelsPdf(batch,n)
      assert.equal(doc.getNumberOfPages(),Math.ceil(batch.items.length/n))
      assert.ok(Math.abs(doc.internal.pageSize.getWidth()-210)<.1)
      assert.ok(Math.abs(doc.internal.pageSize.getHeight()-297)<.1)
      assert.ok(Buffer.from(doc.output("arraybuffer")).subarray(0,5).toString()==="%PDF-")
    }
    await assert.rejects(()=>createPalletLabelsPdf({...batch,items:[label(),label()]},2))
  }finally{rmSync(tmp,{recursive:true,force:true})}
})
