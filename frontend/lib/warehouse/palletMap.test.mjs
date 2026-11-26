import test from "node:test"
import assert from "node:assert/strict"
import { slotsAtLevel, observedSlotState, batchSelectable, restrictedBatchLocation, toggleBatchGroup } from "./palletMap.ts"
import { metricToScreen } from "./coords.ts"
import { layoutColorsForTheme, colorsForType, LAYOUT_COLORS } from "./style.ts"
import { isDarkThemeRoute } from "../appearance.ts"

test("observed occupation is specific to position and level; unknown never means free",()=>{
  const slots=[{position_id:10,observed_level:1,pallet_id:23},{position_id:10,observed_level:2,pallet_id:24}]
  assert.equal(slotsAtLevel(slots,1).get(10).pallet_id,23)
  assert.equal(slotsAtLevel(slots,2).get(10).pallet_id,24)
  assert.equal(observedSlotState(slotsAtLevel(slots,3).get(10)),"unrecorded")
  assert.equal(slotsAtLevel(slots,0).size,0)
})
test("own pallet only when its actual identity matches; old API without id blocks",()=>{
  const slot={position_id:10,observed_level:1,pallet_id:23}
  assert.equal(observedSlotState(slot,23),"own")
  assert.equal(observedSlotState(slot,24),"occupied")
  assert.equal(observedSlotState({position_id:10,observed_level:1}),"occupied")
})
test("horizontal view matches editor: rear left, DER above IZ",()=>{
  const vp={scale:40,offsetX:24,offsetY:24,floorLengthM:23.1,floorWidthM:13.7,viewOrientationDeg:90}
  const rect=(x_m,y_m)=>metricToScreen({x_m,y_m,width_m:1.06,depth_m:1.26,rotation_deg:0},vp)
  assert.ok(rect(20,8).x<rect(3,8).x)
  assert.ok(rect(3,8).y<rect(3,1).y)
})
test("Graphite scope includes editor and register, preserves original light palette",()=>{
  assert.ok(isDarkThemeRoute("/inteligencia-bodega"))
  assert.ok(isDarkThemeRoute("/inteligencia-bodega/pallets"))
  assert.ok(!isDarkThemeRoute("/inteligencia-bodega-legacy"))
  assert.equal(layoutColorsForTheme(false),LAYOUT_COLORS)
  assert.notEqual(layoutColorsForTheme(true).background,LAYOUT_COLORS.background)
  assert.notEqual(colorsForType("position",{},true).stroke,colorsForType("position",{},false).stroke)
  assert.deepEqual(colorsForType("position",{under_stair:true},true).dash,[4,3])
})


test("bulk selection only includes unrecorded regular positions at the chosen level",()=>{
  const pos=(id,code,meta={})=>({id,code,physical_meta_snap:{},constraints_json:{},meta_json:meta})
  const slots=[{position_id:1,observed_level:1,pallet_id:23},{position_id:2,observed_level:2,pallet_id:24}]
  assert.equal(batchSelectable(pos(1,"P7-01"),slotsAtLevel(slots,1)),false)
  assert.equal(batchSelectable(pos(1,"P7-01"),slotsAtLevel(slots,2)),true)
  assert.equal(batchSelectable(pos(2,"P7-02"),slotsAtLevel(slots,1)),true)
  assert.equal(restrictedBatchLocation(pos(3,"BAE-01")),true)
  assert.equal(restrictedBatchLocation(pos(4,"SPECIAL",{storage_kind:"special_restricted"})),true)
  assert.equal(batchSelectable(pos(5,"S-01",{under_stair:true}),new Map()),false)
})
test("select all aisle positions, toggle them off without removing other aisle",()=>{
  const aisle=Array.from({length:10},(_,i)=>i+1)
  const selected=toggleBatchGroup([20],aisle)
  assert.deepEqual(selected,[20,...aisle])
  assert.deepEqual(toggleBatchGroup(selected,aisle),[20])
  assert.deepEqual(toggleBatchGroup([1,2],[2,3,3]),[1,2,3])
  assert.deepEqual(toggleBatchGroup([1],[2,3,4],3),[1,2,3])
})
