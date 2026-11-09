import assert from "node:assert/strict"
import test from "node:test"
import { buildGoogleMapsNavigation, likelyIslandDestination } from "./navigation.ts"

test("Lily Melinka: destino exacto en query/destination, no origen", () => {
  const nav = buildGoogleMapsNavigation({
    latitude: -43.894257,
    longitude: -73.743421,
    city: "Melinka",
    clientName: "Minimarket lily",
  })
  assert.equal(nav.destination_latitude, -43.894257)
  assert.equal(nav.destination_longitude, -73.743421)
  assert.match(nav.google_maps_place_url, /query=-43\.894257%2C-73\.743421/)
  assert.match(nav.google_maps_directions_url, /destination=-43\.894257%2C-73\.743421/)
  assert.equal(nav.google_maps_directions_url.includes("origin="), false)
  assert.equal(nav.likely_no_road_route, true)
  assert.equal(nav.google_maps_url, nav.google_maps_place_url)
})

test("Ancud continental usa directions por defecto", () => {
  const nav = buildGoogleMapsNavigation({
    latitude: -41.87,
    longitude: -73.82,
    city: "Ancud",
  })
  assert.equal(nav.likely_no_road_route, false)
  assert.equal(nav.google_maps_url, nav.google_maps_directions_url)
})

test("likelyIslandDestination por ciudad y latitud", () => {
  assert.equal(likelyIslandDestination("Melinka", -43.89), true)
  assert.equal(likelyIslandDestination("Ancud", -41.87), false)
})
