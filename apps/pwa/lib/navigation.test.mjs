import assert from "node:assert/strict"
import test from "node:test"
import {
  buildGoogleMapsNavigation,
  likelyIslandDestination,
  likelyUnverifiedRoadDestination,
} from "./navigation.ts"

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

test("Quellón NO dispara advertencia de ruta interrumpida", () => {
  assert.equal(likelyUnverifiedRoadDestination("Quellón", -43.12), false)
  assert.equal(likelyIslandDestination("Quellón", -43.12), false)
  const nav = buildGoogleMapsNavigation({
    latitude: -43.117,
    longitude: -73.62,
    city: "Quellón",
  })
  assert.equal(nav.likely_no_road_route, false)
  assert.equal(nav.warning_es, null)
  assert.equal(nav.google_maps_url, nav.google_maps_directions_url)
})

test("Castro tampoco se trata como sin ruta verificada", () => {
  assert.equal(likelyUnverifiedRoadDestination("Castro", -42.48), false)
})

test("likelyUnverifiedRoadDestination por ciudad Melinka y latitud austral", () => {
  assert.equal(likelyUnverifiedRoadDestination("Melinka", -43.89), true)
  assert.equal(likelyUnverifiedRoadDestination("Ancud", -41.87), false)
  assert.equal(likelyUnverifiedRoadDestination(null, -43.9), true)
  assert.equal(likelyUnverifiedRoadDestination(null, -43.1), false)
})
