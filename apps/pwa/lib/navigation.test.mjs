import assert from "node:assert/strict"
import test from "node:test"
import {
  buildGoogleMapsNavigation,
  isMelinkaDestination,
  likelyIslandDestination,
  likelyUnverifiedRoadDestination,
} from "./navigation.ts"

test("Lily Melinka: destino exacto y aviso acotado por nombre", () => {
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
  assert.match(nav.warning_es || "", /Melinka/i)
  assert.equal(/insular/i.test(nav.warning_es || ""), false)
})

test("Ancud continental usa directions por defecto", () => {
  const nav = buildGoogleMapsNavigation({
    latitude: -41.87,
    longitude: -73.82,
    city: "Ancud",
  })
  assert.equal(nav.likely_no_road_route, false)
  assert.equal(nav.warning_es, null)
  assert.equal(nav.google_maps_url, nav.google_maps_directions_url)
})

test("Quellón NO dispara advertencia ni se trata como Melinka", () => {
  assert.equal(isMelinkaDestination("Quellón"), false)
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
  const nav = buildGoogleMapsNavigation({
    latitude: -42.48,
    longitude: -73.76,
    city: "Castro",
  })
  assert.equal(nav.likely_no_road_route, false)
  assert.equal(nav.warning_es, null)
})

test("coordenadas australes sin ciudad Melinka NO se clasifican por latitud", () => {
  assert.equal(likelyUnverifiedRoadDestination(null, -43.9), false)
  assert.equal(likelyUnverifiedRoadDestination("", -44.2), false)
  assert.equal(isMelinkaDestination(null), false)
  const nav = buildGoogleMapsNavigation({
    latitude: -43.9,
    longitude: -73.7,
    city: null,
  })
  // Sin ciudad: ubicación exacta, sin afirmar isla/imposibilidad.
  assert.equal(nav.likely_no_road_route, true)
  assert.equal(nav.google_maps_url, nav.google_maps_place_url)
  assert.equal(nav.warning_es, null)
})

test("Melinka por nombre; Ancud nunca", () => {
  assert.equal(isMelinkaDestination("Melinka"), true)
  assert.equal(likelyUnverifiedRoadDestination("Melinka", -43.89), true)
  assert.equal(likelyUnverifiedRoadDestination("Ancud", -41.87), false)
})
