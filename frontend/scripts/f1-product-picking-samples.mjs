/**
 * F1 — muestras sintéticas picking producto (25/50/100/150 SKU).
 * Usa el builder productivo; no toca API ni planes reales.
 */
import { createRequire } from "module"
import fs from "fs"
import path from "path"
import { fileURLToPath, pathToFileURL } from "url"

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const ROOT = path.resolve(__dirname, "../..")
const OUT = path.resolve(ROOT, "_e12_validation_artifacts/f1_product_picking")
fs.mkdirSync(OUT, { recursive: true })

const CATEGORIES = ["BEBIDAS", "CERVEZAS", "ABARROTES", "LIMPIEZA", "AGUAS", "SNACKS"]
const SECS = [1, 6, 10, 12, 24]
const WAREHOUSES = ["BODEGA CENTRAL", "BODEGA SUR", "BODEGA ORIENTE"]

function header() {
  return {
    planning_number: "PLAN-F1-SYN",
    delivery_date: "2026-11-05",
    truck_name: "Hino 3",
    route_name: "CASTRO-DALCAHUE-COSTA",
    driver_name: "Chofer Validación",
    driver_label: "Chofer Validación",
    assistant_label: "1 peoneta",
    assistant_names: ["Ayudante 1"],
    communes: "Castro, Dalcahue",
    sello: "S-F1-001",
  }
}

function products(n) {
  const items = []
  for (let i = 0; i < n; i++) {
    const sec = SECS[i % SECS.length]
    const cat = CATEGORIES[i % CATEGORIES.length]
    const units = sec === 1 ? 12 + (i % 5) : sec * (1 + (i % 4)) + (i % 3 === 0 ? sec / 2 : 0)
    const long =
      i % 7 === 0
        ? `PRODUCTO LARGO VALIDACION ${i + 1} DESCRIPCION EXTENSA PARA PROBAR CORTE DE COLUMNA SEC${sec}`
        : `Item ${String(i + 1).padStart(3, "0")} Marca Demo SEC${sec}`
    const noBox = i % 19 === 0
    items.push({
      tipo_producto: cat,
      display_name: long,
      codigo_barras: `78016${String(10000000 + i).slice(0, 8)}${String(i).padStart(2, "0")}`,
      unidades: units,
      units_per_box: noBox ? null : sec,
      units_per_box_efectivo: noBox ? null : sec,
      sin_unidad_caja: noBox,
      cajas: noBox ? 0 : units / sec,
      total_monto: Math.round(units * (800 + (i % 40) * 25)),
      sucursal_bodega: WAREHOUSES[i % WAREHOUSES.length],
      product_id: 1000 + i,
      variant_id: 2000 + i,
    })
  }
  // Orden operativo: sucursal luego categoría (estable).
  return items.sort((a, b) => {
    const s = String(a.sucursal_bodega).localeCompare(String(b.sucursal_bodega))
    if (s) return s
    return String(a.tipo_producto).localeCompare(String(b.tipo_producto))
  })
}

function totals(items) {
  let units = 0
  let boxes = 0
  let monto = 0
  const barcodes = new Set()
  for (const it of items) {
    units += Number(it.unidades) || 0
    barcodes.add(String(it.codigo_barras))
    monto += Number(it.total_monto) || 0
    if (!it.sin_unidad_caja && it.units_per_box) {
      boxes += Math.round((Number(it.unidades) / Number(it.units_per_box)) * 100) / 100
    }
  }
  return {
    skus: barcodes.size,
    units: Math.round(units * 100) / 100,
    boxes: Math.round(boxes * 100) / 100,
    monto,
  }
}

async function loadBuilders() {
  const mod = await import(
    pathToFileURL(path.resolve(__dirname, "../lib/dispatch-plan-picking-pdf.ts")).href
  )
  return mod
}

async function main() {
  const { buildDispatchPlanPickingProductoPdf } = await loadBuilders()
  const summary = []

  for (const n of [25, 50, 100, 150]) {
    const items = products(n)
    const t = totals(items)
    const doc = await buildDispatchPlanPickingProductoPdf({
      header: header(),
      items,
      version: 1,
      generatedAt: new Date().toISOString(),
    })
    const file = path.join(OUT, `f1_productos_n${n}.pdf`)
    fs.writeFileSync(file, Buffer.from(doc.output("arraybuffer")))
    const pages = doc.getNumberOfPages()
    // Conservar barcodes y cantidades en sidecar para validación textual.
    const meta = {
      n,
      pages,
      file: path.basename(file),
      orientation: n >= 25 ? "landscape" : "portrait",
      expected: t,
      sampleBarcodes: items.slice(0, 3).map((i) => i.codigo_barras),
      lastBarcode: items[items.length - 1].codigo_barras,
      controlColumn: "CONTROL (vacía)",
    }
    summary.push(meta)
    fs.writeFileSync(path.join(OUT, `f1_productos_n${n}.meta.json`), JSON.stringify(meta, null, 2))
    console.log(
      `n=${n} pages=${pages} skus=${t.skus} units=${t.units} boxes=${t.boxes} monto=${t.monto}`,
    )
  }

  // Contraste estructural vs picking 2653 (campos / páginas de referencia).
  const refPath = path.resolve(ROOT, "_e12_validation_artifacts/ref_picking_productos_2653.pdf")
  const compare = {
    reference: "Picking x productos N° 2653.pdf",
    referencePresent: fs.existsSync(refPath),
    referenceNotes: {
      pages_reported_ops: 2,
      units_ops: 5186,
      boxes_ops: 396.1,
      fields_expected: [
        "N° picking",
        "Fecha / Ruta / Camión",
        "Sellos",
        "Sucursal",
        "Tipo / categoría",
        "Producto + variante",
        "Cajas / cantidad comercial",
      ],
      erp_columns: [
        "Sucursal",
        "Producto",
        "Código",
        "Unidades",
        "Cajas",
        "CONTROL",
        "Monto",
      ],
      note: "No comparar totales 2653 con muestras sintéticas (despachos distintos).",
    },
    samples: summary,
  }
  fs.writeFileSync(path.join(OUT, "f1_summary.json"), JSON.stringify(compare, null, 2))
  console.log("OK F1 product picking samples →", OUT)
}

main().catch((e) => {
  console.error(e)
  process.exit(1)
})
