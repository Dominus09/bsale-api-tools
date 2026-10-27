import { describe, expect, it } from "vitest"
import fs from "node:fs"
import path from "node:path"

const root = path.resolve(__dirname, "../..")

describe("Gestión de revisiones — grilla y cambios sin guardar", () => {
  const reviews = fs.readFileSync(
    path.join(root, "components/analytics-v2/costs/cost-v2-reviews.tsx"),
    "utf8",
  )
  const api = fs.readFileSync(path.join(root, "lib/analytics-v2/api.ts"), "utf8")
  const sql = fs.readFileSync(
    path.join(root, "../backend/sql/067_cost_reviews.sql"),
    "utf8",
  )

  it("edita prioridad y observaciones en línea con guardado", () => {
    expect(reviews).toContain("InlinePriorityCell")
    expect(reviews).toContain("InlineObservationsCell")
    expect(reviews).toContain("updateCostReviewPriority")
    expect(reviews).toContain("updateCostReviewObservations")
    expect(reviews).toContain("Guardar cambios")
    expect(reviews).toContain("Sin guardar")
  })

  it("protege cambios sin guardar al cerrar el drawer", () => {
    expect(reviews).toContain("beforeunload")
    expect(reviews).toContain("Hay cambios sin guardar")
    expect(reviews).toContain("requestClose")
    expect(reviews).toContain("dirty")
  })

  it("no edita costos Bsale y usa empresa del backend", () => {
    expect(reviews).toContain("company_name")
    expect(reviews).toContain("Empresa activa (backend)")
    expect(reviews).not.toContain("getStoredCompanyName")
    expect(reviews).toMatch(/fotograf[ií]a/i)
    expect(reviews).toContain("no se editan costos Bsale")
  })

  it("API no expone borrado de revisiones ni de eventos", () => {
    expect(api).toContain("createCostReview")
    expect(api).toContain("startCostReview")
    expect(api).toContain("resolveCostReview")
    expect(api).toContain("discardCostReview")
    expect(api).not.toMatch(/deleteCostReview/i)
    expect(api).not.toMatch(/DELETE.*costs\/reviews/i)
    expect(api).not.toMatch(/events.*delete/i)
  })

  it("SQL 067 protege historial con RESTRICT y conclusión ≥ 5", () => {
    expect(sql).toContain("ON DELETE RESTRICT")
    expect(sql).not.toMatch(/cost_review_events[\s\S]*ON DELETE CASCADE/)
    expect(sql).toContain("length(trim(conclusion)) >= 5")
    expect(sql).toContain("cost_reviews_status_coherence")
    expect(sql).toContain("rol privilegiado")
  })
})
