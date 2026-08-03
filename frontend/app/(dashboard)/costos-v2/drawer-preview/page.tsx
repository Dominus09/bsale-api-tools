"use client"

import { useState } from "react"

import { CostV2ProductDetailDrawer } from "@/components/costos-v2/cost-v2-product-detail-drawer"
import { Button } from "@/components/ui/button"
import type { CostV2ProductItem } from "@/lib/costos-v2/types"

/**
 * Preview local del drawer rediseñado (fixtures).
 * Ruta: /costos-v2/drawer-preview
 * No usa API ni despliegue.
 */

const johnnie: CostV2ProductItem = {
  variant_id: 1001,
  company_id: null,
  office_id: null,
  barcode: "5000267013602",
  product_name: "JOHNNIE WALKER",
  variant_name: "Red Label 1 LT",
  latest_history_id: 501,
  latest_admission_date: "2026-07-31",
  latest_document_number: 18358,
  current_stored_cost_net: "6510",
  current_stored_gross_cost: "6510",
  current_corrected_gross_cost: "9798",
  current_calculated_iva_amount: "1237",
  current_additional_tax_amount_total: "2051",
  current_additional_taxes: [
    {
      tax_id: 10,
      name: "ILA destilados",
      rate: "0.315",
      category: "ila",
      amount: "2051",
    },
  ],
  current_total_tax_rate: "0.505",
  current_quality_status: "missing_taxes_in_gross",
  current_warnings: [],
  previous_history_id: 500,
  previous_admission_date: "2026-06-15",
  previous_corrected_gross_cost: "9798",
  unit_change_amount: "0",
  unit_change_percent: "0",
  receptions_count: 3,
  last_calculated_at: "2026-08-01T12:00:00Z",
  tax_ids_source: "current_product_tax",
  tax_rates_source: "bsale_taxes",
  calculation_version: "cost-v2.0.0",
  source_history_fingerprint: "abc123",
  tax_context_fingerprint: "def456",
  calculation_result_fingerprint: "ghi789",
  calculation: {
    stored_cost_net: "6510",
    iva: { tax_id: 1, rate: "0.19", amount: "1237" },
    additional_taxes: [
      {
        tax_id: 10,
        name: "ILA destilados",
        rate: "0.315",
        category: "ila",
        amount: "2051",
      },
    ],
    corrected_gross_cost: "9798",
    formula: "net + iva + additional",
  },
  receptions: [
    {
      history_id: 501,
      company_id: null,
      office_id: null,
      admission_date: "2026-07-31",
      document_number: 18358,
      document: null,
      reception_id: 1,
      variant_id: 1001,
      barcode: "5000267013602",
      product_name: "JOHNNIE WALKER",
      variant_name: "Red Label 1 LT",
      stored_cost_net: "6510",
      stored_cost_gross: "6510",
      stored_iva_amount: null,
      stored_other_taxes: null,
      stored_quantity: "12",
      corrected_gross_cost: "9798",
      calculated_iva_amount: "1237",
      additional_tax_amount_total: "2051",
      total_tax_rate: "0.505",
      resolved_tax_ids: [],
      additional_taxes: [],
      tax_ids_source: "current_product_tax",
      tax_rates_source: "bsale_taxes",
      tax_context_quality: "ok",
      historical_tax_context_available: true,
      effective_quality_status: "missing_taxes_in_gross",
      warnings: [],
      suspicious_outlier: false,
      calculation_version: "cost-v2.0.0",
      calculation_batch_id: "batch-1",
      calculated_at: "2026-08-01T12:00:00Z",
      source_history_fingerprint: "abc",
      tax_context_fingerprint: "def",
      calculation_result_fingerprint: "ghi",
      unit_difference: "3288",
    },
    {
      history_id: 500,
      company_id: null,
      office_id: null,
      admission_date: "2026-06-15",
      document_number: 18001,
      document: null,
      reception_id: 2,
      variant_id: 1001,
      barcode: "5000267013602",
      product_name: "JOHNNIE WALKER",
      variant_name: "Red Label 1 LT",
      stored_cost_net: "6510",
      stored_cost_gross: "6510",
      stored_iva_amount: null,
      stored_other_taxes: null,
      stored_quantity: "6",
      corrected_gross_cost: "9798",
      calculated_iva_amount: "1237",
      additional_tax_amount_total: "2051",
      total_tax_rate: "0.505",
      resolved_tax_ids: [],
      additional_taxes: [],
      tax_ids_source: "current_product_tax",
      tax_rates_source: "bsale_taxes",
      tax_context_quality: "ok",
      historical_tax_context_available: true,
      effective_quality_status: "missing_taxes_in_gross",
      warnings: [],
      suspicious_outlier: false,
      calculation_version: "cost-v2.0.0",
      calculation_batch_id: "batch-0",
      calculated_at: "2026-07-01T12:00:00Z",
      source_history_fingerprint: "abc0",
      tax_context_fingerprint: "def0",
      calculation_result_fingerprint: "ghi0",
      unit_difference: "3288",
    },
  ],
}

const chandelle: CostV2ProductItem = {
  ...johnnie,
  variant_id: 2002,
  barcode: "7613034626844",
  product_name: "CHANDELLE NESTLE",
  variant_name: "Chocolate 110g",
  latest_document_number: 19102,
  latest_admission_date: "2026-07-28",
  current_stored_cost_net: "532",
  current_stored_gross_cost: "665",
  current_corrected_gross_cost: "665",
  current_calculated_iva_amount: "101",
  current_additional_tax_amount_total: "32",
  current_additional_taxes: [
    {
      tax_id: 20,
      name: "Anticipo harina",
      rate: "0.12",
      category: "iva_advance",
      amount: "32",
    },
  ],
  current_total_tax_rate: "0.25",
  current_quality_status: "valid_gross",
  previous_corrected_gross_cost: "532",
  unit_change_amount: "133",
  unit_change_percent: "25.1",
  calculation: {
    stored_cost_net: "532",
    iva: { tax_id: 1, rate: "0.19", amount: "101" },
    additional_taxes: [
      {
        tax_id: 20,
        name: "Anticipo harina",
        rate: "0.12",
        category: "iva_advance",
        amount: "32",
      },
    ],
    corrected_gross_cost: "665",
    formula: "net + iva + additional",
  },
  receptions: [
    {
      ...johnnie.receptions![0],
      history_id: 601,
      variant_id: 2002,
      barcode: "7613034626844",
      product_name: "CHANDELLE NESTLE",
      variant_name: "Chocolate 110g",
      admission_date: "2026-07-28",
      document_number: 19102,
      stored_cost_net: "532",
      stored_cost_gross: "665",
      corrected_gross_cost: "665",
      calculated_iva_amount: "101",
      additional_tax_amount_total: "32",
      total_tax_rate: "0.25",
      effective_quality_status: "valid_gross",
    },
    {
      ...johnnie.receptions![1],
      history_id: 600,
      variant_id: 2002,
      barcode: "7613034626844",
      product_name: "CHANDELLE NESTLE",
      variant_name: "Chocolate 110g",
      admission_date: "2026-05-10",
      document_number: 17001,
      stored_cost_net: "426",
      stored_cost_gross: "532",
      corrected_gross_cost: "532",
      calculated_iva_amount: "81",
      additional_tax_amount_total: "25",
      total_tax_rate: "0.25",
      effective_quality_status: "valid_gross",
    },
  ],
}

const incomplete: CostV2ProductItem = {
  ...johnnie,
  variant_id: 3003,
  barcode: "7800000000123",
  product_name: "PRODUCTO SIN CONTEXTO",
  variant_name: "Genérico 1 kg",
  latest_document_number: 20001,
  latest_admission_date: "2026-07-20",
  current_stored_cost_net: "1000",
  current_stored_gross_cost: "1000",
  current_corrected_gross_cost: null,
  current_calculated_iva_amount: null,
  current_additional_tax_amount_total: null,
  current_additional_taxes: [],
  current_total_tax_rate: null,
  current_quality_status: "incomplete_tax_context",
  current_warnings: ["reception_tax_context_unavailable"],
  previous_corrected_gross_cost: null,
  unit_change_amount: null,
  unit_change_percent: null,
  tax_ids_source: null,
  tax_rates_source: null,
  calculation: undefined,
  receptions: [
    {
      ...johnnie.receptions![0],
      history_id: 701,
      variant_id: 3003,
      barcode: "7800000000123",
      product_name: "PRODUCTO SIN CONTEXTO",
      variant_name: "Genérico 1 kg",
      admission_date: "2026-07-20",
      document_number: 20001,
      stored_cost_net: "1000",
      stored_cost_gross: "1000",
      corrected_gross_cost: null,
      calculated_iva_amount: null,
      additional_tax_amount_total: null,
      total_tax_rate: null,
      effective_quality_status: "incomplete_tax_context",
      warnings: ["reception_tax_context_unavailable"],
      tax_ids_source: null,
      tax_rates_source: null,
    },
  ],
}

const FIXTURES: { key: string; label: string; item: CostV2ProductItem }[] = [
  { key: "johnnie", label: "JOHNNIE WALKER", item: johnnie },
  { key: "chandelle", label: "CHANDELLE NESTLE", item: chandelle },
  {
    key: "incomplete",
    label: "Contexto tributario incompleto",
    item: incomplete,
  },
]

export default function CostV2DrawerPreviewPage() {
  const [active, setActive] = useState<string | null>("johnnie")
  const fixture = FIXTURES.find((f) => f.key === active) ?? null

  return (
    <div className="mx-auto max-w-3xl space-y-4 p-6">
      <div>
        <h1 className="text-lg font-semibold">Preview drawer Costos V2</h1>
        <p className="text-sm text-muted-foreground">
          Fixtures locales — no llama API. Solo para revisión visual.
        </p>
      </div>
      <div className="flex flex-wrap gap-2">
        {FIXTURES.map((f) => (
          <Button
            key={f.key}
            type="button"
            variant={active === f.key ? "default" : "outline"}
            onClick={() => setActive(f.key)}
          >
            {f.label}
          </Button>
        ))}
      </div>
      {fixture ? (
        <CostV2ProductDetailDrawer
          open
          onOpenChange={() => setActive(null)}
          variantId={fixture.item.variant_id}
          companyId={3}
          officeId={3}
          dateFrom="2026-01-01"
          dateTo="2026-08-01"
          previewItem={fixture.item}
        />
      ) : null}
    </div>
  )
}
