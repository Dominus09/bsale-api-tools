"""Plan Etapa 3 tras corrección comercial BRUTA + 3A.1.

## 3A / 3A.1 (hecho)
- Vista default = gross commercial.
- Dos calidades: gross_cost_quality + tax_breakdown_quality.
- cost_bruto_erp autoritativo aunque other_taxes sea agregado.
- Tests A–E + 3A.1.

## 3B (hecho — job Coolify, no ejecutar desde Cursor)
python -m backend.jobs.validate_analytics_gross_cost_coverage \\
  --company-id 3 --office-id 1 --days 7 --document-limit 200

Fuentes job:
- distribuidora.documents / document_details (venta bruta/neta)
- analytics.cost_reception_history (cost_net, iva_amount, other_taxes, cost_bruto_erp)
- bsale.variant_cost.average_cost_net (fallback neto)

## Pendiente
- 3C TaxProfile histórico versionado por variante/fecha
- 3D NC asociación + agregados UI
- Separar ILA de other_taxes con certeza
"""
