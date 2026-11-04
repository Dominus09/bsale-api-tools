# Fase E — Auditoría y diseño (sin implementación de pagos mixtos)

Estado: **propuesta para aprobación**. Sin migraciones productivas ni cambios de persistencia financiera.

Referencia operativa: **PLAN-00078** (Hyundai `#4`, `simple_list`, 2 OC, picking v1, sin cuadratura persistida).

---

## 1. Diagnóstico PLAN-00078 (montos)

| Indicador | Valor observado | Origen real |
|-----------|-----------------|-------------|
| Venta OC | **$860.998** | `70645` $624.625 + `70639` $236.373 |
| “Venta facturada” en Resumen UI | **$860.998** | Dashboard suma **`oc_total_amount`** de OC con status `confirmed` (no el total de la factura) |
| Venta picking | **$860.996** | Σ `dispatch_plan_picking_clients.document_total` = facturas `51711`+$624.625 + `51712`+$236.371 |
| `invoiced_sales_clp` persistido | **$860.996** | Margen comercial (`variant_cost`) sobre documentos facturados |
| Margen comercial | **$124.288** | Persistido; 6/6 líneas con costo; sin ORS → sin post-transporte |
| Venta facturada en cuadratura | **$0** | **No hay fila** en `dispatch_plan_cuadratura` para plan 78; UI vacía muestra ceros |

Delta **$2**: OC `70639` = $236.373 vs factura `51712` = $236.371. No es redondeo IVA genérico; es diferencia documental real OC↔factura.

### Cómo mostrar sin confundir

1. **Venta OC (plan)** — snapshot de órdenes.  
2. **Venta facturada (documentos)** — Σ `documents.total_amount` de boletas/facturas confirmadas.  
3. **Venta picking (carga)** — congelada en picking; base de cuadratura.  
4. **Cobrado / pendiente** — solo cierre de ruta (pagos), nunca igualar a venta automáticamente.

Corrección UX/backend recomendada en E1: dejar de usar `oc_total_amount` como “venta facturada” en el dashboard.

No se reparó ni modificó PLAN-00078.

---

## 2. Arquitectura actual del cierre de ruta

- Tablas: `dispatch_plan_cuadratura` (+ v2 JSON), `dispatch_plan_cuadratura_history`, `cash_count` (036).  
- Servicios: `dispatch_plan_cuadratura_service.py`, utils `dispatch_plan_cuadratura_v2.py` / `_snapshot.py`.  
- API: `GET/PUT .../cuadratura`, `POST .../cuadratura/close`; listado `/distribuidora/cuadraturas`.  
- Modelo actual: **un `medio_pago` por documento** por el monto completo.  
- Base de cuadre v2: **`venta_picking` − NC**; arqueo de billetes vs **efectivo documental** solamente.  
- UI hoy: pestaña **Chofer y auditoría** (junto a asignación de chofer). Sin export PDF/Excel de cuadratura.

---

## 3. Diseño técnico — pagos mixtos (para aprobación)

### Principios

- Cobranza operativa ≠ factura Bsale (no mutar Bsale).  
- NC ≠ medio de pago (siguen en `credit_notes_v2`).  
- Un documento puede tener N líneas de pago.  
- Suma(pagos) + saldo pendiente = monto exigible (picking/doc).  
- Arqueo solo vs efectivo registrado.

### Compatibilidad

```json
{
  "related_document_id": 3968161,
  "monto_clp": 624625,
  "medio_pago": "cheque",
  "pagos": [
    { "medio_pago": "efectivo", "monto_clp": 40000, "referencia": null },
    { "medio_pago": "transferencia", "monto_clp": 60000, "referencia": "TRX-…" }
  ]
}
```

- Si `pagos` ausente/vacío → comportamiento actual (`[{medio_pago, monto_clp}]`).  
- Historial cerrado: snapshot JSON inmutable.  
- Opcional (E4): tabla `dispatch_plan_cuadratura_payment_lines` + JSON como proyección UI.

### Validaciones

- Medios ∈ catálogo actual (`efectivo|transferencia|cheque|debito|credito|caja_vecina|pendiente|…`).  
- Montos ≥ 0; suma ≤ monto exigible; pendiente = resto.  
- Transferencia: referencia opcional/recomendada.  
- Cerrada (`squared` / `closed_at`): solo corrección con permiso + historial.  
- Idempotencia: version/etag o `updated_at` optimistic lock.  
- Consolidado: totales por medio, cobrado, pendiente, no entregados, arqueo.

**No implementar hasta aprobación.**

---

## 4. Asignación documental del picking (“Sin asignar”)

No es inconsistencia de generación:

1. Picking plan-level se genera y congela clientes/productos.  
2. `_sync_assignments_from_picking` crea filas en `dispatch_plan_document_load_assignments` con **`load_batch_id = NULL`**.  
3. UI “Sin asignar” = documento **en el picking del plan** pero **sin lote físico** (Picking 1/2/N).

PLAN-00078: 2 docs (`3968161`, `3968163`) con `load_batch_id` null — esperado si no se usó multi-carga.

---

## 5. Generadores Excel / PDF

| Artefacto | Ubicación |
|-----------|-----------|
| Excel cliente | `backend/utils/dispatch_plan_picking.py` → `build_picking_client_excel` |
| Excel producto | mismo → `build_picking_product_excel` |
| Endpoints export | `GET .../picking-cliente/export`, `.../picking-producto/export` |
| PDF cliente/producto | `frontend/lib/dispatch-plan-picking-pdf.ts` (+ logo `quillotana-logo-pdf`) |
| Cuadratura export | **no existe** hoy |

---

## 6. Mejoras visuales de exportaciones (propuesta)

**Picking clientes PDF/Excel:** empresa, PLAN, fecha, camión, cliente, dirección/comuna, Nº doc, líneas, obs. entrega, casillas preparación/recepción; filtros y freeze panes en Excel.

**Picking productos:** código/barras, descripción, qty, unidades/cajas reales, categoría/bodega, columnas preparada/diferencia.

**Cuadratura (nuevo en E3):** resumen admin imprimible — cobrado por medio, mixtos, pendientes, NC, arqueo, firmas; separar venta documentada vs efectivo contado.

Sin cambiar cantidades ni semántica comercial.

---

## 7. Reorganización de pestañas

| # | Pestaña | Contenido |
|---|---------|-----------|
| 1 | Resumen | KPIs claros (OC ≠ facturada ≠ picking); margen; avisos; sin duplicar dashboard |
| 2 | Órdenes y facturación | Tabla OC + docs; conciliación; sin repetir todo el dashboard |
| 3 | Ruta y costos | Mapa embebido si hay geo; km/cobertura; post-transporte |
| 4 | Picking y carga | Cliente / producto / lotes (subsecciones o subtabs) |
| 5 | **Chofer** | Solo asignación + historial |
| 6 | **Cierre de ruta y cuadratura** | Panel cuadratura + futuro export |

Page chrome: Excel / Reparar; header: refresh comercial + mapa.  
Simple-planning: banner **Despacho confirmado** + CTA rojo ancho **Abrir despacho PLAN-XXXXX**.

---

## 8. Cambios previstos (post-aprobación)

| Capa | E1 UI | E2 Picking exports | E3 Cuadratura UI/export | E4 Pagos mixtos |
|------|-------|--------------------|-------------------------|-----------------|
| Frontend | Shell tabs, labels, CTA confirm | PDF layout | Panel + print | Editor splits |
| Backend | Fix venta facturada = docs | Excel polish | Export summary | Validación splits |
| PostgreSQL | — | — | — | Migración solo si se aprueba tabla líneas |

Sin tocar ORS, predespacho, crons, analytics writes.

---

## 9. Riesgos compatibilidad cuadraturas antiguas

- GET puede promover mentalmente a v2 si hay picking.  
- v1 totales globales vs v2 por documento.  
- Regenerar picking cambia `venta_picking` vs medios guardados.  
- Mixed: fallback obligatorio si no hay `pagos[]`.  
- No sumar header `medio_pago` y `pagos` a la vez.

---

## 10. Plan de commits

1. **E1** — UI detalle (6ª pestaña), labels venta, CTA Abrir despacho, fix métrica facturada.  
2. **E2** — Excel/PDF picking visual (semántica intacta).  
3. **E3** — Presentación/export cuadratura (sin mixed persist).  
4. **E4** — Pagos mixtos + migración si hace falta (**tras aprobación**).  
5. **E5** — Tests regresión/seguridad (ORS tradicional, picking, close).

Prioridad de integridad: **corregir “venta facturada” = OC** antes de polish visual.
