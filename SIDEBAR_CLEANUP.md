# FASE UI 1 — Limpieza menú lateral Distribuidora

Documentación de la simplificación del sidebar del módulo **Distribuidora** en `frontend/components/layout/sidebar.tsx`.

**Alcance:** solo navegación lateral. Rutas, páginas, API y componentes siguen intactos.

---

## Items removidos del menú

| Etiqueta | Ruta | Motivo |
|----------|------|--------|
| Clientes | `/distribuidora/clientes` | Módulo cartera no priorizado en flujo operacional actual |
| Análisis clientes | `/distribuidora/clientes/analisis` | Analítica de cartera; uso esporádico / en desarrollo |
| Clientes inactivos | `/distribuidora/clientes/inactivos` | Submódulo de cartera; no operación diaria |
| Vendedores | `/distribuidora/vendedores` | Gestión de fuerza de venta; fuera del foco despacho/ORS |
| Análisis despacho | `/distribuidora/dispatch-analysis` | Reportes; no bloquea operación OC → despacho |
| Pre‑planif. despacho | `/distribuidora/pre-planificacion` | Paso intermedio; flujo consolidado en pre-despacho y ORS |
| Planificación | `/distribuidora/planning` | Duplicado conceptual con **Planif. mapa ORS** (`/distribuidora/planificacion`) |

### Restaurados (bugfix UI — operación vendedores)

Estos ítems se habían ocultado por error en la fase 1; **vuelven al menú** por ser herramientas operativas en uso:

| Etiqueta | Ruta |
|----------|------|
| Rutero vendedores | `/distribuidora/rutero` |
| Mapa rutero | `/distribuidora/mapa` |
| Resumen vendedor | `/distribuidora/resumen-vendedor` |

La sección **Operaciones** (`/operaciones/*`) no se modificó en la fase 1.

---

## Items visibles

| Etiqueta | Ruta | Rol en operación |
|----------|------|------------------|
| Dashboard comercial | `/distribuidora/dashboard` | Vista resumen; sync y KPIs sin romper jerarquía visual |
| Órdenes de compra | `/distribuidora/ordenes-compra` | Entrada del flujo: OC desde proveedor |
| Pre‑despacho OC | `/distribuidora/orders` | Preparación de despacho sobre OC |
| Planif. mapa ORS | `/distribuidora/planificacion` | Planificación operativa en mapa (ORS) |
| Rutero vendedores | `/distribuidora/rutero` | Operación diaria de rutas por vendedor |
| Mapa rutero | `/distribuidora/mapa` | Vista geográfica del rutero |
| Resumen vendedor | `/distribuidora/resumen-vendedor` | Resumen operativo por vendedor |

Iconografía: **Órdenes de compra** (`ShoppingCart`) y **Pre‑despacho OC** (`PackageCheck`) para distinguir pasos del mismo flujo.

---

## Rationale UX

1. **Foco operacional** — El ERP en Distribuidora se usa hoy para el circuito OC → pre-despacho → planificación ORS. Mostrar módulos incompletos o poco usados genera fricción y sensación de producto “a medias”.
2. **Menú minimalista** — Siete entradas (despacho OC + rutero/vendedores); sin módulos de cartera ni planificación legacy en el menú.
3. **Sin deuda técnica** — Ocultar en `navSections` no borra código: equipos pueden seguir probando rutas, bookmarks y links internos en páginas.
4. **Estados visuales** — Activo con `bg-primary/10` y texto primario (sin bloque sólido ni chevron); hover `muted/70` con transición 150ms; padding uniforme `px-3 py-2.5`; iconos 18px alineados; submenú con borde izquierdo más definido y `space-y-0.5`.

---

## Posibles módulos futuros (re-exposición en sidebar)

Cuando estén maduros o con demanda operativa, candidatos a volver al menú (orden sugerido):

| Módulo | Ruta | Cuándo reactivar |
|--------|------|------------------|
| Clientes / Análisis / Inactivos | `/distribuidora/clientes*` | Cartera y CRM integrados al despacho |
| Vendedores | `/distribuidora/vendedores` | Asignación diaria de rutas por vendedor |
| Análisis despacho | `/distribuidora/dispatch-analysis` | KPIs de cumplimiento estables |
| Pre‑planif. despacho | `/distribuidora/pre-planificacion` | Si el paso vuelve a ser obligatorio antes de ORS |
| Planificación (legacy) | `/distribuidora/planning` | Solo si diverge de `planificacion` y ambos coexisten |

**Implementación futura:** añadir de nuevo entradas en el array `items` de la sección `"Distribuidora"` en `sidebar.tsx`, o introducir flag por rol (`hidden?: boolean` / feature flags) sin tocar rutas.

---

## Archivos tocados

- `frontend/components/layout/sidebar.tsx` — definición del menú y estilos del sidebar
- `SIDEBAR_CLEANUP.md` — este documento

## No modificado (por diseño)

- Rutas en `frontend/app/(dashboard)/distribuidora/**`
- Endpoints y servicios backend
- Permisos / roles
- Imports dinámicos de páginas
- Links contextuales dentro de páginas (ej. dashboard → clientes)

---

*Fase UI 1 — mayo 2026*
