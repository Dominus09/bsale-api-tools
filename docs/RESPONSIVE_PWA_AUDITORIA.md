# Responsive & PWA 5.1 — Matriz de auditoría (Etapa A)

Base única: Next.js 16 (App Router) + Design System Graphite Premium 4.1.
Alcance de esta etapa: inventario real completo, fundamentos del layout
compartido, navegación móvil, PWA instalable (manifest, íconos oficiales,
metadata iOS, service worker seguro) y pantallas prioritarias. Sin cambios de
backend, autenticación, permisos ni lógica operativa.

## Fundamentos compartidos ya presentes (verificados en código)

- Layout dashboard: sidebar en flujo en escritorio; **drawer** con overlay
  bajo 768 px que se cierra al navegar; `100dvh`; zona segura superior;
  `main` con `overflow-x-hidden` (el desplazamiento horizontal vive dentro
  de contenedores de tabla, nunca en la página).
- Componentes V2 (`analytics-ui`): `ProfessionalDataTable` con scroll
  horizontal controlado, `SmartFilterBar` con filtros avanzados plegables,
  tablas con **alternativa en tarjetas** en móvil (`cost-v2-products`,
  `margin-v2-table`, `pack-v2-table`, góndolas) y drawers de detalle a ancho
  completo en móvil (`sm:max-w-[min(1050px,92vw)]`).
- Inputs shadcn: `text-base` en móvil (16 px, sin zoom automático de Safari)
  y `text-sm` desde `md`.
- Tema Graphite claro/oscuro persistente solo en rutas adaptadas
  (Dashboard/Costos/Márgenes/Packs V2 y Catálogo); el resto permanece claro.

## Correcciones de Etapa A

| Área | Corrección |
|---|---|
| PWA | `app/manifest.ts` (La Quillotana ERP / Quillotana, standalone, Graphite, `start_url: "/"` que recupera sesión vía raíz) |
| Íconos | Generados desde el **emblema oficial** (5484×4082, sin redibujar: recorte proporcional + fondo Graphite): `public/icons/pwa/icon-192.png`, `icon-512.png`, `icon-maskable-512.png`, `apple-touch-icon.png` (180) |
| iOS | `apple-touch-icon`, `appleWebApp` (capable, título, `black-translucent`), `viewportFit: cover`, `themeColor` Graphite en layout raíz |
| Offline | `public/sw.js`: solo íconos/manifest/`/offline` en caché; **nunca** API, tokens ni datos; navegaciones siempre a red con fallback informativo. Registro solo en producción (`PwaRegister`) |
| Login | `min-h-dvh`, zonas seguras, `autoComplete`/`inputMode`, objetivos táctiles 44 px |
| Selector de empresa | `min-h-dvh`, zonas seguras |
| Header | Botones de menú y usuario a 44 px en móvil |
| Paginador V2 | Flechas 40 px en móvil |
| Botones Excel (Catálogo/Costos/Márgenes) | 40 px en móvil |
| Globales | `overflow-x: clip` en `body`, utilidades de zona segura y `.table-scroll-x` |

## Inventario de rutas (69) y estado

Estado: **A** = adaptada/verificada en Etapa A · **B** = base compartida OK,
revisión fina en Etapa B · **M** = mapa/pantalla especial, revisión en Etapa B.

### Acceso e inicio
| Ruta | Módulo | Estado responsive | Prioridad | Estado corrección |
|---|---|---|---|---|
| `/login` | Acceso | Formulario centrado, inputs 16 px | Alta | A — corregida |
| `/company-selector` | Acceso | Tarjetas 1→2→3 columnas | Alta | A — corregida |
| `/` (home-client) | Inicio | Redirección por sesión/empresa | Alta | A — start_url PWA |
| `/offline` | PWA | Página informativa sin conexión | Alta | A — nueva |

### Inteligencia Comercial (prioritarias)
| Ruta | Componentes principales | Estado responsive | Prioridad | Estado corrección |
|---|---|---|---|---|
| `/analitica-v2/dashboard` | `dashboard-sections`, panel Graphite | Tablas con scroll controlado, rejillas adaptables | Alta | A — verificada |
| `/inteligencia-comercial/catalogo` | `ProfessionalDataTable`, filtros | Tabla ancha con scroll interno; filtros 1→2→4 col | Alta | A — verificada + táctil |
| `/analitica-v2/costos` | `cost-v2-products` (tarjetas móvil), drawer detalle | Tarjetas <md, tabla ≥md, drawer ancho completo | Alta | A — verificada + táctil |
| `/analitica-v2/margenes` | `margin-v2-table` (tarjetas <lg) | Tarjetas <lg, tabla ≥lg | Alta | A — verificada + táctil |
| `/analitica-v2/packs` | `pack-v2-table` (tarjetas <md), sheet detalle | Tarjetas <md, sheet adaptable | Alta | A — verificada |
| `/analitica-v2/promociones` | `promotion-v2-*` | Tablas con scroll interno, dialogs 85vh | Media | B |
| `/gondolas` | Escaneo cámara/USB, tarjetas <md | Tarjetas móvil + tabla ≥md (sin tocar escaneo) | Alta | A — verificada |

### Panel y legacy comercial
| Ruta | Módulo | Estado | Prioridad | Corrección |
|---|---|---|---|---|
| `/dashboard` | Inicio legacy | Base compartida | Media | B |
| `/alerts` | Alertas | Base compartida | Media | B |
| `/margins` | Márgenes legacy | Base compartida | Baja | B |
| `/politica-margenes` | Config. comercial legacy | Base compartida | Baja | B |
| `/promotions` | Promociones legacy | Base compartida | Baja | B |
| `/costos`, `/costos/productos/[variantId]` | Costos legacy | Base compartida | Baja | B |
| `/products-without-cost` | Costos legacy | Base compartida | Baja | B |
| `/orders`, `/orders/[id]` | Órdenes legacy | Base compartida | Baja | B |
| `/analitica/comercial-vendedores` | Analítica legacy | Base compartida | Baja | B |
| `/analitica/notas-credito` | Analítica legacy | Base compartida | Baja | B |
| `/admin/diagnostico` | Administración | Base compartida | Baja | B |

### Compras
| Ruta | Módulo | Estado | Prioridad | Corrección |
|---|---|---|---|---|
| `/compras/generar-oc` | Compras | Base compartida | Alta (Etapa B) | B |
| `/compras/productos` | Compras | Base compartida | Alta (Etapa B) | B |
| `/compras/proveedores` | Compras | Base compartida | Media | B |
| `/compras/registros-oc` | Compras | Base compartida | Media | B |
| `/distribuidora/ordenes-compra` | Compras/Distribuidora | Base compartida | Media | B |
| `/distribuidora/pre-despacho-oc` | Compras/Distribuidora | Base compartida | Media | B |

### Distribuidora y logística
| Ruta | Módulo | Estado | Prioridad | Corrección |
|---|---|---|---|---|
| `/distribuidora/dashboard` | Distribuidora | Base compartida | Alta (Etapa B) | B |
| `/distribuidora/clientes` (+`/analisis`, `/inactivos`) | Distribuidora | Base compartida | Media | B |
| `/distribuidora/cuadraturas` (+`/[id]`) | Distribuidora | Base compartida | Media | B |
| `/distribuidora/dispatch-analysis` | Distribuidora | Base compartida | Media | B |
| `/distribuidora/maestro-logistico` | Distribuidora | Base compartida | Media | B |
| `/distribuidora/mapa` | Mapas | Pantalla de mapa (layout sin padding) | Media | M |
| `/distribuidora/orders` | Distribuidora | **Renovada Graphite 5.2 (B1)** | Alta | **Implementada, pendiente de validación visual** |
| `/distribuidora/pendientes` | Distribuidora | Base compartida | Media | B |
| `/distribuidora/planificacion`, `/pre-planificacion`, `/planning` | Planificación | Base compartida | Alta (Etapa B) | B |
| `/distribuidora/planificaciones` (+`/[id]`, `/[id]/mapa`) | Planificación/Mapas | Base compartida / mapa | Media | B / M |
| `/distribuidora/resumen-vendedor`, `/vendedores`, `/rutero`, `/sin-georef` | Distribuidora | Base compartida | Media | B |
| `/logistica/cargas` (+`/[id]`, `/certificar`, `/mapa`, `/nueva`) | Bodega y cargas | Base compartida / mapa | Alta (Etapa B) | B / M |
| `/logistica/mapa-entregas/[code]` | Mapas | Mapa sin padding | Media | M |
| `/logistica/auditoria-pesos`, `/logistica/peso-ordenes` | Logística | Base compartida | Media | B |
| `/operaciones` (+`dashboard`, `georreferencias`, `incidencias`, `mapa`, `vendedor/[codigo]`, `vendedores`) | Operaciones | Base compartida / mapa | Media | B / M |

### Sucursales y góndolas
| Ruta | Módulo | Estado | Prioridad | Corrección |
|---|---|---|---|---|
| `/sucursales/etiquetas`, `/sucursales/etiquetas2` | Etiquetas | Base compartida (flujo de impresión) | Media | B |
| `/sucursales/ofertas` | Sucursales | Base compartida | Media | B |
| `/sucursales/recepciones`, `/sucursales/trazabilidad` | Sucursales | Base compartida | Media | B |
| `/gondolas/recorridos/[id]` | Góndolas | Operativa en terreno (iPhone) | Alta | B — sin tocar escaneo |

## Graphite Premium 5.2 — Etapa B1: `/distribuidora/orders` (Pre‑despacho OC)

Renovación visual sin cambios de lógica operativa (mismos handlers, estados
draft/applied, selección de órdenes, asignación de camiones, sincronización y
paso a planificación):

- **Tema oscuro habilitado solo en esta ruta** de Distribuidora:
  `/distribuidora/orders` se agregó a `DARK_THEME_ROUTE_PREFIXES`
  (`frontend/lib/appearance.ts`), por lo que la preferencia claro/oscuro y el
  botón luna/sol ahora aplican también aquí, igual que en Inteligencia
  Comercial V2. Las demás rutas de Distribuidora siguen en claro hasta su
  propia etapa (evita el conflicto de cascada CSS ya corregido en globals.css:
  el bloque `.dark` de tokens `--lq-*` permanece después del `:root` claro).
- **Encabezado unificado**: eyebrow, título 28/32 px y descripción con tokens
  `--lq-*`; acciones «Recargar vista» y «Sync Bsale» a 44 px en móvil / 36 px
  en escritorio. Se eliminó el hack `-m-6` de la raíz (la pantalla usa el
  padding estándar del layout, como V2).
- **KPI superiores** (`PreDespachoKpiStrip`): tarjetas `rounded-xl` con
  tokens lq, altura mínima uniforme (104 px), etiqueta 12 px, valor 24 px,
  grilla 2 columnas en móvil / 5 en escritorio; mismos acentos semánticos y
  mismo comportamiento clicable (fijar estado resumen).
- **FilterBar unificada**: fechas y buscador con controles de 44 px en móvil
  (área táctil ≥ 44 × 44 px en los controles principales);
  ayuda del buscador 10→12 px; fila secundaria con «Solo no facturadas»,
  «Mostrar todo» y «Estado resumen» integrados en la misma tarjeta. En móvil
  los filtros secundarios permanecen **visibles apilados** (no se ocultan en
  un drawer) porque son controles operativos críticos y ocultar el estado del
  filtro activo induciría errores; todos con altura táctil ≥ 44 px en móvil.
- **Resumen por comuna**: mini-KPI con tokens lq y valores 22 px; tabla «Por
  comuna» en tarjeta con scroll horizontal interno; chips de «Observaciones»
  con altura táctil de 44 px en móvil.
- **Tabla de planificación** (`PreDespachoPlanningTable`): encabezados
  10→11 px, datos 12→13 px, selector de camión 44 px en móvil / 36 px en
  escritorio, franja de grupo
  migrada de slate fijo a token `muted`. Se conservan columna «Camión»
  sticky, scroll interno vertical/horizontal, agrupación por comuna y todas
  las acciones.
- **Overlay de carga** (`PreDespachoLoadingOverlay`): migrado de colores
  slate fijos a tokens semánticos (correcto en claro y oscuro).
- **Chips de estado** (`PreDespachoStatusChips`): altura táctil 44 px en
  móvil / 36 px en escritorio y texto 13 px.
- **Revisión previa a publicación (5.2 B1)**: diff verificado como
  exclusivamente visual/responsive (handlers, estados, selección de camiones
  y sincronización intactos); controles principales con área táctil
  ≥ 44 × 44 px en móvil; encabezado de la columna sticky «Camión» con fondo
  opaco para evitar superposiciones al desplazar. Los enlaces densos dentro
  de celdas de datos (detalle de peso) mantienen densidad de tabla con área
  táctil ampliada en móvil; el input del drawer de pesos (40 px) queda como
  observación menor para una etapa posterior.

### Estado de la ruta (matriz 5.2)

| Ruta | Pendiente | En implementación | Implementada | Validada escritorio | Validada tablet | Validada móvil | Validada claro/oscuro |
|---|---|---|---|---|---|---|---|
| `/distribuidora/orders` | — | — | **Sí (B1)** | Pendiente | Pendiente | Pendiente | Pendiente |

La ruta está **implementada**, no «validada»: sin navegador automatizado ni
dispositivos reales en este entorno, la validación visual (1366/1440/1920,
768/1024, 360/390/430 px, temas claro y oscuro, tablas, filtros, modales y
drawers) queda **pendiente** y debe ejecutarse tras el despliegue autorizado.
TypeScript: 62 líneas (línea base, 0 errores nuevos). Build: correcto.

### Componentes compartidos reutilizables para las siguientes pantallas

Ya existen y deben reutilizarse (no crear paralelos), en
`frontend/components/analytics-v2/shared/analytics-ui.tsx` y `tokens.ts`:

- `AnalyticPageShell` / `AnalyticPageHeader`: contenedor y encabezado
  estándar (eyebrow, título, descripción, acciones).
- `MetricCard`: KPI estándar con tokens lq.
- `SmartFilterBar`: filtros principales + avanzados (ojo: renderiza
  `advanced` dos veces —móvil/escritorio—, no usar con controles con `id`
  duplicado; en ese caso, patrón FilterBar de esta página).
- `ProfessionalDataTable`: tabla con scroll interno y estados.
- `SectionCard`, `StatusBadge`, `DataStatusBar`, `EmptyState`,
  `AnalyticsLoading`, `AttentionRow`.
- Tokens `--lq-*` y utilidades Tailwind (`border-lq-line`, `bg-lq-surface`,
  `text-lq-ink/muted/faint`, `shadow-lq-xs/sm/md`): usar siempre en vez de
  colores fijos para que claro/oscuro funcionen sin variantes manuales.
- Patrón responsive de esta entrega: controles `h-11 sm:h-9` (44 px en
  móvil), tarjetas
  `rounded-xl border-lq-line bg-lq-surface shadow-lq-xs`, tipografía mínima
  11 px en encabezados de tabla y 13 px en datos.

### Siguiente grupo recomendado

Resto de Distribuidora según esta matriz: `/distribuidora/dashboard`,
`/distribuidora/planificacion` (+ `pre-planificacion`/`planning`),
`/distribuidora/planificaciones`, `/distribuidora/cuadraturas`,
`/distribuidora/rutero`/`vendedores`/`resumen-vendedor`, mapas
(`/distribuidora/mapa`, `/logistica/mapa-entregas/[code]`) con revisión
especial de controles táctiles, habilitando el tema oscuro ruta por ruta.

## Problemas detectados (resumen)

1. ~~Sin manifest ni íconos PWA~~ → corregido en Etapa A.
2. ~~Metadata iOS incompleta (apple icon genérico SVG)~~ → corregido.
3. ~~Sin viewport `cover` ni tema PWA~~ → corregido.
4. Header con objetivos de 40 px → 44 px en móvil (corregido).
5. Módulos legacy y Distribuidora sin revisión fina de tablas/formularios en
   360–430 px → Etapa B (la base compartida evita overflow de página, pero
   pueden existir tablas anchas sin contenedor en pantallas puntuales).
6. Mapas a pantalla completa: verificar controles táctiles y paneles en
   móvil → Etapa B (sin cambios de lógica).

## Pendientes de validación real (no afirmados como probados)

- Instalación «Agregar a Inicio» en iPhone/iPad y apertura standalone.
- Comportamiento del tema claro/oscuro al abrir la PWA instalada.
- Pruebas en dispositivos reales 360/390/430/768/1024 px y escritorio.
- La verificación de esta etapa es estática + build (ver informe de entrega).
