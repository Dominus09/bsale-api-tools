# Responsive & PWA 5.1 — Matriz de auditoría (Etapa A)

> **Actualización Graphite Premium 6.0 (ver sección al final):** corrección
> del error React #310 en Análisis de Márgenes, migración de colores fijos a
> tokens en todo el frontend y tema oscuro habilitado en todos los módulos
> del dashboard. Las filas marcadas «B»/«M» en las tablas siguientes quedan
> **implementadas visualmente (6.0)** — migración de tokens + Dark — y
> **pendientes de validación visual** por dispositivo; la tabla histórica
> 5.1 se conserva como registro.

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

---

# Graphite Premium 6.0 — Corrección #310 + unificación visual global

## Prioridad 0: error React #310 en Análisis de Márgenes — CORREGIDO

- **Ruta**: `/analitica-v2/margenes`.
- **Causa raíz**: el `useState` de `exportingMode` estaba declarado después
  del retorno anticipado `if (companyId == null) return …`. Al resolverse la
  empresa, el segundo render ejecutaba un Hook más que el primero → React
  #310 «Rendered more hooks than during the previous render», al terminar la
  carga inicial. Sin relación con backend ni cálculos.
- **Corrección**: el Hook subió junto a los demás, antes de cualquier
  retorno (con comentario preventivo). Funcionalidad intacta: indicadores,
  filtros, tabla, políticas, precios objetivo, Excel y temas.
- **Regresión automática**: ESLint quedó configurado por primera vez en el
  proyecto (`frontend/eslint.config.mjs`: `react-hooks/rules-of-hooks` en
  `error` + parser TS + plugin Next; devDeps `eslint`,
  `eslint-plugin-react-hooks`, `typescript-eslint`,
  `@next/eslint-plugin-next`). La regla detecta el patrón original
  (comprobado con archivo sintético) y el lint de **todo el frontend** pasa
  con 0 errores: no hay otros #310 latentes en las 69 rutas.
- **Pendiente**: recorridos funcionales (filtros, empresa, Excel, temas)
  contra la app desplegada.

## Unificación visual aplicada a toda la ERP

1. **Migración de colores fijos a tokens** (232 reemplazos en 23 archivos +
   ajustes manuales): `bg-white → bg-card`, `bg-slate-50/100/200 →
   bg-muted(/40)`, `text-slate-400…950 → text-muted-foreground /
   text-foreground`, `border/divide-slate-* → border-border`, equivalentes
   zinc/gray. En claro el resultado visual es el mismo; en oscuro pasa a ser
   correcto.
2. **Protegidos sin tocar** (fijos intencionales): documento imprimible de
   OC (`oc-invoice.tsx`), papel y vista previa física del Generador de
   Etiquetas, visor de cámara de Góndolas (`bg-slate-950` de video; sí se
   migraron sus textos de estado inactivo), sidebar grafito, paneles oscuros
   sobre mapas y badges oscuros intencionales.
3. **Tema oscuro habilitado en todos los módulos del dashboard**:
   `DARK_THEME_ROUTE_PREFIXES` ahora lista los 18 segmentos reales
   (`/admin`, `/alerts`, `/analitica`, `/analitica-v2`, `/compras`,
   `/costos`, `/dashboard`, `/distribuidora`, `/gondolas`,
   `/inteligencia-comercial`, `/logistica`, `/margins`, `/operaciones`,
   `/orders`, `/politica-margenes`, `/products-without-cost`,
   `/promotions`, `/sucursales`). El botón luna/sol aparece en todo el
   dashboard y la preferencia persiste; los portales ya heredan vía `<html>`.
4. **Grupo A (Inteligencia Comercial)**: aviso de políticas de Márgenes a
   tokens; variantes `dark:` en acentos naranja/fucsia de Promociones y
   Dashboard que carecían de ellas.
5. **Sidebar**: las etiquetas ya no se truncan (pueden ocupar dos líneas) y
   cada enlace tiene `title`; en modo compacto se mantiene el Tooltip.
6. **PWA intacta**: manifest, íconos, service worker y página offline sin
   cambios.

## Estado por grupo (6.0)

| Grupo | Implementada visualmente | Validada escritorio | Validada tablet | Validada móvil | Validada claro | Validada oscuro |
|---|---|---|---|---|---|---|
| A. Inteligencia Comercial y Analítica | Sí (referencia + ajustes) | Pendiente | Pendiente | Pendiente | Pendiente | Pendiente |
| B. Sucursales (Góndolas, Etiquetas, Etiquetas Socios, Ofertas, Recepciones, Trazabilidad) | Sí (tokens + Dark) | Pendiente | Pendiente | Pendiente | Pendiente | Pendiente |
| C. Distribuidora (todas las rutas) | Sí (tokens + Dark; orders con renovación 5.2) | Pendiente | Pendiente | Pendiente | Pendiente | Pendiente |
| D. Compras y Logística | Sí (tokens + Dark) | Pendiente | Pendiente | Pendiente | Pendiente | Pendiente |
| E. Administración y Finanzas (admin, alerts, margins, orders, products-without-cost, política) | Sí (tokens + Dark) | Pendiente | Pendiente | Pendiente | Pendiente | Pendiente |
| F. Layout (login, selector de empresa grafito; sidebar/header/dashboard) | Sí | Pendiente | Pendiente | Pendiente | — (login grafito) | Pendiente |

«Implementada» significa: superficies/textos/bordes sobre tokens semánticos
y tema oscuro habilitado con portales sincronizados. **No** significa
validación visual: sin navegador ni dispositivos en este entorno, todas las
columnas de validación quedan pendientes y deben recorrerse tras el
despliegue autorizado (360/390/430/768/1024/1366/1440/1920 px, ambos temas).

## Continuación 6.0 (post-hotfix `5617dca`) — renovación por componentes

Publicado el hotfix de Márgenes, la renovación continuó por componentes,
no solo tokens. Estado diferenciado por ruta (T = tokens aplicados,
C = componentes/encabezado unificado, R = responsive reforzado,
O = tema oscuro habilitado con superficies tokenizadas,
V = validación visual — pendiente en todas por falta de navegador):

| Ruta | T | C | R | O | V | Nota |
|---|---|---|---|---|---|---|
| `/gondolas` | Sí | Sí (ya usaba `AnalyticPageHeader` y controles 44 px) | Sí | Sí | Pendiente | Escaneo/cámara intactos; textos de cámara migrados |
| `/gondolas/recorridos/[id]` | Sí | Sí (mismo sistema) | Sí | Sí | Pendiente | Sesiones y reposición intactos |
| `/sucursales/etiquetas` | Sí | Sí (encabezado Graphite + acciones 44 px) | Parcial | Sí | Pendiente | **Impresión/PDF/papel sin cambios** |
| `/sucursales/etiquetas2` | Sí | Sí (encabezado, sin padding duplicado, color fijo eliminado) | Parcial | Sí | Pendiente | Formatos físicos intactos |
| `/sucursales/ofertas` | Sí | Sí (encabezado + acciones 44 px) | Parcial | Sí | Pendiente | — |
| `/sucursales/recepciones` | Sí | Sí (encabezado + acción 44 px) | Parcial | Sí | Pendiente | Escaneo intacto |
| `/sucursales/trazabilidad` | Sí | Sí (encabezado + KPI con variantes dark) | Parcial | Sí | Pendiente | — |
| 42 páginas del dashboard (todos los grupos) | Sí | **Títulos unificados** a la escala Graphite 28/32 px `text-lq-ink` | — | Sí | Pendiente | Incluye Distribuidora, Compras, Logística, Operaciones, Analítica y Administración |
| Resto de rutas del inventario | Sí | Pendiente (encabezados propios V2 o de detalle ya coherentes) | Base | Sí | Pendiente | Se continúa por grupos |

«Responsive: Parcial» = la base compartida (sin scroll de página, controles
del encabezado a 44 px en móvil) está aplicada; la revisión fina de cada
tabla/formulario interno queda por ruta. No se marca ninguna ruta como
completada solo por tokens: la columna C exige encabezado/componentes
revisados, y la V sigue pendiente en todas.

## Corrección responsive iPhone + Stock en Márgenes (post-capturas del usuario)

Las capturas reales en iPhone mostraron el sidebar anclado y el contenido
comprimido. Diagnóstico verificado en código y en el HTML compilado:

- La meta viewport emitida es correcta
  (`width=device-width, initial-scale=1, viewport-fit=cover`).
- La fragilidad era estructural: el layout decidía «móvil o escritorio»
  con un hook JS (`useIsMobile`, estado inicial = escritorio). Si la
  hidratación tarda o la PWA instalada sirve un bundle anterior, el
  teléfono pinta el sidebar **en flujo** y comprime el contenido; no es un
  problema de píxeles físicos ni se resuelve con `overflow-x: hidden`.
- **Corrección**: el breakpoint estructural ahora es CSS — el sidebar en
  flujo solo existe ≥ 768 px (`hidden md:block`) y el drawer solo < 768 px
  (`md:hidden`). El estado solo controla apertura/cierre; con JS lento o
  ausente, un teléfono jamás muestra el sidebar anclado. Se suma lo ya
  publicado en `5751e80` (safe areas del drawer, Sheets y barra inferior).
- Pre‑despacho OC: sin cambios estructurales nuevos (ya usa este layout);
  el buscador aprovecha el ancho en pantallas XL.

**Stock Total en Análisis de Márgenes** (`/analitica-v2/margenes`):

- Columna «Stock Total» (tabla y tarjetas móviles) junto a costo/precio:
  `sum(quantity)` de `bsale_raw.stocks` por variante (misma definición del
  Catálogo Maestro; sin disponible/reservado ni recepciones históricas).
  El stock es de la variante y se repite en cada lista **sin duplicarse**.
- Estados: `con_stock` (> 0), `cero`, `negativo` (marcado en rojo) y
  `sin_sincronizar` («s/d», nunca cero).
- Filtro «Existencias» (Todos / Con stock / Sin stock 0 o negativo; el
  «s/d» queda fuera de ambos) aplicado **antes de paginar**: afecta total,
  tabla, paginación y los tres modos de Excel. Columna «Stock Total»
  agregada al Excel (vacío = sin sincronizar).
- Pruebas: servicio (multilista sin duplicar, cero, negativo, s/d, filtro
  inválido) y Excel reabierto; suite analytics_v2: 417 aprobadas. La
  validación contra los valores reales de producción sigue limitada por
  los permisos del rol de lectura (sin acceso a `bsale_raw.stocks` vía MCP).

## Graphite Premium 7.0 — Renovación integral (21-oct-2026)

Inventario real actualizado: **72 `page.tsx`** (69 de dashboard + `/login`,
`/company-selector`, `/offline`; la raíz `/` es redirección por sesión).
Sobre la base de Graphite 6.0 (tokens `--lq-*` en todo el frontend, dark
habilitado en los 18 segmentos del dashboard, H1 unificados 28/32 px),
la etapa 7.0 agrega:

- **Hotfix Pre-despacho OC (`/distribuidora/orders`)**: `DAY_BADGE_CLASS`
  redefinido con borde/fondo/texto explícitos en claro y oscuro (el texto
  `*-900` ya no queda sobre fondo `*-950`). Contraste calculado WCAG:
  8,7–14,4:1 en ambos temas para los 7 días; «Sin día» neutro con tokens.
  Badges de peso/estado y textos de la tabla subidos de 8–10 px a 11 px.
- **Legibilidad ERP completa**: 104 textos de 8/9/10 px elevados a 11 px
  en 47 archivos (UI operativa). Se conservan solo en la vista física de
  etiquetas (tamaño impreso) y en el documento imprimible de OC.
- **Contraste dark de acentos**: 140 variantes `dark:text-*-300`
  agregadas a textos semánticos 600/700/800 sin variante (41 archivos),
  con resguardo de paneles claros intencionales.
- Panel «Precio Socio» de Etiquetas Socios con variante dark; selector
  de empresa con la escala tipográfica Graphite.
- Hotfixes iOS publicados (`62167e3`, `7982c9c`) integrados y no
  revertidos: breakpoint `lg`, barra `black`, safe areas, SW y manifest
  con revalidación, filtro de Existencias en Catálogo y Márgenes.

### Estado por grupo (7.0)

| Grupo | Rutas | Tokens | Dark | Responsive | Estado |
|---|---|---|---|---|---|
| Acceso (`/login`, `/company-selector`, `/offline`, `/`) | 4 | Sí | N/A (grafito fijo) | Sí | Implementada sin validación visual |
| Inteligencia Comercial (V2, catálogo, costos legacy, márgenes legacy, política, sin costo, promociones legacy, alerts, dashboard, admin) | 15 | Sí | Sí | Sí | Implementada sin validación visual |
| Sucursales (góndolas + recorridos, etiquetas, etiquetas2, ofertas, recepciones, trazabilidad) | 7 | Sí | Sí (papel de etiqueta fijo) | Sí | Implementada sin validación visual |
| Distribuidora (dashboard, orders, ordenes-compra, pre-despacho-oc, planificaciones + detalle + mapa, planificación, planning, pre-planificación, pendientes, sin-georef, rutero, mapa, resumen-vendedor, vendedores, clientes + análisis + inactivos, cuadraturas + detalle, dispatch-analysis, maestro-logístico) | 23 | Sí | Sí (tiles de mapa claros) | Sí | Implementada sin validación visual |
| Compras (generar-oc, productos, proveedores, registros-oc) | 4 | Sí | Sí (documento OC imprimible fijo) | Sí | Implementada sin validación visual |
| Logística (cargas + nueva + detalle + certificar + mapa, peso-ordenes, auditoria-pesos, mapa-entregas) | 8 | Sí | Sí | Sí | Implementada sin validación visual |
| Operaciones (hub, dashboard, vendedores + detalle, mapa, georreferencias, incidencias) | 7 | Sí | Sí (tiles de mapa claros) | Sí | Implementada sin validación visual |
| Analítica legacy (comercial-vendedores, notas-credito, orders + detalle) | 4 | Sí | Sí | Sí | Implementada sin validación visual |
| **Total** | **72** | | | | |

Ninguna ruta se marca «Validada»: no hay navegador en el entorno.
Pendientes de validación visual/manual por ruta y tema, y de revisión
fina: gráficos Recharts y mapas en oscuro, modales/drawers por pantalla,
y el recorrido completo en iPhone real (PWA reinstalada).

## Qué NO incluye esta etapa (honestidad de alcance)

- No se reescribieron los encabezados de las 69 rutas al componente
  `AnalyticPageHeader`: la unificación se hizo sobre la capa de tokens y
  temas, que era la causa de la mezcla visual. La adopción de componentes
  compartidos por pantalla sigue el patrón documentado en 5.2 y puede
  continuar módulo a módulo.
- Mapas (tiles claros de Leaflet sobre UI oscura), gráficos Recharts con
  colores propios y el input del drawer de pesos de Pre‑despacho (40 px)
  requieren revisión visual específica.
- Verificación: TypeScript 62 líneas (línea base, 0 nuevos), ESLint de
  Hooks 0 errores, build correcto.
