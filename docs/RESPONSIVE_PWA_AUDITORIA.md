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
| `/distribuidora/orders`, `/distribuidora/pendientes` | Distribuidora | Base compartida | Media | B |
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
