# Análisis técnico del frontend ERP

## 1. Alcance del análisis

Este análisis cubre **solamente el frontend** del ERP.

- **Incluido:** `frontend/` (rutas App Router, páginas, componentes, hooks, contextos, estilos, config, Docker frontend, cliente API).
- **Excluido:** `backend/`, `docs/backend/`, SQL, modelos, migraciones y base de datos.
- **Referencia backend mínima permitida:** solo para nombrar endpoints consumidos por frontend (sin auditar lógica backend).
- **Requiere verificación manual:** uso real en producción de pantallas legacy (`/distribuidora/pendientes`, `/distribuidora/sin-georef`), protección por roles finos y assets huérfanos.

## 2. Resumen ejecutivo

- El frontend es **Next.js App Router + React 19 + TypeScript** (`frontend/package.json`, `frontend/app/`).
- Tiene cobertura funcional amplia en módulos: dashboard/analítica, compras, pedidos, promociones y un dominio fuerte de distribuidora.
- Hay base reusable sólida en `components/ui/` (shadcn + Radix), layout y utilidades.
- El consumo API está centralizado principalmente en `frontend/lib/api.ts`, con guardas de sesión por `localStorage` y `Authorization: Bearer`.
- Partes con madurez menor: módulo sucursales (varias pantallas sin consumo API explícito en revisión estática), toasts no montados y artefactos duplicados.
- Riesgos principales:
  - auth/permisos por rol no uniformemente aplicados en cliente (token sí, rol fino requiere verificación),
  - dependencia de `localStorage` sin capa robusta de sesión/refresh token,
  - código legacy/deprecado mantenido por compatibilidad.
- Oportunidad de limpieza inmediata: hooks duplicados, logs debug, componentes no montados, archivo CSS alterno no referenciado.

## 3. Arquitectura actual del frontend

- **Framework:** Next.js App Router (`frontend/app/**/page.tsx`, `frontend/app/**/layout.tsx`).
- **Inicio/build:**
  - `npm run dev` => `next dev`
  - `npm run build` => `node scripts/run-build.cjs`
  - `npm start` => `node .next/standalone/server.js`
  - fuente: `frontend/package.json`.
- **Rutas:** por filesystem en `frontend/app/`.
- **Páginas/módulos:** agrupadas en `frontend/app/(dashboard)/...`.
- **Componentes reutilizables:** `frontend/components/ui/*`, `frontend/components/layout/*`, `frontend/components/distribuidora/*`.
- **Consumo APIs:** central en `frontend/lib/api.ts`, secundario `frontend/services/orders.ts`.
- **Estado global:** contexto `DistribuidoraPlanningProvider` (`frontend/context/distribuidora-planning-selection.tsx`); no se detecta Redux/Zustand.
- **Estilos:** Tailwind v4 + variables CSS en `frontend/app/globals.css`; UI con shadcn/Radix.
- **Formularios/validaciones:** mayormente manuales (`useState`, checks, `required`); `react-hook-form` instalado y wrappers en `components/ui/form.tsx`, uso real requiere verificación.
- **Errores/loading:** manejo por página (`isLoading`, `error` states).
- **Autenticación frontend:** `localStorage` token + guard en `frontend/app/(dashboard)/layout.tsx`; login en `frontend/app/login/page.tsx`.

| Carpeta/Archivo | Rol dentro del frontend | Observación |
|---|---|---|
| `frontend/app/` | Rutas/páginas/layouts | App Router |
| `frontend/app/(dashboard)/layout.tsx` | Guard auth + shell principal | redirige a `/login` o `/company-selector` |
| `frontend/components/ui/` | Componentes base UI | shadcn/Radix |
| `frontend/components/layout/` | Header/Sidebar | navegación por módulos |
| `frontend/components/distribuidora/` | Componentes dominio distribuidora | módulo de mayor complejidad |
| `frontend/lib/api.ts` | Cliente API principal | 69 llamadas fetch |
| `frontend/lib/api-base.ts` | resolución base URL/proxy | usa `/api-upstream` en browser |
| `frontend/services/orders.ts` | cliente pedidos aislado | 3 llamadas fetch |
| `frontend/context/distribuidora-planning-selection.tsx` | estado compartido planning | provider activo |
| `frontend/app/globals.css` | estilos globales y tokens | incluye estilos leaflet |

## 4. Tecnologías y herramientas utilizadas

| Herramienta | Tipo | Detectada en | Uso aparente | Estado | Observación |
|---|---|---|---|---|---|
| Next.js 16 | framework | `frontend/package.json`, `frontend/app/` | app web ERP | En uso | App Router |
| React 19 | UI | `frontend/package.json` | componentes | En uso | — |
| TypeScript | lenguaje | `frontend/tsconfig.json`, dependencias | tipado estático | En uso | `strict: true` |
| Tailwind CSS v4 | estilos | `frontend/app/globals.css`, `postcss.config.mjs` | sistema de estilos | En uso | + `tw-animate-css` |
| shadcn/ui + Radix | UI primitives | `frontend/components/ui/*`, `components.json` | inputs/dialog/table/sidebar | En uso | amplio |
| lucide-react | iconografía | múltiples imports | iconos UI | En uso | — |
| react-leaflet/leaflet | mapas | `frontend/components/distribuidora/*`, deps | mapa rutero | En uso | estilos en globals |
| dnd-kit | drag & drop | deps + módulos distribuidora | ordenamiento | En uso aparente | requiere verificación puntual por pantalla |
| react-hook-form | formularios | deps + `components/ui/form.tsx` | wrappers de formulario | Parcial | uso directo en páginas no evidente |
| zod/@hookform/resolvers | validación | deps | validación schema | Requiere verificación | sin uso directo detectado |
| jsPDF/xlsx/html2canvas | export/impresión | deps + utilidades/lib | reportes/export | En uso aparente | revisar cobertura real |
| Docker standalone | deploy | `frontend/Dockerfile` | imagen producción | En uso | multi-stage |

## 5. Inventario completo de rutas frontend

Inventario detallado en: `docs/frontend/RUTAS_FRONTEND.md`.

Resumen:
- **33 rutas/pantallas** detectadas en `frontend/app/**/page.tsx`.
- Rutas principales: acceso (`/`, `/login`, `/company-selector`), dashboard/analítica, compras, pedidos, distribuidora, sucursales.
- Rutas legacy con redirect: `/distribuidora/pendientes`, `/distribuidora/sin-georef`.

| Módulo | Ruta frontend | Archivo | Componente/Página | Qué hace | Datos que muestra | Acciones disponibles | Endpoints consumidos | Auth/Permisos | Estado |
|---|---|---|---|---|---|---|---|---|---|
| Ver `RUTAS_FRONTEND.md` | Ver `RUTAS_FRONTEND.md` | `frontend/app/**/page.tsx` | páginas App Router | navegación ERP | KPIs, OCs, clientes, rutas, pedidos | filtros, sync, CRUD parcial, export | ver inventario por ruta | guard token en layout; permisos finos requiere verificación | mayormente activas |

## 6. Inventario de componentes frontend

| Tipo | Componente/Archivo | Función | Usado por | Props/Datos | Consume API | Estado | Observación |
|---|---|---|---|---|---|---|---|
| Layout | `frontend/components/layout/sidebar.tsx` | navegación lateral modular | `app/(dashboard)/layout.tsx` | secciones/nav items | No | En uso | define módulos activos y disabled |
| Layout | `frontend/components/layout/header.tsx` | cabecera y acciones de sesión | `app/(dashboard)/layout.tsx` | toggles/sidebar | No | En uso | — |
| Provider | `frontend/context/distribuidora-planning-selection.tsx` | estado IDs para planning | `distribuidora/layout`, `distribuidora/planning` | Set<number> | No | En uso | contexto global módulo |
| Dominio | `frontend/components/distribuidora/mapa-rutero-client.tsx` | mapa interactivo rutero | `distribuidora/mapa/page.tsx` | rutas, clientes, base | Sí (via lib/api) | En uso | componente crítico |
| Dominio | `frontend/components/distribuidora/rutero-vista-client.tsx` | vista rutero | `distribuidora/rutero/page.tsx` | filtros/filas | Sí | En uso | — |
| Dominio | `frontend/components/distribuidora/resumen-vendedor-client.tsx` | resumen semanal vendedor | `distribuidora/resumen-vendedor/page.tsx` | vendedor/días | Sí | En uso | export/visual |
| UI base | `frontend/components/ui/*` (69 archivos) | primitives UI | transversal | props Radix/shadcn | No directo | En uso | algunos candidatos a limpieza |
| Hook | `frontend/hooks/use-mobile.ts` | breakpoint mobile | `components/ui/sidebar.tsx` | boolean | No | En uso | duplicado con `components/ui/use-mobile.tsx` |
| Hook | `frontend/hooks/use-toast.ts` | API toast local | `distribuidora/orders/page.tsx`, `components/ui/toaster.tsx` | toast state | No | En uso parcial | toaster no montado |

## 7. Consumo de APIs y endpoints desde frontend

Inventario detallado en: `docs/frontend/APIS_CONSUMIDAS_FRONTEND.md`.

| Archivo | Función/Hook | Método | Endpoint/URL | Datos enviados | Datos recibidos esperados | Usado en | Manejo de error/loading | Auth/Token | Estado |
|---|---|---|---|---|---|---|---|---|---|
| `frontend/lib/api.ts` | múltiples (`login`, `getSuppliers`, `getDistribuidora...`) | GET/POST/PATCH/PUT/DELETE | `/login`, `/suppliers`, `/purchase-*`, `/distribuidora/*`, etc. | query/body según caso | JSON + blobs (excel) | páginas dashboard/compras/distribuidora | manejo de `!res.ok`, errores por función; loading en página | usa Bearer por `localStorage` | Activo |
| `frontend/services/orders.ts` | `getOrders`, `getOrderById`, `updateOrderStatus` | GET/PUT | `/orders`, `/orders/{id}`, `/orders/{id}/status` | query/body | JSON pedidos | rutas `/orders*` | errores sí, loading en página | token no explícito (requiere verificación) | Activo |
| `frontend/lib/resumen-vendedor-pdf-route-canvas.ts` | fetch externo | GET | URL externa (tile/img) | — | binario/canvas | resumen vendedor PDF | requiere verificación | no aplica | Activo |

## 8. Formularios y validaciones

| Formulario | Archivo/Ruta | Campos | Validaciones | Endpoint destino | Manejo de errores | Estado |
|---|---|---|---|---|---|---|
| Login | `frontend/app/login/page.tsx` (`/login`) | email, password | `required`, type email, error string | `POST /login` | mensaje en tarjeta + loading | Completo |
| Selector empresa | `frontend/app/company-selector/page.tsx` | selección de empresa | requiere token y list disponible | `GET /companies` | estado loading/error | Completo |
| Proveedor | `frontend/app/(dashboard)/compras/proveedores/page.tsx` | name, contact, phone, email, notes | validación manual `name` obligatorio + normalización | `POST/PATCH /suppliers*` | mensajes en UI | Completo |
| Generar OC | `frontend/app/(dashboard)/compras/generar-oc/page.tsx` | empresa, sucursal, proveedor, líneas | checks numéricos/manuales (`Number.isFinite`, >0, etc.) | `POST /purchase-orders/generate-from-lines` | feedback y controles loading | Completo |
| Promociones | `frontend/app/(dashboard)/promotions/page.tsx` | tipo/canal/fechas/items/companies | reglas manuales de fecha/items/canal/valor | `POST /promotions` | errores de backend parseados | Completo |
| Rutero/observaciones | `frontend/components/distribuidora/rutero-vista-client.tsx` | observación/tipo atención/sábado | validaciones de interacción | endpoints rutero | mensajes/estado requiere verificación | Requiere verificación |

## 9. Estado, contexto y datos globales

| Estado/Store/Contexto | Archivo | Qué guarda | Quién lo usa | Persistencia | Observación |
|---|---|---|---|---|---|
| Sesión auth | `frontend/lib/api.ts`, `frontend/app/login/page.tsx` | `token`, `email`, `role`, `company_id`, `company_name` | layout dashboard + páginas | `localStorage` | sin refresh token |
| Demo mode | `frontend/lib/api.ts` | `demo_mode` | funciones de fallback | `sessionStorage` | fallback por error de red |
| Planning context | `frontend/context/distribuidora-planning-selection.tsx` | set de `document_id` seleccionados | planning distribuidora | memoria React | estado compartido módulo |
| Estado UI local | múltiples páginas | `isLoading`, `error`, filtros | cada pantalla | local | no hay store global |

## 10. Estilos, UI y diseño

| Elemento UI/Estilo | Archivo | Uso | Estado | Observación |
|---|---|---|---|---|
| Estilos globales + tokens | `frontend/app/globals.css` | base, tema, leaflet | En uso | archivo principal |
| Tailwind + PostCSS | `frontend/postcss.config.mjs` | pipeline CSS | En uso | con `@tailwindcss/postcss` |
| Sistema UI shadcn | `frontend/components/ui/*`, `frontend/components.json` | componentes reutilizables | En uso | cobertura amplia |
| Layout app | `frontend/app/(dashboard)/layout.tsx` | shell responsive | En uso | sidebar/header |
| Tema dark/light | `globals.css` + `next-themes` dep | variables `.dark` | Parcial | `ThemeProvider` no montado (requiere verificación) |

## 11. Variables de entorno y configuración frontend

| Variable/Config | Archivo | Uso | Obligatoria | Observación |
|---|---|---|---|---|
| `NEXT_PUBLIC_API_URL` | `frontend/lib/api-base.ts`, `frontend/next.config.mjs` | base URL backend/proxy target | Recomendada | fallback a `https://api.quillotana.cl` |
| `NEXT_PUBLIC_API_NO_PROXY` | `frontend/lib/api-base.ts` | desactiva `/api-upstream` | No | sólo browser |
| `API_PROXY_TARGET` | `frontend/next.config.mjs` | rewrite server-side | No | prioriza sobre `NEXT_PUBLIC_API_URL` |
| `NODE_OPTIONS` | `frontend/Dockerfile` | memoria build | No | optimiza build en entornos limitados |
| `PORT` | `frontend/Dockerfile` | puerto runtime | Sí (deploy) | default 3000 |

No se detectaron archivos `.env` dentro de `frontend/` en esta revisión.

## 12. Seguridad frontend

| Riesgo | Archivo/Ruta | Severidad | Evidencia | Recomendación |
|---|---|---|---|---|
| Token en localStorage | `frontend/lib/api.ts`, `frontend/login/page.tsx` | Media | `localStorage.setItem("token"...` | evaluar httpOnly cookie o estrategia híbrida |
| Control de permisos por rol no evidente | `frontend/app/(dashboard)/layout.tsx` | Media | guard por token/empresa, no por rol fino | agregar guard por rol/capability en rutas críticas |
| Servicio orders sin header auth explícito | `frontend/services/orders.ts` | Media | fetch sin `Authorization` | verificar si backend requiere token y unificar cliente |
| Dependencia de URLs externas de imágenes | login/sidebar/home | Baja-Media | `img src` a blob URL externa | mover assets críticos a `public/` |
| Legacy endpoints aún disponibles en cliente | `frontend/lib/api.ts` (`@deprecated`) | Baja | funciones pendientes/sin-georef | retirar tras confirmar no uso |

## 13. Código duplicado, muerto o limpiable

| Tipo | Archivo/Carpeta | Motivo | Evidencia | Riesgo | Verificación recomendada |
|---|---|---|---|---|---|
| Hook duplicado | `frontend/components/ui/use-mobile.tsx` | duplicado de `frontend/hooks/use-mobile.ts` | sin imports al path ui | Bajo | grep final antes de borrar |
| Hook duplicado | `frontend/components/ui/use-toast.ts` | duplicado de `frontend/hooks/use-toast.ts` | sin imports al path ui | Bajo | idem |
| Componente no montado | `frontend/components/ui/toaster.tsx` | no se monta en layout | sin imports en app/layout | Medio | confirmar UX de toasts |
| Componente no montado | `frontend/components/ui/sonner.tsx` | no se monta | sin imports | Medio | confirmar roadmap |
| Provider no usado | `frontend/components/theme-provider.tsx` | sin import | búsqueda sin matches | Bajo | confirmar dark mode futuro |
| CSS alterno | `frontend/styles/globals.css` | posible huérfano | no import detectado | Bajo | revisar build scripts |
| Logs debug | archivos distribuidora indicados | ruido en producción | `console.log` detectados | Bajo | remover/feature flag |

## 14. Problemas e inconsistencias detectadas

| Problema | Archivo/Ruta | Impacto | Prioridad | Recomendación |
|---|---|---|---|---|
| Consumo API no totalmente unificado | `lib/api.ts` vs `services/orders.ts` | mantenimiento | Media | consolidar en un solo cliente |
| Módulos sucursales con integración incierta | `app/(dashboard)/sucursales/*` | funcional | Media | confirmar alcance real y backend asociado |
| Dependencias instaladas sin uso evidente | `package.json` (`zod`, resolvers, etc.) | bundle/maintenance | Baja-Media | auditar imports efectivos |
| Toaster sin montaje | UI global | UX | Media | montar o retirar |
| Roles no aplicados en guard | rutas dashboard | seguridad funcional | Media-Alta | introducir control por rol |

## 15. Recomendaciones de ordenamiento frontend

| Etapa | Acción | Archivos involucrados | Prioridad | Riesgo | Resultado esperado |
|---|---|---|---|---|---|
| 1 | Documentar lo existente | `docs/frontend/*` | Alta | Bajo | baseline compartido |
| 2 | Completar inventario de rutas y pantallas | `app/**/page.tsx`, `sidebar.tsx` | Alta | Bajo | mapa funcional claro |
| 3 | Ordenar consumo de APIs | `lib/api.ts`, `services/orders.ts` | Alta | Medio | cliente unificado |
| 4 | Limpiar código muerto bajo riesgo | hooks duplicados, logs, toaster | Media | Bajo | deuda técnica menor |
| 5 | Unificar componentes reutilizables | `components/ui/*`, layout | Media | Medio | coherencia UI |
| 6 | Mejorar validaciones y manejo errores | formularios compras/promos/login | Alta | Medio | UX más robusta |
| 7 | Revisar seguridad frontend y permisos | layout/auth/api client | Alta | Medio-Alto | menor exposición |
| 8 | Mejorar estilos/responsividad | globals + páginas críticas | Media | Medio | experiencia homogénea |
| 9 | Agregar pruebas mínimas | rutas críticas + smoke UI | Alta | Bajo-Medio | seguridad de cambios |

## 16. Plan de limpieza propuesto

### Limpieza segura

- Duplicados de hooks (`use-mobile`, `use-toast` en carpeta `components/ui`).
- Logs debug en páginas distribuidora.

### Limpieza con revisión

- Toaster/Sonner no montados.
- `ThemeProvider` no usado.
- `styles/globals.css` alterno no referenciado.
- módulos sucursales con integración incierta.

### No tocar todavía

- `frontend/lib/api.ts` (núcleo de contratos).
- `frontend/app/(dashboard)/layout.tsx` (guard base).
- `frontend/components/distribuidora/*` (dominio crítico).
- proxy/config API (`next.config.mjs`, `lib/api-base.ts`).

## 17. Próximos pasos recomendados

1. Validar primero **auth y permisos por rol** en frontend (qué rutas deben restringirse).
2. Documentar primero contratos de APIs activas (`APIS_CONSUMIDAS_FRONTEND.md`) y rutas legacy.
3. Limpiar primero duplicados/lows risk (hooks + logs + componentes no montados confirmados).
4. No tocar aún módulos distribuidora críticos ni cliente API central sin pruebas.
5. Para limpieza segura real: agregar smoke tests y checklist de regresión por módulo.
