# Limpieza propuesta del frontend ERP

## Importante

Este documento no elimina ni modifica código. Solo identifica candidatos de limpieza para revisión manual.

## Limpieza segura

| Tipo | Archivo/Carpeta | Motivo | Evidencia | Riesgo | Verificación antes de eliminar |
|---|---|---|---|---|---|
| Hook duplicado | `frontend/components/ui/use-mobile.tsx` | Duplica `frontend/hooks/use-mobile.ts` | Imports detectados solo a `@/hooks/use-mobile` | Bajo | Buscar imports dinámicos/alias antes de borrar |
| Hook duplicado | `frontend/components/ui/use-toast.ts` | Duplica `frontend/hooks/use-toast.ts` | Imports detectados solo a `@/hooks/use-toast` | Bajo | Confirmar que no lo exporta librería externa |
| Logs debug | `frontend/app/(dashboard)/distribuidora/orders/page.tsx` | `console.log` de carga de órdenes | búsqueda `console.log` | Bajo | Retirar logs o condicionar a `NODE_ENV !== 'production'` |
| Logs debug | `frontend/app/(dashboard)/distribuidora/pre-planificacion/page.tsx` | `console.log` de carga | búsqueda `console.log` | Bajo | Igual que arriba |
| Logs debug | `frontend/components/distribuidora/mapa-rutero-client.tsx` | `console.log("SIM ORDEN...")` | búsqueda `console.log` | Bajo | Verificar que no sea diagnóstico activo en operación |

## Limpieza con revisión manual

| Tipo | Archivo/Carpeta | Motivo | Evidencia | Riesgo | Verificación antes de eliminar |
|---|---|---|---|---|---|
| Componente no montado | `frontend/components/ui/toaster.tsx` | No hay import de montaje en layout | búsqueda de imports sin resultados | Medio | Confirmar UX de toasts antes de remover |
| Componente no montado | `frontend/components/ui/sonner.tsx` | Igual que toaster | búsqueda de imports sin resultados | Medio | Validar si planificado para próximas vistas |
| Provider no usado | `frontend/components/theme-provider.tsx` | Definido pero no montado | búsqueda de imports sin resultados | Bajo | Validar roadmap de dark mode con `next-themes` |
| Estilo alterno | `frontend/styles/globals.css` | Posible duplicado de `app/globals.css` | `app/layout.tsx` usa `app/globals.css` | Bajo | Confirmar que no se inyecta por build custom |
| Dependencia posiblemente no usada | `frontend/package.json` (`@hookform/resolvers`, `zod`) | No aparecen usos directos en páginas | búsqueda estática sin `useForm` real | Medio | Verificar imports indirectos y ramas no cargadas |
| Módulo UI sin API clara | `frontend/app/(dashboard)/sucursales/*` | Pantallas parecen estáticas/locales | revisión rápida sin consumo de `lib/api.ts` | Medio | Verificar backlog/feature flags antes de tocar |
| API legacy en cliente | `frontend/lib/api.ts` funciones `@deprecated` | Mantiene endpoints viejos | comentarios `@deprecated` | Medio | Confirmar que no haya links externos/usuarios marcadores |
| Asset potencialmente huérfano | `frontend/public/placeholder*` e íconos no referenciados | Sin referencias textuales | búsqueda por nombre | Medio | Revisar uso implícito por Next metadata/manifiestos |

## No tocar todavía

| Tipo | Archivo/Carpeta | Motivo | Riesgo |
|---|---|---|---|
| Cliente API principal | `frontend/lib/api.ts` | Punto central de contratos frontend-backend | Alto |
| Layout/guards dashboard | `frontend/app/(dashboard)/layout.tsx` | Control de acceso base | Alto |
| Módulos Distribuidora | `frontend/app/(dashboard)/distribuidora/**`, `frontend/components/distribuidora/**` | Flujo crítico y complejo de operación | Alto |
| Config proxy API | `frontend/next.config.mjs`, `frontend/lib/api-base.ts` | Afecta CORS y producción | Alto |
