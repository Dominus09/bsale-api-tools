# Quillotana Operativa — entrada general y selector explícito
Fecha: 2026-11-27.

- Login: Quillotana Operativa / Gestión integral de operaciones.
- Después de verificar acceso PWA y perfiles, toda sesión entra a /modulos.
- Nunca auto-redirigir a Choferes solo porque sea el único módulo operativo.
- Selector muestra únicamente perfiles autorizados por auth/me; metadatos locales solo ante error de conexión, no 401/403.
- Choferes: operativo; Vendedores: en preparación; Bodega y Supervisión: próximamente. Las páginas informativas no activan flujos ni permisos.
- Menú Cambiar módulo y enlace de marca conservados. Seleccionar Choferes mantiene /despachos y sus protecciones.
- Mantener manifest id y start_url /despachos, service worker, IndexedDB, outbox, sesiones y backend intactos. El arranque frío de la PWA instalada es una ruta offline ya validada; no migrar el launcher hasta comprobar caché del selector en iOS. La entrada web / y el login sí van al selector.
- QA: ejecutar npm run test:map y npm run build en apps/pwa, revisar login, selección con uno o varios perfiles, cambiar módulo desde Choferes y retorno a despacho preparado; iPhone online y modo avión sin generar entregas ficticias. No borrar datos locales. No SQL, flags ni redeploy automático.
