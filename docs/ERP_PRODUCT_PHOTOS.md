# Maestro logístico / Fotografías — paso 04

## Alcance
Rama aislada, sin cambio de producción ni escritura en catálogo. ERP pagina en servidor (50 por defecto, 25/50/100), aborta solicitudes obsoletas, busca nombre/SKU/barcode/variant_id y edita por products_master.id. Se mantiene el default legado de otras pantallas. Cajas por pallet opcional, cobertura separada, cálculo de unidades y peso de carga, y auditoría privada de fotos/logística incluidos bajo migración aditiva propuesta 070. Los endpoints nuevos son exclusivos de gerencia y no cambian contratos legacy.

Repositorio inspeccionado: Dominus09/cat-quillotana, main, public/products/{barcode}.webp; 1568 WebP al momento de la auditoría. El catálogo usa el campo image de la API, derivado de bsale.catalog_view.image_url. Los archivos son estáticos de build. No asumir que el commit refresca el sitio ni que image_url siempre coincide con el asset. No cambiar el catálogo público, precios, stock o mapeo SQL en este PR. Verificar la relación catalog_view y URL real para una muestra, incluida una variante sin foto. Si image_url falta o apunta a otro recurso, acordar una corrección separada antes de activar.

## Configuración segura
- CATALOG_PHOTO_WRITES_ENABLED=0 por defecto.
- Token GitHub fine-grained limitado al repositorio cat-quillotana con Contents read/write. Guardar exclusivamente como secreto de backend (Coolify). Nunca en frontend, git, logs o .env público.
- CATALOG_PHOTO_PREVIEW_SECRET: aleatorio, mínimo 32 caracteres, compartido entre workers. La firma liga usuario, producto, barcode, bytes, SHA previo y vencimiento de 15 min.
- HEIC se soporta con pillow-heif.
- IA opcional: construir imagen con ENABLE_CATALOG_PHOTO_AI=1; esto instala rembg[cpu] y descarga modelo u2netp durante build. Activar CATALOG_PHOTO_AI_ENABLED=1 solo tras verificar CPU/RAM y recorte real. Por defecto IA desactivada. Marco blanco no quita fondo; recorte automático sí. Nunca confirmar una imagen sin revisión visual.
- Endpoints protegidos por require_management_access (ERP/gerencia). El servidor obtiene barcode por ID y exige variante única empresa 3 / catalog_view. Si hay duplicados, no escribe.
- GitHub Contents PUT con SHA esperado para reemplazo. Auditoría privada en app.catalog_photo_audit: actor ERP, producto, variante, código, SHA anterior, SHA256 preparado, SHA nuevo, commit y fechas. El commit público solo contiene barcode y referencia aleatoria. La fila started se confirma antes de cualquier PUT; si el resultado es incierto queda para reconciliar. No hay caché persistente del original en preview. No se suben fotos reales durante QA.
- Migración 070 propuesta (NO ejecutada): columna boxes_per_pallet y dos tablas de auditoría. PRODUCT_MASTER_PALLET_WRITES_ENABLED=0 por defecto. No ejecutar SQL sin backup, confirmar destino analytics y autorización. Ambas funciones detectan esquema ausente y no habilitan escritura. La columna admite NULL explícito, sin imputar valores. No afecta definición de completitud, unidades por caja ni peso por caja. El peso calculado excluye tara del pallet.
- Confirmación requiere los mismos bytes de WebP firmado. Si timeout o conflicto, consultar estado antes de reintentar. No repetir automáticamente.
- Restaurar foto: recuperar el archivo de un commit anterior, preparar y publicar bajo el flujo autorizado; no revertir commits de otros productos.

## QA pendiente
1. pytest backend/tests/test_product_photo_service.py y pruebas HTTP con dependencias mock: 401/403, duplicado, escritura OFF, archivo inválido, confirmación expirada/manipulada, SHA concurrente, éxito 200/201, timeout y fallo de GitHub.
2. Frontend: build/typecheck contra base; 25/50/100, página y búsqueda sin descargar universo, edición por ID, vista a 390px y escritorio, foco del diálogo.
3. Preview JPG, PNG, HEIC, botellas transparentes y packs; sin llamadas PUT.
4. Con repo/branch de pruebas aislado o mocked GitHub, verificar nueva foto y reemplazo; no probar escrituras sobre catálogo real sin autorización.
5. Activación controlada: verificar URL de ERP test.quillotana.cl, token, flags, publicar solo SKU aprobado, verificar commit, despliegue del catálogo y foto de la variante en cat.quillotana.cl. No ejecutar migraciones, no deploy ni activación de flags por este PR. Si un commit se confirmó pero falla la actualización de auditoría, buscar referencia audit_id en GitHub y reconciliar la fila started; no repetir publicación.

## Catalog companion PR
The companion cat-quillotana branch `feat/catalog-verified-local-photos` builds a verified filename/content-version manifest. Enable `NEXT_PUBLIC_CATALOG_LOCAL_PHOTOS_ENABLED=1` only after catalog QA; confirm it is deployed before enabling ERP publication. ERP blocks an API image pointing to an unrelated file; no implicit override. Missing/known placeholder images may use a newly deployed local file. The catalog and ERP switches remain OFF by default.
