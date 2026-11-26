# Maestro logístico / Fotografías — paso 04

## Alcance
Rama aislada, sin cambio de producción ni escritura en catálogo. ERP pagina en servidor (50 por defecto, 25/50/100), aborta solicitudes obsoletas, busca nombre/SKU/barcode/variant_id y edita por products_master.id. Se mantiene el default legado de otras pantallas. Cajas por pallet y auditoría logística requieren migración separada, fuera de esta primera entrega.

Repositorio inspeccionado: Dominus09/cat-quillotana, main, public/products/{barcode}.webp; 1568 WebP al momento de la auditoría. El catálogo usa el campo image de la API, derivado de bsale.catalog_view.image_url. Los archivos son estáticos de build. No asumir que el commit refresca el sitio ni que image_url siempre coincide con el asset. No cambiar el catálogo público, precios, stock o mapeo SQL en este PR. Verificar la relación catalog_view y URL real para una muestra, incluida una variante sin foto. Si image_url falta o apunta a otro recurso, acordar una corrección separada antes de activar.

## Configuración segura
- CATALOG_PHOTO_WRITES_ENABLED=0 por defecto.
- Token GitHub fine-grained limitado al repositorio cat-quillotana con Contents read/write. Guardar exclusivamente como secreto de backend (Coolify). Nunca en frontend, git, logs o .env público.
- CATALOG_PHOTO_PREVIEW_SECRET: aleatorio, mínimo 32 caracteres, compartido entre workers. La firma liga usuario, producto, barcode, bytes, SHA previo y vencimiento de 15 min.
- HEIC se soporta con pillow-heif.
- IA opcional: construir imagen con ENABLE_CATALOG_PHOTO_AI=1; esto instala rembg[cpu] y descarga modelo u2netp durante build. Activar CATALOG_PHOTO_AI_ENABLED=1 solo tras verificar CPU/RAM y recorte real. Por defecto IA desactivada. Marco blanco no quita fondo; recorte automático sí. Nunca confirmar una imagen sin revisión visual.
- Endpoints protegidos por require_management_access (ERP/gerencia). El servidor obtiene barcode por ID y exige variante única empresa 3 / catalog_view. Si hay duplicados, no escribe.
- GitHub Contents PUT con SHA esperado para reemplazo; commits conservan usuario ERP numérico, ID producto, SHA previo, hash de imagen y SHA nuevo verificable en respuesta. No hay escritura ni caché persistente de original en preview. No se suben fotos reales durante QA.
- Confirmación requiere los mismos bytes de WebP firmado. Si timeout o conflicto, consultar estado antes de reintentar. No repetir automáticamente.
- Restaurar foto: recuperar el archivo de un commit anterior, preparar y publicar bajo el flujo autorizado; no revertir commits de otros productos.

## QA pendiente
1. pytest backend/tests/test_product_photo_service.py y pruebas HTTP con dependencias mock: 401/403, duplicado, escritura OFF, archivo inválido, confirmación expirada/manipulada, SHA concurrente, éxito 200/201, timeout y fallo de GitHub.
2. Frontend: build/typecheck contra base; 25/50/100, página y búsqueda sin descargar universo, edición por ID, vista a 390px y escritorio, foco del diálogo.
3. Preview JPG, PNG, HEIC, botellas transparentes y packs; sin llamadas PUT.
4. Con repo/branch de pruebas aislado o mocked GitHub, verificar nueva foto y reemplazo; no probar escrituras sobre catálogo real sin autorización.
5. Activación controlada: verificar URL de ERP test.quillotana.cl, token, flags, publicar solo SKU aprobado, verificar commit, despliegue del catálogo y foto de la variante en cat.quillotana.cl. No migraciones, no deploy ni activación de flags por este PR.
