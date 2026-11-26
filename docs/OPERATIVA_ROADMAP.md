# Operativa / ERP — hoja de ruta maestra

Fecha de actualización: 2026-11-26. Estados distinguen código, QA, merge y producción. No confundir una rama publicada con un despliegue.

## 01 — ORS (resuelto)
- Migración al dominio HeiGIT, PR #8; consulta autenticada HTTP 200 y operador confirma funcionamiento.
- Mantener contingencia ORS en un ticket separado.

## 02 — Choferes 2.1 (merge)
- PR #9: outbox, lease, idempotencia, protección de fotos, Mis entregas.
- QA reportado 175/175, build OK, dos E2E lab (driver-flow con ajuste local). Confirmar redeploy y smoke en dispositivos reales.

## 03 — Cierre visual y QA Choferes
- Inicio/jornada, resumen, clientes, mapa, ficha, productos, cobranza, cierre y evidencias: identidad grafito/burdeos, iPhone-first, controles táctiles y estados reales.
- No modificar contratos de entrega, cantidades, evidencia, grants, métodos de pago ni flags.
- El caso de entrega parcial sin conexión no está cubierto por el alcance Offline V2 existente. Mantenerlo explícito; otro ticket si se requiere.
- Criterio de cierre: build, unitarias, E2E lab reproducibles, QA visual y smoke de producción. Comparar TypeScript vs base y resolver deuda. No borrar registros locales.

## 04 — Maestro logístico de productos + imágenes (siguiente prioridad)
- Revisar la pantalla ERP /distribuidora/maestro-logistico y la versión realmente desplegada. Código actual ya tiene paginación de servidor, con tamaño 500; usuario informa lentitud/carga de ~6000 productos. Objetivo: 50 por defecto, selector 25/50/100, búsqueda/filtros en servidor, cancelación de solicitudes obsoletas, métricas de cobertura separadas y conservar edición inline. No descargar todo el universo para paginar en cliente.
- Agregar cajas por pallet, entero positivo opcional por variante/SKU, NULL si no verificado. Mantener units_per_box y peso caja independientes; calcular unidades y peso de carga por pallet solo con datos válidos, sin incluir tara ficticia. Auditar cambios. Migración aditiva previa revisión/QA y autorización.
- Administración de fotos en ERP: buscar por nombre, SKU, variant_id o barcode; previsualizar, subir/reemplazar, estado sin foto. Validar MIME/decodificación, tamaño, convertir a WebP. Mantener convención existente {barcode}.webp y confirmar repo, rama y ruta reales del catálogo antes de escritura. Resolver barcodes duplicados explícitamente. Token GitHub solo en backend, permisos mínimos, autorización gerencia, auditoría, control de SHA para reemplazo, no exponer credenciales. Verificar cómo cat.quillotana.cl sirve imágenes (estáticas en build vs URL remota) y definir refresco/publicación sin alterar fotos existentes. No migrar a R2 sin otra decisión. No inventar URL, repo ni mapping.
- QA: paginación real y velocidad, guardado por ID/variante, cajas por pallet y foto visible en catálogo; rollback y permisos.

## 05 — Catálogo Unificado V2
- Código integrado; verificar CATALOG_SOURCE_PUBLIC y despliegue. Consultas puntuales a Bsale para stock disponible oficina 1 y precio lista 13. Validar UPB Sol x18: distinguir pack vendible de equivalencia logística. Verificar imágenes asociadas. Activación reversible, solo tras aprobación.

## 06 — Clientes y georreferenciación
- Mantener sync_clients y OC operativos; verificar tareas, última ejecución y datos de clientes. Preservar coordenadas.

## 07 — Ventas V2 / CRON comerciales
- Boletas, facturas, NC sobre RAW. Auditar documentos, idempotencia, empresas y fechas antes de activar. No interferir con OC o clientes. Horarios por definir.

## 08 — Products Master V2
- Identidad/precio/stock desde RAW; mantener overlay logístico manual por variant_id. No sustituir picking/peso sin QA y decisión separada.

## 09 — PWA Vendedores 2.0
- Menú ya integrado. Jornada, ruta, visitas, recuperación, historial cliente, catálogo separado de ofertas y hasta 20 SKU foco semanales de gerencia.

## 10 — Contingencia ORS
- Rama publicada, no merge; actualizar respecto de main, pruebas y revisión de pesos/costos antes de aprobar.

## 11 — Inteligencia de Bodega
- Desarrollo independiente. ERP real: test.quillotana.cl; work.quillotana.cl no se usa. Verificar destino efectivo antes de SQL.

## Reglas de entrega
- GitHub por ramas, QA local en worktree, PR aprobado y merge, redeploy Coolify manual por operador.
- No SQL, flags, escritura productiva, credenciales, limpieza de outbox ni despliegue sin autorización. 
