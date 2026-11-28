# Planificador de almacenamiento — MVP vacaciones

- Ruta: /inteligencia-bodega/planificador, solo gerencia ERP.
- Levantamiento agregado por variante Bsale y origen. Escáner externo (Enter),
  cámara (luego Buscar), búsqueda y carga pegada código;completos;molidos;origen.
  Mismo producto/origen reemplaza conteo; otro origen consolida.
- Molido cuenta como pallet físico, no fracción. Pallet mixto con varias variantes:
  no duplicar el pallet por SKU; levantar aparte hasta implementar composición mixta.
- Dos niveles simulados, uno por defecto; directo solo completos de misma variante,
  rack proyectado permite distintos SKU; no certifica seguridad.
- Asignación manual por selección de posiciones y bloque. Sugerencia por volumen
  en sector seleccionado (IZ desde fondo; BL por fila), no motor FEFO.
- No modifica pallets, stock Bsale, ni plano existente BC.
- Persistencia server-side por escenario, optimistic lock, copia y PDF A3
  con plano por bodega/nivel, destinos detallados y pendientes.
- PDF es destino objetivo, no secuencia de movimientos observados. Verificar fechas
  y accesibilidad; FEFO/FIFO durante ejecución. No hay drag/drop ni consumo.
- BL: 17.6 x 6 m físicos; 5.5 m útiles; 4 módulos, 12 filas, 5 profundidades.
  58 posiciones provisionales, dos no utilizables (espacio muerto frontal y
  mueble pendiente retiro). F3/F4 gap modelado ~0.55 m vs ~0.70 reportado;
  los tramos y medidas de pallet no calzan exactamente. Banda de sensores
  frontal 0.5 m y postes son hipótesis; confirmar portón y accesos. No publicar
  ni autorizar niveles hasta revisar en terreno.

## QA / entrega
Usar worktree aislado. Ejecutar unit tests planner, integración en BD VACÍA
local warehouse_pallets_test_planner_<sufijo> (variable WAREHOUSE_PLANNER_TEST_DSN),
regresión warehouse y frontend tests, ESLint, TS y Next build.
Probar en móvil cámara/lector y carga rápida, guardar y recargar, colisión de
versiones, conteo repetido, asignar directo/rack, impedir BAE, exportar PDF
con ambas bodegas y niveles y revisar legibilidad visual. Confirmar estado
actual de main y ausencia de modificaciones a registros observados.
No merge, deploy o SQL real sin aprobación expresa.
Precheck SELECT ONLY 010_011; 010 y 011 manuales tras aprobación, API primero
y ERP después. No auto-migraciones.
