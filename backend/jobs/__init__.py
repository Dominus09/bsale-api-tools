"""Jobs de producción (sync) y shims de compatibilidad hacia ``backend.debug``.

Ejecución típica (desde la raíz del repo):

- ``python -m backend.jobs.sync_bsale_distribuidora``
- ``python -m backend.jobs.sync_distribuidora_related``
- ``python -m backend.jobs.sync_rutero``

Herramientas de depuración viven en ``backend.debug``; los módulos
``backend.jobs.debug_*`` y ``export_oc_*`` son shims que reenvían allí.
"""
