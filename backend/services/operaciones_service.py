"""
Monitoreo operacional vendedores / rutas (solo lectura).

Fuente: ``bsale.vendedores_app``, ``bsale.rutas_dia``, ``bsale.visitas`` (app móvil).
No modifica contratos ``/app_distribuidora``.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

from backend.db import get_connection
from backend.services.operaciones_visitas import (
    ESTADO_INCIDENCIA,
    es_visita_realizada,
    sql_in_estados_realizados,
)
from backend.services.visita_foto_service import resolve_foto_display
from backend.schemas.operaciones import (
    GpsActual,
    IncidenciaRow,
    IncidenciasListResponse,
    MarcadorMapa,
    OperacionesDashboardKpis,
    OperacionesDashboardResponse,
    OperacionesMetricasResponse,
    RutaMapaResponse,
    VendedorDetalleResponse,
    VendedorOperacionesRow,
    VendedorUbicacionMapa,
    VendedoresListResponse,
    VisitaTimelineItem,
)

logger = logging.getLogger(__name__)

_OFFLINE_MINUTES = int(os.getenv("OPERACIONES_OFFLINE_MINUTES", "15"))
_ATRASADO_PCT = float(os.getenv("OPERACIONES_ATRASADO_PCT", "50"))


def _offline_threshold() -> timedelta:
    return timedelta(minutes=max(1, _OFFLINE_MINUTES))


def _estado_conexion(
    *,
    activo: bool,
    updated_at: datetime | None,
    porcentaje: float,
    pending_sync: int,
    tiene_ruta: bool,
) -> str:
    if not activo or not tiene_ruta:
        return "offline"
    now = datetime.now(timezone.utc)
    if updated_at is not None:
        if updated_at.tzinfo is None:
            updated_at = updated_at.replace(tzinfo=timezone.utc)
        if now - updated_at > _offline_threshold():
            return "offline"
    else:
        return "offline"
    if pending_sync > 0 or (porcentaje < _ATRASADO_PCT and porcentaje < 100):
        return "atrasado"
    return "activo"


def _row_to_vendedor(row: tuple, nombres: dict[str, str]) -> VendedorOperacionesRow:
    (
        codigo,
        nombre,
        activo,
        ruta_id,
        estado_ruta,
        total_cli,
        visitados,
        pendientes,
        pct,
        hora_inicio,
        ruta_updated,
        incidencias,
        pending_sync,
        km,
        gps_lat,
        gps_lon,
        gps_at,
    ) = row
    cod = str(codigo)
    pct_f = float(pct or 0)
    pending = int(pending_sync or 0)
    tiene_ruta = ruta_id is not None
    estado = _estado_conexion(
        activo=bool(activo),
        updated_at=ruta_updated,
        porcentaje=pct_f,
        pending_sync=pending,
        tiene_ruta=tiene_ruta,
    )
    gps = None
    if gps_lat is not None and gps_lon is not None:
        gps = GpsActual(
            lat=float(gps_lat),
            lon=float(gps_lon),
            updated_at=gps_at,
        )
    return VendedorOperacionesRow(
        codigo=cod,
        nombre=str(nombre or nombres.get(cod, cod)),
        activo=bool(activo),
        ruta_id=int(ruta_id) if ruta_id is not None else None,
        estado_ruta=str(estado_ruta) if estado_ruta else None,
        estado_conexion=estado,  # type: ignore[arg-type]
        visitas_realizadas=int(visitados or 0),
        visitas_pendientes=int(pendientes or 0),
        incidencias=int(incidencias or 0),
        porcentaje_avance=round(pct_f, 2),
        ultima_sync=ruta_updated,
        pending_sync_count=pending,
        bateria_pct=None,
        gps=gps,
        kilometros_recorridos=round(float(km or 0) / 1000.0, 2),
    )


_SQL_ESTADOS_REALIZADOS = sql_in_estados_realizados()

_SQL_VENDEDORES_BASE = f"""
SELECT
    va.codigo,
    va.nombre,
    va.activo,
    rd.id AS ruta_id,
    rd.estado AS estado_ruta,
    COALESCE(vst.total_cli, rd.total_clientes, 0),
    COALESCE(vst.visitados, 0),
    GREATEST(
        0,
        COALESCE(vst.total_cli, rd.total_clientes, 0) - COALESCE(vst.visitados, 0)
    ),
    CASE
        WHEN COALESCE(vst.total_cli, rd.total_clientes, 0) > 0
        THEN ROUND(
            100.0 * COALESCE(vst.visitados, 0)
                / COALESCE(vst.total_cli, rd.total_clientes, 0)::numeric,
            2
        )
        ELSE 0
    END,
    rd.hora_inicio,
    rd.updated_at AS ruta_updated,
    COALESCE(vst.incidencias, 0) AS incidencias,
    COALESCE(ps.cnt, 0) AS pending_sync,
    COALESCE(km.sum_m, 0) AS km_metros,
    gps.lat AS gps_lat,
    gps.lon AS gps_lon,
    gps.fecha_hora_visita AS gps_at
FROM bsale.vendedores_app va
LEFT JOIN bsale.rutas_dia rd
    ON rd.vendedor = va.codigo AND rd.fecha = %s
LEFT JOIN LATERAL (
    SELECT
        COUNT(*)::int AS total_cli,
        COUNT(*) FILTER (WHERE v.estado IN ({_SQL_ESTADOS_REALIZADOS}))::int AS visitados,
        COUNT(*) FILTER (WHERE v.estado = '{ESTADO_INCIDENCIA}')::int AS incidencias
    FROM bsale.visitas v
    WHERE v.ruta_id = rd.id
) vst ON rd.id IS NOT NULL
LEFT JOIN LATERAL (
    SELECT COUNT(*)::int AS cnt
    FROM bsale.visitas v
    WHERE v.ruta_id = rd.id AND v.sync_status = 'pending_sync'
) ps ON rd.id IS NOT NULL
LEFT JOIN LATERAL (
    SELECT COALESCE(SUM(v.distancia_metros), 0) AS sum_m
    FROM bsale.visitas v
    WHERE v.ruta_id = rd.id
      AND v.estado IN ({_SQL_ESTADOS_REALIZADOS})
      AND v.distancia_metros IS NOT NULL
) km ON rd.id IS NOT NULL
LEFT JOIN LATERAL (
    SELECT v.lat_visita AS lat, v.lon_visita AS lon, v.fecha_hora_visita
    FROM bsale.visitas v
    WHERE v.ruta_id = rd.id
      AND v.lat_visita IS NOT NULL
      AND v.lon_visita IS NOT NULL
    ORDER BY v.fecha_hora_visita DESC NULLS LAST, v.updated_at DESC
    LIMIT 1
) gps ON rd.id IS NOT NULL
WHERE va.tipo_usuario = 'vendedor'
ORDER BY va.nombre NULLS LAST, va.codigo
"""


def _fetch_vendedores_rows(cur, fecha: date) -> list[VendedorOperacionesRow]:
    cur.execute(_SQL_VENDEDORES_BASE, (fecha,))
    rows = cur.fetchall()
    items = [_row_to_vendedor(r, {}) for r in rows]
    if logger.isEnabledFor(logging.DEBUG):
        visitados = sum(i.visitas_realizadas for i in items)
        inc = sum(i.incidencias for i in items)
        pend = sum(i.visitas_pendientes for i in items)
        logger.debug(
            "operaciones vendedores fecha=%s filas=%s visitas_realizadas=%s incidencias=%s pendientes=%s",
            fecha,
            len(items),
            visitados,
            inc,
            pend,
        )
    return items


def _aggregate_kpis(items: list[VendedorOperacionesRow], fecha: date) -> OperacionesDashboardKpis:
    total_cli = sum((i.visitas_realizadas + i.visitas_pendientes) for i in items)
    visitados = sum(i.visitas_realizadas for i in items)
    pendientes = sum(i.visitas_pendientes for i in items)
    inc = sum(i.incidencias for i in items)
    activos = sum(1 for i in items if i.estado_conexion == "activo")
    pending = sum(i.pending_sync_count for i in items)
    km = sum(i.kilometros_recorridos for i in items)
    ultima = max((i.ultima_sync for i in items if i.ultima_sync), default=None)
    pct = round(100.0 * visitados / total_cli, 2) if total_cli else 0.0
    return OperacionesDashboardKpis(
        fecha=fecha,
        total_clientes=total_cli,
        clientes_visitados=visitados,
        clientes_pendientes=pendientes,
        incidencias=inc,
        vendedores_activos=activos,
        vendedores_total=len(items),
        porcentaje_cumplimiento=pct,
        visitas_pending_sync=pending,
        ultima_sincronizacion=ultima,
        kilometros_recorridos=round(km, 2),
    )


def get_dashboard(fecha: date | None = None) -> OperacionesDashboardResponse:
    f = fecha or date.today()
    t0 = time.perf_counter()
    conn = get_connection()
    try:
        cur = conn.cursor()
        items = _fetch_vendedores_rows(cur, f)
        kpis = _aggregate_kpis(items, f)
        cur.close()
    finally:
        conn.close()
    logger.info(
        "operaciones dashboard fecha=%s vendedores=%s visitados=%s incidencias=%s pendientes=%s ms=%.0f",
        f,
        len(items),
        kpis.clientes_visitados,
        kpis.incidencias,
        kpis.clientes_pendientes,
        (time.perf_counter() - t0) * 1000,
    )
    return OperacionesDashboardResponse(kpis=kpis, vendedores_resumen=items)


def get_vendedores(fecha: date | None = None) -> VendedoresListResponse:
    f = fecha or date.today()
    conn = get_connection()
    try:
        cur = conn.cursor()
        items = _fetch_vendedores_rows(cur, f)
        cur.close()
    finally:
        conn.close()
    return VendedoresListResponse(fecha=f, items=items)


def get_vendedor_detalle(codigo: str, fecha: date | None = None) -> VendedorDetalleResponse | None:
    f = fecha or date.today()
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT codigo, nombre FROM bsale.vendedores_app
            WHERE codigo = %s AND tipo_usuario = 'vendedor'
            LIMIT 1
            """,
            (codigo,),
        )
        va = cur.fetchone()
        if not va:
            return None

        cur.execute(
            """
            SELECT id, estado, hora_inicio, hora_fin, porcentaje_cumplimiento, updated_at
            FROM bsale.rutas_dia WHERE vendedor = %s AND fecha = %s LIMIT 1
            """,
            (codigo, f),
        )
        rd = cur.fetchone()
        ruta_id = int(rd[0]) if rd else None

        timeline: list[VisitaTimelineItem] = []
        incidencias: list[VisitaTimelineItem] = []
        km = 0.0
        estado_conexion = "offline"
        ultima_sync = None
        pct = 0.0

        if ruta_id is not None:
            cur.execute(
                """
                SELECT
                    id, cliente_id, nombre_fantasia, direccion, comuna, orden_ruta,
                    estado, tipo_incidencia, observacion, foto_url, fecha_hora_visita,
                    lat_visita, lon_visita, distancia_metros, sync_status
                FROM bsale.visitas
                WHERE ruta_id = %s
                ORDER BY orden_ruta, id
                """,
                (ruta_id,),
            )
            pending = 0
            for row in cur.fetchall():
                vid = int(row[0])
                foto_raw = row[9]
                foto_disp, _ = resolve_foto_display(foto_raw, vid)
                item = VisitaTimelineItem(
                    id=vid,
                    cliente_id=str(row[1]),
                    nombre_fantasia=row[2],
                    direccion=row[3],
                    comuna=row[4],
                    orden_ruta=int(row[5]),
                    estado=str(row[6]),
                    tipo_incidencia=row[7],
                    observacion=row[8],
                    foto_url=foto_disp,
                    fecha_hora_visita=row[10],
                    lat_visita=float(row[11]) if row[11] is not None else None,
                    lon_visita=float(row[12]) if row[12] is not None else None,
                    distancia_metros=float(row[13]) if row[13] is not None else None,
                    sync_status=str(row[14]),
                )
                timeline.append(item)
                if item.estado == ESTADO_INCIDENCIA:
                    incidencias.append(item)
                if item.distancia_metros and es_visita_realizada(item.estado):
                    km += float(item.distancia_metros)
                if item.sync_status == "pending_sync":
                    pending += 1

            ultima_sync = rd[5]
            total_vis = len(timeline)
            visitados = sum(1 for t in timeline if es_visita_realizada(t.estado))
            pend = max(0, total_vis - visitados)
            pct = round(100.0 * visitados / total_vis, 2) if total_vis else 0.0
            estado_conexion = _estado_conexion(
                activo=True,
                updated_at=ultima_sync,
                porcentaje=pct,
                pending_sync=pending,
                tiene_ruta=True,
            )

        cur.close()
    finally:
        conn.close()

    return VendedorDetalleResponse(
        codigo=str(va[0]),
        nombre=str(va[1]),
        fecha=f,
        ruta_id=ruta_id,
        estado_ruta=str(rd[1]) if rd else None,
        hora_inicio=rd[2] if rd else None,
        hora_fin=rd[3] if rd else None,
        porcentaje_cumplimiento=round(pct, 2),
        kilometros_recorridos=round(km / 1000.0, 2),
        estado_conexion=estado_conexion,  # type: ignore[arg-type]
        ultima_sync=ultima_sync,
        timeline=timeline,
        incidencias=incidencias,
    )


def get_ruta_mapa(ruta_id: int) -> RutaMapaResponse | None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT rd.id, rd.fecha, rd.vendedor, va.nombre
            FROM bsale.rutas_dia rd
            LEFT JOIN bsale.vendedores_app va ON va.codigo = rd.vendedor
            WHERE rd.id = %s
            """,
            (ruta_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        rid, fecha, vendedor, nombre = int(row[0]), row[1], str(row[2]), row[3]

        cur.execute(
            """
            SELECT id, cliente_id, nombre_fantasia,
                   COALESCE(lat_visita, lat_cliente),
                   COALESCE(lon_visita, lon_cliente),
                   estado, tipo_incidencia
            FROM bsale.visitas
            WHERE ruta_id = %s
              AND COALESCE(lat_visita, lat_cliente) IS NOT NULL
              AND COALESCE(lon_visita, lon_cliente) IS NOT NULL
            ORDER BY orden_ruta
            """,
            (ruta_id,),
        )
        marcadores: list[MarcadorMapa] = []
        for r in cur.fetchall():
            est = str(r[5])
            if est == ESTADO_INCIDENCIA:
                map_est = "incidencia"
            elif es_visita_realizada(est):
                map_est = "visitado"
            else:
                map_est = "pendiente"
            marcadores.append(
                MarcadorMapa(
                    visita_id=int(r[0]),
                    cliente_id=str(r[1]),
                    nombre_fantasia=r[2],
                    lat=float(r[3]),
                    lon=float(r[4]),
                    estado=map_est,  # type: ignore[arg-type]
                    vendedor=vendedor,
                    tipo_incidencia=r[6],
                ),
            )

        cur.execute(
            """
            SELECT lat_visita, lon_visita, fecha_hora_visita
            FROM bsale.visitas
            WHERE ruta_id = %s AND lat_visita IS NOT NULL AND lon_visita IS NOT NULL
            ORDER BY fecha_hora_visita DESC NULLS LAST
            LIMIT 1
            """,
            (ruta_id,),
        )
        gps = cur.fetchone()
        vendedor_ubicacion = None
        if gps:
            items = _fetch_vendedores_rows(cur, fecha if isinstance(fecha, date) else fecha.date())  # type: ignore[union-attr]
            row_v = next((i for i in items if i.codigo == vendedor), None)
            vendedor_ubicacion = VendedorUbicacionMapa(
                codigo=vendedor,
                nombre=str(nombre or vendedor),
                lat=float(gps[0]),
                lon=float(gps[1]),
                estado_conexion=row_v.estado_conexion if row_v else "offline",
                updated_at=gps[2],
            )
        cur.close()
    finally:
        conn.close()

    return RutaMapaResponse(
        fecha=fecha if isinstance(fecha, date) else fecha.date(),  # type: ignore[union-attr]
        ruta_id=rid,
        vendedor=vendedor,
        vendedor_nombre=nombre,
        marcadores=marcadores,
        vendedor_ubicacion=vendedor_ubicacion,
    )


def get_incidencias(
    fecha: date | None = None,
    vendedor: str | None = None,
    limit: int = 200,
) -> IncidenciasListResponse:
    f = fecha or date.today()
    lim = max(1, min(limit, 500))
    conn = get_connection()
    try:
        cur = conn.cursor()
        params: list[Any] = [f]
        v_filter = ""
        if vendedor:
            v_filter = " AND rd.vendedor = %s"
            params.append(vendedor.strip())
        params.append(lim)
        cur.execute(
            f"""
            SELECT
                v.id, v.ruta_id, rd.vendedor, va.nombre,
                v.cliente_id, v.nombre_fantasia, v.comuna,
                v.tipo_incidencia, v.observacion, v.foto_url,
                v.fecha_hora_visita, v.sync_status
            FROM bsale.visitas v
            INNER JOIN bsale.rutas_dia rd ON rd.id = v.ruta_id
            LEFT JOIN bsale.vendedores_app va ON va.codigo = rd.vendedor
            WHERE rd.fecha = %s AND v.estado = 'incidencia'
            {v_filter}
            ORDER BY v.fecha_hora_visita DESC NULLS LAST, v.id DESC
            LIMIT %s
            """,
            tuple(params),
        )
        items: list[IncidenciaRow] = []
        fotos_ok = 0
        fotos_miss = 0
        for r in cur.fetchall():
            vid = int(r[0])
            foto_raw = r[9]
            foto_disp, tiene = resolve_foto_display(foto_raw, vid)
            if tiene:
                fotos_ok += 1
            else:
                fotos_miss += 1
            items.append(
                IncidenciaRow(
                    id=vid,
                    ruta_id=int(r[1]),
                    vendedor=str(r[2]),
                    vendedor_nombre=r[3],
                    cliente_id=str(r[4]),
                    nombre_fantasia=r[5],
                    comuna=r[6],
                    tipo_incidencia=r[7],
                    observacion=r[8],
                    foto_url=foto_disp,
                    tiene_foto=tiene,
                    fecha_hora_visita=r[10],
                    sync_status=str(r[11]),
                ),
            )
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "operaciones incidencias fecha=%s total=%s fotos_ok=%s fotos_sin=%s",
                f,
                len(items),
                fotos_ok,
                fotos_miss,
            )
        cur.close()
    finally:
        conn.close()
    return IncidenciasListResponse(fecha=f, total=len(items), items=items)


def get_metricas(fecha: date | None = None) -> OperacionesMetricasResponse:
    f = fecha or date.today()
    dash = get_dashboard(f)
    return OperacionesMetricasResponse(
        fecha=f,
        dashboard=dash.kpis,
        por_vendedor=dash.vendedores_resumen,
    )
