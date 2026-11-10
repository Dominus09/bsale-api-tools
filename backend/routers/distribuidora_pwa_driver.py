"""PWA operativa — despachos del chofer autenticado.

No exige app ERP. Ownership estricto por ``dispatch_plan.driver_user_id``.
P2: lectura. P3: propuesta GPS + foto fachada (sin aprobar).
P4: visitas de entrega (inicio / reanudación / cierre).
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile
from pydantic import BaseModel, Field

from backend.auth.multiapp import require_pwa_profiles
from backend.services.distribuidora import client_location_service as loc_svc
from backend.services.distribuidora import delivery_visit_service as visit_svc
from backend.services.distribuidora import dispatch_plan_driver_service as driver_svc
from backend.services.distribuidora import dispatch_plan_pwa_driver_read_service as read_svc
from backend.utils.json_safe import serialize_value

router = APIRouter(
    prefix="/distribuidora/pwa/driver",
    tags=["PWA chofer"],
    dependencies=[Depends(require_pwa_profiles("chofer"))],
)


def _uid(user: dict) -> int:
    raw = user.get("id") if user.get("id") is not None else user.get("uid")
    try:
        return int(raw)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=401, detail="Usuario sin id") from exc


@router.get("/despachos")
def list_my_despachos(
    limit: int = Query(50, ge=1, le=200),
    user: dict = Depends(require_pwa_profiles("chofer")),
):
    """Mis despachos: solo filas con ``driver_user_id`` = usuario JWT + SPA vigente."""
    uid = _uid(user)
    items = driver_svc.list_plans_for_driver_user(uid, limit=limit)
    visible: list[dict] = []
    for row in items:
        try:
            read_svc.enforce_driver_plan_read_access(user, int(row["id"]))
            visible.append(row)
        except HTTPException as exc:
            if exc.status_code in (403, 503):
                continue
            if exc.status_code == 404:
                continue
            raise
    return serialize_value(
        {
            "items": visible,
            "driver_user_id": uid,
            "count": len(visible),
        }
    )


@router.get("/despachos/{plan_id}")
def get_my_despacho(
    plan_id: int,
    user: dict = Depends(require_pwa_profiles("chofer")),
):
    """Ficha read-only. Plan ajeno / oficina revocada → 404/403."""
    uid = read_svc.enforce_driver_plan_read_access(user, int(plan_id))
    row = driver_svc.get_plan_summary_for_driver(uid, int(plan_id))
    if row is None:
        raise HTTPException(status_code=404, detail="Despacho no encontrado")
    return serialize_value(
        {
            "plan": row,
            "driver_user_id": uid,
            "read_only": True,
            "nav": {
                "mapa": True,
                "clientes": True,
                "ficha": True,
                "note_es": (
                    "Mapa, clientes y ficha. Puede iniciar visitas de entrega "
                    "(P4 online)."
                ),
            },
            "p4": visit_svc.schema_status(),
            "future_contracts": read_svc.FUTURE_CONTRACTS,
        }
    )


@router.get("/despachos/{plan_id}/clientes")
def list_my_clientes(
    plan_id: int,
    q: str | None = Query(None, description="Buscar por cliente o folio"),
    user: dict = Depends(require_pwa_profiles("chofer")),
):
    read_svc.enforce_driver_plan_read_access(user, int(plan_id))
    return read_svc.list_clients_for_driver(int(plan_id), q=q)


@router.get("/despachos/{plan_id}/clientes/{customer_key:path}")
def get_my_cliente(
    plan_id: int,
    customer_key: str,
    user: dict = Depends(require_pwa_profiles("chofer")),
):
    read_svc.enforce_driver_plan_read_access(user, int(plan_id))
    return read_svc.get_client_ficha_for_driver(int(plan_id), customer_key)


@router.get("/despachos/{plan_id}/mapa")
def get_my_mapa(
    plan_id: int,
    origin_lat: float | None = Query(None),
    origin_lng: float | None = Query(None),
    user: dict = Depends(require_pwa_profiles("chofer")),
):
    read_svc.enforce_driver_plan_read_access(user, int(plan_id))
    lat, lng = read_svc.validate_origin_coords(origin_lat, origin_lng)
    return read_svc.get_map_for_driver(
        int(plan_id),
        origin_lat=lat,
        origin_lng=lng,
    )


@router.get("/futuro/contratos")
def get_future_contracts(
    user: dict = Depends(require_pwa_profiles("chofer")),
):
    _uid(user)
    return read_svc.get_future_contracts()


class LocationProposalBody(BaseModel):
    latitude: float
    longitude: float
    accuracy_m: float | None = None
    captured_at: str | None = None
    confirm_low_accuracy: bool = False
    confirm_significant_move: bool = False
    facade_photo_id: int | None = Field(None, ge=1)
    idempotency_key: str | None = Field(None, max_length=120)


class FacadePrepareBody(BaseModel):
    content_type: str = "image/jpeg"
    idempotency_key: str | None = Field(None, max_length=120)


class FacadeConfirmBody(BaseModel):
    photo_id: int = Field(..., ge=1)


@router.get("/p3/status")
def p3_status(user: dict = Depends(require_pwa_profiles("chofer"))):
    _uid(user)
    return loc_svc.schema_status()


@router.get("/p4/status")
def p4_status(user: dict = Depends(require_pwa_profiles("chofer"))):
    _uid(user)
    return visit_svc.schema_status()


class StartVisitBody(BaseModel):
    latitude: float | None = None
    longitude: float | None = None
    accuracy_m: float | None = None
    gps_captured_at: str | None = None
    gps_source: str | None = Field(None, max_length=40)
    gps_exception_code: str | None = Field(None, max_length=64)
    gps_exception_note: str | None = Field(None, max_length=500)
    idempotency_key: str | None = Field(None, max_length=120)


class FinalizeVisitBody(BaseModel):
    expected_version: int = Field(..., ge=1)
    status: str = Field(..., max_length=32)
    result_reason: str | None = Field(None, max_length=64)
    result_note: str | None = Field(None, max_length=1000)
    latitude: float | None = None
    longitude: float | None = None
    accuracy_m: float | None = None
    gps_captured_at: str | None = None
    gps_source: str | None = Field(None, max_length=40)
    gps_exception_code: str | None = Field(None, max_length=64)
    gps_exception_note: str | None = Field(None, max_length=500)


@router.post("/despachos/{plan_id}/clientes/{customer_key:path}/visitas/start")
def start_delivery_visit(
    plan_id: int,
    customer_key: str,
    body: StartVisitBody,
    user: dict = Depends(require_pwa_profiles("chofer")),
):
    """Inicia o reanuda visita de entrega. No duplica visita activa."""
    return visit_svc.start_visit(
        user=user,
        plan_id=int(plan_id),
        customer_key=customer_key,
        latitude=body.latitude,
        longitude=body.longitude,
        accuracy_m=body.accuracy_m,
        gps_captured_at=body.gps_captured_at,
        gps_source=body.gps_source,
        gps_exception_code=body.gps_exception_code,
        gps_exception_note=body.gps_exception_note,
        idempotency_key=body.idempotency_key,
    )


@router.get("/despachos/{plan_id}/visitas/{visit_id}")
def get_delivery_visit(
    plan_id: int,
    visit_id: int,
    user: dict = Depends(require_pwa_profiles("chofer")),
):
    return visit_svc.get_visit_detail(
        user=user, plan_id=int(plan_id), visit_id=int(visit_id)
    )


@router.post("/despachos/{plan_id}/visitas/{visit_id}/finalize")
def finalize_delivery_visit(
    plan_id: int,
    visit_id: int,
    body: FinalizeVisitBody,
    user: dict = Depends(require_pwa_profiles("chofer")),
):
    return visit_svc.finalize_visit(
        user=user,
        plan_id=int(plan_id),
        visit_id=int(visit_id),
        expected_version=int(body.expected_version),
        status=body.status,
        result_reason=body.result_reason,
        result_note=body.result_note,
        latitude=body.latitude,
        longitude=body.longitude,
        accuracy_m=body.accuracy_m,
        gps_captured_at=body.gps_captured_at,
        gps_source=body.gps_source,
        gps_exception_code=body.gps_exception_code,
        gps_exception_note=body.gps_exception_note,
    )


class DeliveryLineBody(BaseModel):
    source_detail_id: int = Field(..., ge=1)
    line_number: int | None = Field(None, ge=0)
    variant_id: int | None = None
    sku_code: str | None = Field(None, max_length=64)
    product_name: str | None = Field(None, max_length=240)
    planned_units: float = Field(..., ge=0)
    delivered_units: float = Field(..., ge=0)
    undelivered_reason: str | None = Field(None, max_length=64)
    undelivered_note: str | None = Field(None, max_length=500)


class SaveDocumentLinesBody(BaseModel):
    related_document_id: int = Field(..., ge=1)
    oc_document_id: int | None = Field(None, ge=1)
    document_number: int | None = None
    document_type: str | None = Field(None, max_length=64)
    document_total: float | None = None
    result_status: str = Field("partial", max_length=32)
    notes: str | None = Field(None, max_length=1000)
    lines: list[DeliveryLineBody]
    # Opcional: PWA antigua no lo envía; P4.1+ detecta conflictos concurrentes.
    expected_version: int | None = Field(None, ge=1)


class DeclareCollectionBody(BaseModel):
    document_result_id: int | None = Field(None, ge=1)
    method: str = Field(..., max_length=32)
    document_amount: float | None = None
    declared_amount: float | None = None
    note: str | None = Field(None, max_length=500)


class WhatsAppReportBody(BaseModel):
    client_name: str | None = Field(None, max_length=200)
    planning_code: str | None = Field(None, max_length=64)
    driver_name: str | None = Field(None, max_length=120)


@router.get("/reportes-whatsapp/demo")
def list_demo_whatsapp_reports(
    user: dict = Depends(require_pwa_profiles("chofer")),
):
    """Escenarios ficticios para prueba familiar. Sin escritura productiva."""
    return visit_svc.list_demo_whatsapp_reports(user=user)


@router.post("/despachos/{plan_id}/visitas/{visit_id}/documentos")
def save_visit_document_lines(
    plan_id: int,
    visit_id: int,
    body: SaveDocumentLinesBody,
    user: dict = Depends(require_pwa_profiles("chofer")),
):
    return visit_svc.save_document_lines(
        user=user,
        plan_id=int(plan_id),
        visit_id=int(visit_id),
        related_document_id=int(body.related_document_id),
        oc_document_id=body.oc_document_id,
        document_number=body.document_number,
        document_type=body.document_type,
        document_total=body.document_total,
        result_status=body.result_status,
        notes=body.notes,
        lines=[ln.model_dump() for ln in body.lines],
        expected_version=body.expected_version,
    )


@router.post("/despachos/{plan_id}/visitas/{visit_id}/cobranza")
def declare_visit_collection(
    plan_id: int,
    visit_id: int,
    body: DeclareCollectionBody,
    user: dict = Depends(require_pwa_profiles("chofer")),
):
    return visit_svc.declare_collection(
        user=user,
        plan_id=int(plan_id),
        visit_id=int(visit_id),
        document_result_id=body.document_result_id,
        method=body.method,
        document_amount=body.document_amount,
        declared_amount=body.declared_amount,
        note=body.note,
    )


@router.post("/despachos/{plan_id}/visitas/{visit_id}/reporte-whatsapp")
def generate_whatsapp_report(
    plan_id: int,
    visit_id: int,
    body: WhatsAppReportBody,
    user: dict = Depends(require_pwa_profiles("chofer")),
):
    return visit_svc.build_whatsapp_report(
        user=user,
        plan_id=int(plan_id),
        visit_id=int(visit_id),
        client_name=body.client_name,
        planning_code=body.planning_code,
        driver_name=body.driver_name,
    )


@router.post("/despachos/{plan_id}/visitas/{visit_id}/evidencia")
async def upload_visit_evidence(
    plan_id: int,
    visit_id: int,
    evidence_kind: str = Form(...),
    latitude: float | None = Form(None),
    longitude: float | None = Form(None),
    accuracy_m: float | None = Form(None),
    idempotency_key: str | None = Form(None),
    file: UploadFile = File(...),
    user: dict = Depends(require_pwa_profiles("chofer")),
):
    data = await file.read()
    return visit_svc.upload_incident_evidence(
        user=user,
        plan_id=int(plan_id),
        visit_id=int(visit_id),
        evidence_kind=evidence_kind,
        data=data,
        content_type=file.content_type,
        latitude=latitude,
        longitude=longitude,
        accuracy_m=accuracy_m,
        idempotency_key=idempotency_key,
    )


class EvidenceExceptionBody(BaseModel):
    evidence_kind: str = Field(..., max_length=40)
    exception_note: str = Field(..., max_length=500)
    latitude: float | None = None
    longitude: float | None = None


@router.post("/despachos/{plan_id}/visitas/{visit_id}/evidencia/excepcion")
def register_visit_evidence_exception(
    plan_id: int,
    visit_id: int,
    body: EvidenceExceptionBody,
    user: dict = Depends(require_pwa_profiles("chofer")),
):
    return visit_svc.register_evidence_exception(
        user=user,
        plan_id=int(plan_id),
        visit_id=int(visit_id),
        evidence_kind=body.evidence_kind,
        exception_note=body.exception_note,
        latitude=body.latitude,
        longitude=body.longitude,
    )


@router.post("/despachos/{plan_id}/visitas/{visit_id}/conformidad")
async def submit_visit_conformity(
    plan_id: int,
    visit_id: int,
    receptor_name: str = Form(""),
    refused: bool = Form(False),
    refuse_reason: str | None = Form(None),
    latitude: float | None = Form(None),
    longitude: float | None = Form(None),
    accuracy_m: float | None = Form(None),
    signature: UploadFile | None = File(None),
    user: dict = Depends(require_pwa_profiles("chofer")),
):
    sig_bytes = await signature.read() if signature is not None else None
    return visit_svc.submit_conformity(
        user=user,
        plan_id=int(plan_id),
        visit_id=int(visit_id),
        receptor_name=receptor_name,
        signature_png=sig_bytes,
        refused=bool(refused),
        refuse_reason=refuse_reason,
        latitude=latitude,
        longitude=longitude,
        accuracy_m=accuracy_m,
    )


@router.post("/despachos/{plan_id}/clientes/{customer_key:path}/ubicacion")
def propose_client_location(
    plan_id: int,
    customer_key: str,
    body: LocationProposalBody,
    user: dict = Depends(require_pwa_profiles("chofer")),
):
    """Propuesta GPS del chofer. No aprueba ni escribe bsale.clients."""
    return loc_svc.propose_location(
        user=user,
        plan_id=int(plan_id),
        customer_key=customer_key,
        latitude=float(body.latitude),
        longitude=float(body.longitude),
        accuracy_m=body.accuracy_m,
        captured_at=body.captured_at,
        confirm_low_accuracy=bool(body.confirm_low_accuracy),
        confirm_significant_move=bool(body.confirm_significant_move),
        facade_photo_id=body.facade_photo_id,
        idempotency_key=body.idempotency_key,
    )


@router.post("/despachos/{plan_id}/clientes/{customer_key:path}/foto-fachada/prepare")
def prepare_facade_photo(
    plan_id: int,
    customer_key: str,
    body: FacadePrepareBody,
    user: dict = Depends(require_pwa_profiles("chofer")),
):
    return loc_svc.prepare_facade_upload(
        user=user,
        plan_id=int(plan_id),
        customer_key=customer_key,
        content_type=body.content_type,
        idempotency_key=body.idempotency_key,
    )


@router.post("/despachos/{plan_id}/clientes/{customer_key:path}/foto-fachada/confirm")
def confirm_facade_photo(
    plan_id: int,
    customer_key: str,
    body: FacadeConfirmBody,
    user: dict = Depends(require_pwa_profiles("chofer")),
):
    return loc_svc.confirm_facade_upload(
        user=user,
        plan_id=int(plan_id),
        customer_key=customer_key,
        photo_id=int(body.photo_id),
    )


@router.post("/despachos/{plan_id}/clientes/{customer_key:path}/foto-fachada")
async def upload_facade_photo(
    plan_id: int,
    customer_key: str,
    file: UploadFile = File(...),
    idempotency_key: str | None = Form(None),
    user: dict = Depends(require_pwa_profiles("chofer")),
):
    """Subida multipart vía API (alternativa a URL firmada)."""
    data = await file.read()
    return loc_svc.upload_facade_bytes(
        user=user,
        plan_id=int(plan_id),
        customer_key=customer_key,
        content_type=file.content_type or "image/jpeg",
        data=data,
        idempotency_key=idempotency_key,
    )
