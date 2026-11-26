"""Solo PostgreSQL desechable local. Nunca usar analytics ni credenciales productivas.

Crear una BD VACÍA llamada warehouse_pallets_test_<sufijo>; definir
WAREHOUSE_PALLETS_TEST_DSN para esa BD. No elimina schemas ni usa datos reales.
"""
import os
from pathlib import Path
from uuid import uuid4

import psycopg2
from psycopg2.extensions import parse_dsn
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.services.warehouse.pallets import PalletService
from backend.services.warehouse.errors import WarehouseConflictError, WarehouseValidationError
from backend.routers import warehouse as router_module

DSN = os.getenv("WAREHOUSE_PALLETS_TEST_DSN", "").strip()
SQL = Path(__file__).resolve().parents[1] / "sql" / "warehouse"
pytestmark = pytest.mark.skipif(not DSN,reason="Requiere BD local VACÍA warehouse_pallets_test_<sufijo>")


@pytest.fixture(scope="module")
def service():
    config=parse_dsn(DSN)
    db=config.get("dbname","")
    host=config.get("host","")
    assert db.startswith("warehouse_pallets_test_")
    assert host in ("localhost","127.0.0.1","::1","") or host.startswith("/var/run/postgresql")
    assert not any(s in DSN.lower() for s in ("analytics","quillotana","production","coolify"))
    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT current_database(),current_setting('data_directory'),to_regnamespace('warehouse'),to_regnamespace('bsale')")
            actual,datadir,warehouse,bsale=cur.fetchone()
            assert actual==db
            assert not any(s in datadir.lower() for s in ("analytics","quillotana","production","coolify"))
            assert warehouse is None and bsale is None, "Usar una BD nueva; nunca limpiar schemas existentes."
            cur.execute("""
                CREATE SCHEMA bsale;
                CREATE TABLE bsale.products(company_id bigint,bsale_id bigint,name text,product_type_id bigint,PRIMARY KEY(company_id,bsale_id));
                CREATE TABLE bsale.product_types(company_id bigint,bsale_id bigint,name text,PRIMARY KEY(company_id,bsale_id));
                CREATE TABLE bsale.variants(company_id bigint,bsale_id bigint,product_id bigint,description text,bar_code text,code text,units_per_box integer,PRIMARY KEY(company_id,bsale_id));
                INSERT INTO bsale.product_types VALUES(3,1,'Bebidas');
                INSERT INTO bsale.products VALUES(3,1,'Producto fixture',1),(3,2,'Otro producto',1),(4,1,'Otra empresa',NULL);
                INSERT INTO bsale.variants VALUES(3,100,1,'350 cc','000100','SKU100',24),(3,101,2,'1 litro','000101','SKU101',NULL),(4,200,1,'Otra empresa','000200','SKU200',12);
            """)
            cur.execute((SQL/"001_schema_layout_AWAITING_APPROVAL.sql").read_text(encoding="utf-8"))
            cur.execute("SET warehouse.pallets_lab='off'")
            with pytest.raises(psycopg2.Error):
                cur.execute((SQL/"008_pallet_register_AWAITING_APPROVAL.sql").read_text(encoding="utf-8"))
            conn.rollback()
            cur.execute("SET warehouse.pallets_lab='on'")
            for _ in range(2):
                cur.execute((SQL/"008_pallet_register_AWAITING_APPROVAL.sql").read_text(encoding="utf-8"))
            cur.execute("""
                INSERT INTO warehouse.facilities(id,code,name,company_id) VALUES(1,'BC','Fixture',3),(2,'OTHER','Fixture otra empresa',4);
                INSERT INTO warehouse.floors(id,facility_id,code,name,length_m,width_m) VALUES(1,1,'PISO1','Piso',23.1,13.7),(2,2,'PISO1','Otro piso',10,10);
                INSERT INTO warehouse.layout_revisions(id,floor_id,status,row_version) VALUES(1,1,'draft',6),(2,2,'draft',1);
                INSERT INTO warehouse.physical_elements(id,facility_id,floor_id,element_type,code,name)
                VALUES(1,1,1,'position','P1-01','Posición 1'),(2,1,1,'position','P1-02','Posición 2'),
                      (3,1,1,'position','BAE-01','Bajo escalera'),(4,2,2,'position','OT-01','Otra empresa');
                INSERT INTO warehouse.layout_elements(revision_id,physical_element_id,code_snap,name_snap,element_type_snap,width_m,depth_m)
                SELECT CASE WHEN floor_id=1 THEN 1 ELSE 2 END,id,code,name,element_type,1.26,1.06 FROM warehouse.physical_elements;
            """)
    return PalletService(lambda:psycopg2.connect(DSN))


def line(variant=100, **overrides):
    return {"variant_id":variant,"quantity_status":"verified","boxes":2,"loose_units":1,
            "expected_units_per_box":24,"expiry_status":"dated","expires_on":"2027-01-01","lot":"LOT-A",**overrides}


def body(**overrides):
    return {"registration_key":str(uuid4()),"floor_id":1,"pallet_kind":"complete",
            "position_id":None,"observed_level":None,"layout_revision_id":None,"layout_row_version":None,
            "observed_ack":False,"received_on":None,"notes":None,"lines":[line()],**overrides}


def loc(position,level=1,version=6):
    return {"position_id":position,"observed_level":level,"layout_revision_id":1,"layout_row_version":version,"observed_ack":True}


def create(svc,data):
    return svc.register(data,actor="lab",uid=1)


def test_register_retry_move_void_and_history(service):
    data=body(**loc(1))
    first=create(service,data)
    assert first["lines"][0]["total_units"]==49
    assert first["code"].startswith("PAL-")
    assert create(service,data)["id"]==first["id"]
    with pytest.raises(WarehouseConflictError):
        create(service,{**data,"notes":"changed"})
    with pytest.raises(WarehouseConflictError):
        create(service,body(**loc(1)))
    upper=create(service,body(**loc(1,2)))
    assert upper["observed_level"]==2
    with pytest.raises(WarehouseConflictError):
        service.observe_location(first["id"],{**loc(2),"expected_row_version":9,"reason":"stale"},actor="lab",uid=1)
    moved=service.observe_location(first["id"],{**loc(2),"expected_row_version":1,"reason":"Ubicación observada"},actor="lab",uid=1)
    assert moved["code"]==first["code"] and moved["position_id"]==2 and moved["row_version"]==2
    voided=service.void(first["id"],{"expected_row_version":2,"reason":"Registro duplicado"},actor="lab",uid=1)
    assert voided["status"]=="voided"
    history=service.get(first["id"])["events"]
    assert [e["action"] for e in history]==["voided","location_observed","registered"]
    assert history[1]["detail_json"]["before"]["position_id"]==1
    assert history[1]["detail_json"]["after"]["position_id"]==2
    new=create(service,body(**loc(2)))
    assert new["id"]!=first["id"]
    assert service.locations(1)["revision"]["row_version"]==6


def test_mixed_unknown_and_catalog_scope(service):
    found=service.search_products(1,"000100")["items"]
    assert len(found)==1 and found[0]["variant_id"]==100
    assert not service.search_products(1,"000200")["items"]
    with pytest.raises(WarehouseValidationError):
        create(service,body(lines=[line(200)]))
    with pytest.raises(WarehouseValidationError):
        create(service,body(pallet_kind="mixed",lines=[line()]))
    unknown=line(101,quantity_status="unknown",boxes=None,loose_units=None,expected_units_per_box=None,
                 expiry_status="unknown",expires_on=None,lot=None)
    p=create(service,body(pallet_kind="mixed",lines=[line(),unknown]))
    assert p["received_on"] is None and p["position_id"] is None
    assert p["lines"][1]["total_units"] is None
    assert any(x["id"]==p["id"] for x in service.list(1,query=p["code"])["items"])


def test_invalid_layout_and_ack_rollback(service):
    with pytest.raises(WarehouseConflictError):
        create(service,body(**loc(3,version=5)))
    with pytest.raises(WarehouseValidationError):
        create(service,body(**{**loc(3),"observed_ack":False}))
    with pytest.raises(WarehouseValidationError):
        create(service,body(**loc(4)))
    with pytest.raises(WarehouseValidationError):
        create(service,body(lines=[line(),line(101)]))
    assert not any(s["position_id"]==3 for s in service.locations(1)["observed_slots"])


def test_routes_with_management_dependency_and_no_catalog_writes(service,monkeypatch):
    app=FastAPI()
    app.include_router(router_module.router)
    monkeypatch.setattr(router_module,"default_pallet_service",lambda:service)
    client=TestClient(app)
    # Real management dependency must reject absent credentials.
    assert client.get("/warehouse/pallets?floor_id=1").status_code in (401,403)
    app.dependency_overrides[router_module.require_management_access]=lambda:{"id":1,"email":"lab"}
    assert client.get("/warehouse/pallets/locations?floor_id=1").status_code==200
    assert client.get("/warehouse/pallets/products?floor_id=1&q=SKU100").status_code==200
    response=client.post("/warehouse/pallets",json=body())
    assert response.status_code==201,response.text
    pid=response.json()["id"]
    assert client.get(f"/warehouse/pallets/{pid}").json()["events"][0]["action"]=="registered"
    assert client.post("/warehouse/pallets",json={**body(),"total_units":999}).status_code==422
    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT units_per_box FROM bsale.variants WHERE company_id=3 AND bsale_id=100")
            assert cur.fetchone()[0]==24
            cur.execute("SELECT row_version FROM warehouse.layout_revisions WHERE id=1")
            assert cur.fetchone()[0]==6
