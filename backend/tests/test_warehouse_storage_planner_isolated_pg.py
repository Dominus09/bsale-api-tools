"""PostgreSQL desechable, independiente de los demás tests.
WAREHOUSE_PLANNER_TEST_DSN: BD VACÍA local warehouse_pallets_test_planner_<sufijo>.
Nunca ejecutar contra analytics ni conexiones productivas.
"""
import os
from pathlib import Path
from uuid import uuid4

import psycopg2
from psycopg2.extensions import parse_dsn
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from backend.routers import warehouse_storage_planner as module
from backend.services.warehouse.storage_planner import StoragePlanner, PlanSave
from backend.services.warehouse.errors import WarehouseConflictError, WarehouseValidationError

DSN=os.getenv("WAREHOUSE_PLANNER_TEST_DSN","").strip()
SQL=Path(__file__).resolve().parents[1]/"sql"/"warehouse"
pytestmark=pytest.mark.skipif(not DSN,reason="Requiere PostgreSQL local desechable")

@pytest.fixture(scope="module")
def lab():
    config=parse_dsn(DSN)
    db=config.get("dbname","")
    assert db.startswith("warehouse_pallets_test_planner_")
    host=config.get("host","")
    assert host in ("localhost","127.0.0.1","::1","") or host.startswith("/var/run/postgresql")
    assert not any(s in DSN.lower() for s in ("analytics","quillotana","production","coolify"))
    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT current_database(),current_setting('data_directory'),to_regnamespace('warehouse'),to_regnamespace('bsale')")
            actual,datadir,w,b=cur.fetchone()
            assert actual==db and w is None and b is None
            assert not any(s in datadir.lower() for s in ("analytics","quillotana","production","coolify"))
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
            cur.execute("SET warehouse.pallets_lab='on'")
            cur.execute((SQL/"008_pallet_register_AWAITING_APPROVAL.sql").read_text(encoding="utf-8"))
            for _ in range(2):
                for name in ("010_storage_planner_AWAITING_APPROVAL.sql","011_seed_bodega_lata_draft_AWAITING_APPROVAL.sql"):
                    cur.execute((SQL/name).read_text(encoding="utf-8"))
            cur.execute("""SELECT fl.id FROM warehouse.floors fl JOIN warehouse.facilities f ON f.id=fl.facility_id WHERE f.code='BL' AND f.company_id=3""")
            fid,=cur.fetchone()
    svc=StoragePlanner(lambda:psycopg2.connect(DSN))
    locations=svc.locations(fid)
    return svc,fid,locations

def body(lab,**overrides):
    _,fid,loc=lab
    p=loc["items"][0]
    return {"id":str(uuid4()),"name":"Plan fixture","expected_row_version":0,
            "counts":[{"variant_id":100,"source":"BL / P1","kind":"complete","count":2}],
            "slots":[{"floor_id":fid,"revision_id":loc["revision"]["id"],"revision_version":loc["revision"]["row_version"],
                     "position_id":p["id"],"level":1,"variant_id":100,"kind":"complete","method":"direct"}],**overrides}

def test_seed_and_idempotence(lab):
    svc,fid,loc=lab
    assert len(loc["items"])==58
    assert loc["revision"]["status"]=="draft"
    assert not loc["observed_slots"]
    codes={p["code"] for p in loc["items"]}
    assert "BL-F01-01" not in codes and "BL-F01-05" not in codes
    assert all(p["stack_levels_max"] is None for p in loc["items"])

def test_persist_version_conflict_and_no_stock_writes(lab):
    svc,_,_=lab
    d=body(lab)
    first=svc.save_plan(PlanSave(**d),"lab")
    assert first["row_version"]==1
    assert first["document"]["products"][0]["barcode"]=="000100"
    assert svc.get_plan(first["id"])["document"]==first["document"]
    assert any(x["id"]==first["id"] for x in svc.list_plans()["items"])
    updated=svc.save_plan(PlanSave(**{**d,"expected_row_version":1,"name":"Editado"}),"lab")
    assert updated["row_version"]==2
    with pytest.raises(WarehouseConflictError):
        svc.save_plan(PlanSave(**{**d,"expected_row_version":1}),"lab")
    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as cur:
            for table in ("pallets","pallet_lines","pallet_events"):
                cur.execute("SELECT count(*) FROM warehouse."+table)
                assert cur.fetchone()[0]==0
            cur.execute("SELECT count(*) FROM bsale.variants")
            assert cur.fetchone()[0]==3

def test_reject_stale_layout_and_foreign_variant(lab):
    svc,_,_=lab
    d=body(lab)
    d["slots"][0]["revision_version"]+=1
    with pytest.raises(WarehouseConflictError):
        svc.save_plan(PlanSave(**d),"lab")
    d=body(lab,slots=[],counts=[{"variant_id":200,"source":"BL","kind":"complete","count":1}])
    with pytest.raises(WarehouseValidationError):
        svc.save_plan(PlanSave(**d),"lab")

def test_auth_guard_and_api(lab,monkeypatch):
    svc,_,_=lab
    monkeypatch.setattr(module,"default_storage_planner",lambda:svc)
    app=FastAPI();app.include_router(module.router)
    client=TestClient(app)
    assert client.get("/warehouse/storage-plans").status_code in (401,403)
    app.dependency_overrides[module.require_management_access]=lambda:{"email":"lab@example.invalid"}
    r=client.post("/warehouse/storage-plans",json=body(lab))
    assert r.status_code==200,r.text
    assert client.get("/warehouse/storage-plans/"+r.json()["id"]).status_code==200
