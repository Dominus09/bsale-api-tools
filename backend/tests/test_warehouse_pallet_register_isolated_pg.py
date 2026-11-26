"""Solo PostgreSQL desechable local. Nunca usar analytics ni credenciales productivas.

Crear una BD VACÍA llamada warehouse_pallets_test_<sufijo>; definir
WAREHOUSE_PALLETS_TEST_DSN para esa BD. No elimina schemas ni usa datos reales.
"""
import os
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import psycopg2
from psycopg2.extensions import parse_dsn
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.services.warehouse.pallets import PalletService, chile_business_date
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


def test_received_on_real_date_and_future_rejected(service, monkeypatch):
    app = FastAPI()
    app.include_router(router_module.router)
    monkeypatch.setattr(router_module, "default_pallet_service", lambda: service)
    app.dependency_overrides[router_module.require_management_access] = lambda: {"id": 1, "email": "lab"}
    client = TestClient(app)
    today = chile_business_date()
    payload = body(received_on=today.isoformat())
    response = client.post("/warehouse/pallets", json=payload)
    assert response.status_code == 201, response.text
    saved = response.json()
    assert saved["received_on"] == today.isoformat()
    assert client.get(f"/warehouse/pallets/{saved['id']}").json()["received_on"] == today.isoformat()
    # Más de un día para evitar carrera si la prueba cruza medianoche.
    future = body(received_on=(today + timedelta(days=2)).isoformat())
    rejected = client.post("/warehouse/pallets", json=future)
    assert rejected.status_code == 400, rejected.text
    assert "Fecha de ingreso futura" in rejected.text
    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM warehouse.pallets WHERE registration_key=%s::uuid",
                        (future["registration_key"],))
            assert cur.fetchone()[0] == 0


def test_pallet_details_migration_edit_and_history(service, monkeypatch):
    from backend.services.warehouse.pallets import PalletSchemaUnavailable
    # Un pallet real de fixture; la ubicación nunca cambia durante la edición.
    original = create(service, body(**loc(3), lines=[line(quantity_status="unknown",boxes=None,loose_units=None,
        expiry_status="unknown",expires_on=None,lot=None)]))
    old_line = original["lines"][0]["id"]
    patch = {"expected_row_version":1,"reason":"Conteo físico verificado","pallet_kind":"complete",
             "received_on":chile_business_date().isoformat(),"notes":"Fixture corrección",
             "lines":[{**line(),"line_id":old_line}],"remove_line_ids":[]}
    with pytest.raises(PalletSchemaUnavailable):
        service.update_details(original["id"],patch,actor="lab",uid=1)
    assert service.get(original["id"])["row_version"] == 1
    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("SET warehouse.pallets_lab='off'")
            with pytest.raises(psycopg2.Error):
                cur.execute((SQL/"009_pallet_details_audit_AWAITING_APPROVAL.sql").read_text(encoding="utf-8"))
            conn.rollback()
            cur.execute("SET warehouse.pallets_lab='on'")
            for _ in range(2):
                cur.execute((SQL/"009_pallet_details_audit_AWAITING_APPROVAL.sql").read_text(encoding="utf-8"))
    app = FastAPI()
    app.include_router(router_module.router)
    monkeypatch.setattr(router_module,"default_pallet_service",lambda:service)
    client=TestClient(app)
    endpoint=f"/warehouse/pallets/{original['id']}/details"
    assert client.patch(endpoint,json=patch).status_code in (401,403)
    app.dependency_overrides[router_module.require_management_access]=lambda:{"id":1,"email":"lab"}
    response=client.patch(endpoint,json=patch)
    assert response.status_code==200,response.text
    updated=response.json()
    assert updated["id"]==original["id"] and updated["code"]==original["code"]
    assert updated["position_id"]==original["position_id"] and updated["observed_level"]==original["observed_level"]
    assert updated["lines"][0]["id"]==old_line and updated["lines"][0]["total_units"]==49
    assert updated["row_version"]==2
    assert client.patch(endpoint,json=patch).status_code==409
    slots=client.get("/warehouse/pallets/locations?floor_id=1").json()["observed_slots"]
    assert any(s["pallet_id"]==original["id"] and s["pallet_code"]==original["code"] for s in slots)
    history=service.get(original["id"])["events"]
    assert history[0]["action"]=="details_updated"
    assert history[0]["detail_json"]["before"]["lines"][0]["total_units"] is None
    assert history[0]["detail_json"]["after"]["lines"][0]["total_units"]==49
    # Omisión accidental, cambio de variante bajo el mismo ID y fecha futura.
    bad={**patch,"expected_row_version":2}
    for payload in (
        {**bad,"lines":[{**line(),"line_id":None}]},
        {**bad,"lines":[{**line(101,boxes=0,loose_units=1,expected_units_per_box=None),"line_id":old_line}]},
        {**bad,"received_on":(chile_business_date()+timedelta(days=2)).isoformat()},
    ):
        assert client.patch(endpoint,json=payload).status_code==400
    assert service.get(original["id"])["row_version"]==2
    # Corrección explícita: otra variante y lote, nueva línea y retiro auditado.
    replacement={**bad,"pallet_kind":"partial","lines":[{**line(101,boxes=0,loose_units=8,
        expected_units_per_box=None),"line_id":None}],"remove_line_ids":[old_line]}
    replaced=client.patch(endpoint,json=replacement)
    assert replaced.status_code==200,replaced.text
    assert replaced.json()["code"]==original["code"]
    assert replaced.json()["lines"][0]["id"]!=old_line
    event=service.get(original["id"])["events"][0]
    assert event["detail_json"]["removed_line_ids"]==[old_line]
    assert event["detail_json"]["before"]["lines"][0]["id"]==old_line
    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT row_version FROM warehouse.layout_revisions WHERE id=1")
            assert cur.fetchone()[0]==6
            cur.execute("SELECT units_per_box FROM bsale.variants WHERE company_id=3 AND bsale_id=100")
            assert cur.fetchone()[0]==24
    service.void(original["id"],{"expected_row_version":3,"reason":"Fixture finalizado"},actor="lab",uid=1)
    assert client.patch(endpoint,json={**replacement,"expected_row_version":4}).status_code==409


# Fase 2C: solo datos de laboratorio en la BD desechable del fixture.
@pytest.fixture(scope="module")
def batch_service(service):
    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO warehouse.physical_elements(id,facility_id,floor_id,element_type,code,name)
                           VALUES(100,1,1,'aisle','P7','Pasillo 7 fixture')""")
            cur.execute("""INSERT INTO warehouse.physical_elements(id,facility_id,floor_id,parent_id,element_type,code,name)
                           SELECT i,1,1,100,'position','P7-'||lpad((i-100)::text,2,'0'),'Fixture '||i
                           FROM generate_series(101,116) i""")
            cur.execute("""INSERT INTO warehouse.layout_elements
                (revision_id,physical_element_id,code_snap,name_snap,element_type_snap,parent_physical_id_snap,width_m,depth_m)
                SELECT 1,id,code,name,element_type,parent_id,1.26,1.06 FROM warehouse.physical_elements WHERE id BETWEEN 100 AND 116""")
    return service


def batch_body(positions,level=1,**patch):
    return {"batch_key":str(uuid4()),"floor_id":1,"layout_revision_id":1,
            "layout_row_version":6,"observed_level":level,"observed_ack":True,
            "items":[{"position_id":pid,"pallet_kind":"complete","received_on":None,
                      "notes":None,"lines":[line()]} for pid in positions],**patch}


def batch_counts():
    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as cur:
            result=[]
            for table in ("pallets","pallet_lines","pallet_events"):
                cur.execute("SELECT count(*) FROM warehouse."+table)
                result.append(cur.fetchone()[0])
            return tuple(result)


def test_batch_ten_positions_and_exact_replay(batch_service,monkeypatch):
    service=batch_service
    app=FastAPI()
    app.include_router(router_module.router)
    monkeypatch.setattr(router_module,"default_pallet_service",lambda:service)
    client=TestClient(app)
    payload=batch_body(list(range(101,111)))
    payload["items"][2]["lines"]=[line(quantity_status="unknown",boxes=None,loose_units=None,
        expiry_status="unknown",expires_on=None,lot=None)]
    payload["items"][3]["lines"]=[line(boxes=3,loose_units=0,lot="LOT-B"),
        line(boxes=1,loose_units=0,lot="LOT-C",expires_on="2027-02-01")]
    payload["items"][4]["pallet_kind"]="partial"
    payload["items"][4]["received_on"]=chile_business_date().isoformat()
    before=batch_counts()
    assert client.post("/warehouse/pallets/batch",json=payload).status_code in (401,403)
    app.dependency_overrides[router_module.require_management_access]=lambda:{"id":1,"email":"lab"}
    saved=client.post("/warehouse/pallets/batch",json=payload)
    assert saved.status_code==201,saved.text
    result=saved.json()
    records=result["items"]
    assert not result["replayed"] and len({p["code"] for p in records})==10
    assert [p["position_id"] for p in records]==list(range(101,111))
    assert records[2]["lines"][0]["total_units"] is None
    assert len(records[3]["lines"])==2 and records[3]["lines"][1]["lot"]=="LOT-C"
    assert records[4]["pallet_kind"]=="partial"
    assert batch_counts()==tuple(a+b for a,b in zip(before,(10,11,10)))
    for i,p in enumerate(records,1):
        event=service.get(p["id"])["events"][0]
        assert event["action"]=="registered"
        assert event["detail_json"]["registration_batch"]=={"batch_key":payload["batch_key"],"ordinal":i,"size":10}
    replay=client.post("/warehouse/pallets/batch",json={**payload,"items":list(reversed(payload["items"]))})
    assert replay.status_code==201 and replay.json()["replayed"]
    assert [p["id"] for p in replay.json()["items"]]==[p["id"] for p in records]
    # Reuse same UUID even for a completely different set or a shorter group.
    for items in (payload["items"][:2],batch_body([111,112])["items"]):
        bad=client.post("/warehouse/pallets/batch",json={**payload,"items":items})
        assert bad.status_code==409,bad.text
    assert batch_counts()==tuple(a+b for a,b in zip(before,(10,11,10)))
    assert service.locations(1)["revision"]["row_version"]==6
    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT units_per_box FROM bsale.variants WHERE company_id=3 AND bsale_id=100")
            assert cur.fetchone()[0]==24


def test_batch_validation_and_rollback_no_partial_rows(batch_service,monkeypatch):
    service=batch_service
    # Independent of the ten-position test and its execution order.
    create(service,body(**loc(116,level=8)))
    before=batch_counts()
    for payload,exc in (
        (batch_body([111,116],level=8),WarehouseConflictError),  # occupied
        (batch_body([111,3]),WarehouseValidationError), # restricted BAE
        (batch_body([111,4]),WarehouseValidationError), # different company/floor
        (batch_body([111,112],layout_row_version=5),WarehouseConflictError),
    ):
        with pytest.raises(exc):
            service.register_batch(payload,actor="lab",uid=1)
        assert batch_counts()==before
    bad=batch_body([111,112])
    bad["items"][1]["lines"]=[line(200)]
    with pytest.raises(WarehouseValidationError):
        service.register_batch(bad,actor="lab",uid=1)
    assert batch_counts()==before
    # Force a real transactional rollback after INSERT into all three tables.
    original=service._insert_registration
    count=0
    def fail_after_first(*args,**kwargs):
        nonlocal count
        result=original(*args,**kwargs)
        count+=1
        if count==1:
            raise WarehouseValidationError("fixture rollback after first pallet")
        return result
    monkeypatch.setattr(service,"_insert_registration",fail_after_first)
    with pytest.raises(WarehouseValidationError):
        service.register_batch(batch_body([111,112]),actor="lab",uid=1)
    assert count==1 and batch_counts()==before


def test_batch_concurrent_overlap_rolls_back_losing_group(batch_service):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier
    # Both transactions have inserted a different first slot before racing
    # for the shared second slot. The unique index must roll the loser back.
    barrier=Barrier(2,timeout=15)
    before=batch_counts()
    def run(positions):
        local=PalletService(lambda:psycopg2.connect(DSN))
        original=local._insert_registration
        count=0
        def insert(*args,**kwargs):
            nonlocal count
            result=original(*args,**kwargs)
            count+=1
            if count==1: barrier.wait()
            return result
        local._insert_registration=insert
        try:
            result=local.register_batch(batch_body(positions,level=9),actor="lab",uid=1)
            return ("ok",positions,result)
        except WarehouseConflictError:
            return ("conflict",positions,None)
    with ThreadPoolExecutor(max_workers=2) as pool:
        a=pool.submit(run,[111,113])
        b=pool.submit(run,[112,113])
        results=[a.result(timeout=45),b.result(timeout=45)]
    assert sorted(r[0] for r in results)==["conflict","ok"]
    winner=next(r[1] for r in results if r[0]=="ok")
    slots=[s for s in batch_service.locations(1)["observed_slots"] if s["observed_level"]==9]
    assert {s["position_id"] for s in slots}==set(winner)
    assert batch_counts()==tuple(a+2 for a in before)


def test_batch_simultaneous_retry_is_exactly_once(batch_service):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier
    before=batch_counts()
    payload=batch_body([114,115],level=10)
    barrier=Barrier(2,timeout=15)
    def run():
        barrier.wait()
        return batch_service.register_batch(payload,actor="lab",uid=1)
    with ThreadPoolExecutor(max_workers=2) as pool:
        a,b=pool.submit(run),pool.submit(run)
        results=[a.result(timeout=45),b.result(timeout=45)]
    assert sorted(r["replayed"] for r in results)==[False,True]
    assert [p["id"] for p in results[0]["items"]]==[p["id"] for p in results[1]["items"]]
    assert batch_counts()==tuple(a+2 for a in before)


def test_pallet_labels_batch_read_only_identity_and_void(batch_service,monkeypatch):
    service=batch_service
    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("SET warehouse.pallets_lab='on'")
            cur.execute((SQL/"009_pallet_details_audit_AWAITING_APPROVAL.sql").read_text(encoding="utf-8"))
    registered=service.register_batch(batch_body([101,102],level=15),actor="lab",uid=1)["items"]
    ids=[p["id"] for p in registered]
    payload={"floor_id":1,"pallet_ids":list(reversed(ids))}
    app=FastAPI()
    app.include_router(router_module.router)
    monkeypatch.setattr(router_module,"default_pallet_service",lambda:service)
    client=TestClient(app)
    url="/warehouse/pallets/labels/preview"
    before=batch_counts()
    assert client.post(url,json=payload).status_code in (401,403)
    app.dependency_overrides[router_module.require_management_access]=lambda:{"id":1,"email":"lab"}
    response=client.post(url,json=payload)
    assert response.status_code==200,response.text
    assert response.headers["cache-control"]=="no-store"
    labels=response.json()["items"]
    assert [p["id"] for p in labels]==payload["pallet_ids"]
    assert len({p["qr_value"] for p in labels})==2
    for label in labels:
        assert label["qr_value"]==label["code"]
        assert not ({"position_id","position_code_snap","observed_level","floor_id","notes","events"}&set(label))
        assert label["lines"][0]["product_name_snap"]=="Producto fixture"
    assert batch_counts()==before
    positions=service.locations(1)["items"]
    assert next(p for p in positions if p["id"]==101)["parent_code"]=="P7"
    assert next(p for p in positions if p["id"]==101)["parent_id"]==100
    assert service.locations(1)["revision"]["row_version"]==6
    old_label=labels[1]
    moved=service.observe_location(ids[0],{**loc(103,15),"expected_row_version":1,"reason":"Traslado observado"},actor="lab",uid=1)
    assert moved["position_id"]==103
    service.update_details(ids[0],{
        "expected_row_version":2,"reason":"Fecha verificada","pallet_kind":"partial",
        "received_on":None,"notes":None,"remove_line_ids":[],
        "lines":[{**line(boxes=4,expires_on="2028-02-20"),"line_id":moved["lines"][0]["id"]}],
    },actor="lab",uid=1)
    before=batch_counts()
    new_label=client.post(url,json={"floor_id":1,"pallet_ids":[ids[0]]}).json()["items"][0]
    assert new_label["qr_matrix"]==old_label["qr_matrix"]
    assert new_label["qr_value"]==old_label["qr_value"]
    assert new_label["row_version"]==3
    assert new_label["lines"][0]["expires_on"]=="2028-02-20"
    assert new_label["lines"][0]["total_units"]==97
    assert batch_counts()==before
    assert client.get(f"/warehouse/pallets/{ids[0]}").json()["position_code_snap"]=="P7-03"
    assert client.post(url,json={"floor_id":1,"pallet_ids":[ids[0],ids[0]]}).status_code==422
    assert client.post(url,json={"floor_id":1,"pallet_ids":[ids[0],999999999]}).status_code==409
    assert client.post(url,json={"floor_id":2,"pallet_ids":[ids[0]]}).status_code==404
    assert batch_counts()==before
    service.void(ids[1],{"expected_row_version":1,"reason":"Anulación fixture"},actor="lab",uid=1)
    before=batch_counts()
    assert client.post(url,json=payload).status_code==409
    assert client.get(f"/warehouse/pallets/{ids[1]}").json()["status"]=="voided"
    assert batch_counts()==before
    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT units_per_box FROM bsale.variants WHERE company_id=3 AND bsale_id=100")
            assert cur.fetchone()[0]==24
            cur.execute("SELECT row_version FROM warehouse.layout_revisions WHERE id=1")
            assert cur.fetchone()[0]==6
