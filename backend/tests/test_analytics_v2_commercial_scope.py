from datetime import datetime, timezone

import pytest

from backend.services.analytics.validate_distribuidora_source import assert_sql_is_read_only
from backend.services.analytics_v2.commercial_scope import (
    PRICE_LISTS_SQL,
    CommercialScopeNotConfigured,
    is_price_list_active,
    policy_review_status,
    resolve_price_list_scope,
    result_validity,
    rule_in_commercial_scope,
)
from backend.services.analytics_v2.contracts import (
    COMMERCIAL_PRICE_LISTS,
    RETIRED_PRICE_LISTS,
    PolicyReviewStatus,
    PriceListScopeStatus as P,
    ResultValidity,
)

GONE = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _rows(*items):
    return [
        {"price_list_id": pid, "name": name, "state": state, "missing_since": missing}
        for pid, name, state, missing in items
    ]


C3_RAW = _rows(
    (3, "Mayorista antigua", 0, None),
    (12, "SUPERMERCADO LA QUILLOTANA", 0, None),
    (13, "RUTA Y WEB LA QUILLOTANA", 0, None),
    (14, "COMODITI ALCOHOLES", 1, None),
    (16, "MELINKA", 0, None),
    (20, "Lista vieja", 0, GONE),
)


def test_company3_configuration_matches_business_decision():
    assert set(COMMERCIAL_PRICE_LISTS[3]) == {12, 13, 16}
    assert set(RETIRED_PRICE_LISTS[3]) == {14}


def test_active_requires_state_zero_and_present():
    assert is_price_list_active(0, None)
    assert not is_price_list_active(1, None)
    assert not is_price_list_active(None, None)
    assert not is_price_list_active(0, GONE)


def test_company3_evaluates_only_authorized_and_vigent_lists():
    scope = resolve_price_list_scope(3, C3_RAW)
    assert scope.evaluated_ids == {12, 13, 16}
    status = {e.price_list_id: e.status for e in scope.entries}
    assert status[14] is P.RETIRADA
    assert status[3] is P.VIGENTE_NO_AUTORIZADA
    assert status[20] is P.NO_VIGENTE
    assert scope.discrepancies == ()
    assert not rule_in_commercial_scope(14, scope)
    assert rule_in_commercial_scope(13, scope)


def test_retired_comoditi_reactivated_in_raw_is_flagged_not_evaluated():
    rows = _rows((12, "a", 0, None), (13, "b", 0, None), (14, "COMODITI ALCOHOLES", 0, None), (16, "c", 0, None))
    scope = resolve_price_list_scope(3, rows)
    assert 14 not in scope.evaluated_ids
    assert [e.price_list_id for e in scope.discrepancies] == [14]
    assert scope.entry(14).status is P.RETIRADA_REACTIVADA
    assert scope.to_json()["discrepancies"][0]["discrepancy"] is True


def test_name_never_decides_vigency():
    rows = _rows((12, "COMODITI", 0, None), (13, "x", 0, None), (16, "y", 0, None))
    assert 12 in resolve_price_list_scope(3, rows).evaluated_ids


def test_authorized_list_not_vigent_in_raw_is_a_discrepancy():
    rows = _rows((12, "a", 0, None), (13, "b", 1, None))
    scope = resolve_price_list_scope(3, rows)
    assert scope.evaluated_ids == {12}
    assert {e.price_list_id: e.status for e in scope.discrepancies} == {
        13: P.AUTORIZADA_NO_VIGENTE,
        16: P.AUTORIZADA_NO_VIGENTE,
    }
    assert scope.entry(16).in_raw is False


def test_company_without_configuration_is_rejected_and_explicit_config_is_isolated():
    with pytest.raises(CommercialScopeNotConfigured):
        resolve_price_list_scope(1, _rows((2, "a", 0, None)))
    scope = resolve_price_list_scope(1, _rows((2, "a", 0, None), (12, "b", 0, None)), authorized={2: "Detalle"}, retired={})
    assert scope.evaluated_ids == {2}


def test_price_list_sql_is_read_only_company_scoped_and_skips_payload():
    assert_sql_is_read_only(PRICE_LISTS_SQL)
    assert "company_id = %s" in PRICE_LISTS_SQL
    assert "payload" not in PRICE_LISTS_SQL


def test_results_are_provisional_until_policy_and_cost_validated():
    assert policy_review_status() is PolicyReviewStatus.PENDIENTE_REVISION
    validity, reasons = result_validity(PolicyReviewStatus.PENDIENTE_REVISION, cost_verified=True)
    assert validity is ResultValidity.PROVISIONAL and reasons == ["politica_pendiente_revision"]
    validity, reasons = result_validity(PolicyReviewStatus.VALIDADA, cost_verified=False)
    assert validity is ResultValidity.PROVISIONAL and reasons == ["costo_no_verificado"]
    validity, _ = result_validity(None, cost_verified=True)
    assert validity is ResultValidity.PROVISIONAL
    validity, reasons = result_validity(PolicyReviewStatus.VALIDADA, cost_verified=True)
    assert validity is ResultValidity.VALIDADO and reasons == []
