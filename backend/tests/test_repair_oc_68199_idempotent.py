"""El reparador delega el descubrimiento dinámico al reconciliador general."""

from unittest.mock import MagicMock, patch

from backend.jobs import repair_oc_68199_details_from_bsale_source as repair


def test_repair_delegates_folio_and_local_id_to_general_reconciler():
    expected = {
        "status": "dry_run_needs_sync",
        "local_document_id": 3832233,
        "current_bsale_source_document_id": 3832999,
        "wrote": False,
    }
    with (
        patch.object(repair, "read_bsale_token_from_env", return_value="token"),
        patch.object(repair, "BsaleClient", return_value=MagicMock()) as client_cls,
        patch.object(repair, "reconcile_one_oc", return_value=expected) as reconcile,
    ):
        result = repair.execute_repair(dry_run=True)

    assert result == expected
    reconcile.assert_called_once_with(
        client_cls.return_value,
        folio=68199,
        local_document_id=3832233,
        dry_run=True,
    )


def test_repair_execute_already_in_sync_does_not_recalculate_by_default():
    expected = {
        "status": "already_in_sync",
        "local_document_id": 3832233,
        "wrote": False,
    }
    with (
        patch.object(repair, "read_bsale_token_from_env", return_value="token"),
        patch.object(repair, "BsaleClient", return_value=MagicMock()),
        patch.object(repair, "reconcile_one_oc", return_value=expected),
        patch.object(repair, "recalculate_order_weight") as recalculate,
    ):
        result = repair.execute_repair(dry_run=False)

    assert result["status"] == "already_in_sync"
    assert result["wrote"] is False
    recalculate.assert_not_called()


def test_repair_optional_weight_recalculate_uses_keyword_only_signature():
    expected = {
        "status": "already_in_sync",
        "local_document_id": 3832233,
        "wrote": False,
    }
    with (
        patch.object(repair, "read_bsale_token_from_env", return_value="token"),
        patch.object(repair, "BsaleClient", return_value=MagicMock()),
        patch.object(repair, "reconcile_one_oc", return_value=expected),
        patch.object(
            repair,
            "recalculate_order_weight",
            return_value={"peso_total_kg": 300, "porcentaje_cobertura": 100},
        ) as recalculate,
    ):
        result = repair.execute_repair(
            dry_run=False,
            recalculate_weight=True,
        )

    recalculate.assert_called_once_with(
        document_id=3832233,
        company_id=3,
        office_id=1,
        persist=True,
    )
    assert result["peso_despues_kg"] == 300
