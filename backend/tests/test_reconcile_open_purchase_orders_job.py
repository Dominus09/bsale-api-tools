"""CLI programable para reconciliación automática de OCs abiertas."""

from unittest.mock import MagicMock, patch

from backend.jobs import reconcile_open_purchase_orders as job


def test_cli_execute_passes_exact_batch_configuration(capsys):
    result = {"status": "completed", "errors": 0, "ocs_checked": 10}
    with (
        patch.object(job, "load_dotenv_if_available"),
        patch.object(job, "require_bsale_token", return_value="token"),
        patch.object(job, "BsaleClient", return_value=MagicMock()) as client_class,
        patch.object(
            job,
            "reconcile_open_purchase_orders_batch",
            return_value=result,
        ) as reconcile,
    ):
        exit_code = job.main(
            [
                "--execute",
                "--limit",
                "10",
                "--recent-days",
                "30",
                "--company-id",
                "3",
                "--office-id",
                "1",
            ]
        )

    assert exit_code == 0
    reconcile.assert_called_once_with(
        client_class.return_value,
        execute=True,
        limit=10,
        recent_days=30,
        company_id=3,
        office_id=1,
    )
    assert '"ocs_checked": 10' in capsys.readouterr().out


def test_cli_defaults_to_dry_run_and_dry_run_overrides_execute():
    with (
        patch.object(job, "load_dotenv_if_available"),
        patch.object(job, "require_bsale_token", return_value="token"),
        patch.object(job, "BsaleClient", return_value=MagicMock()),
        patch.object(
            job,
            "reconcile_open_purchase_orders_batch",
            return_value={"status": "completed", "errors": 0},
        ) as reconcile,
    ):
        assert job.main(["--execute", "--dry-run"]) == 0

    assert reconcile.call_args.kwargs["execute"] is False


def test_item_errors_exit_zero_unless_strict_flag():
    with (
        patch.object(job, "load_dotenv_if_available"),
        patch.object(job, "require_bsale_token", return_value="token"),
        patch.object(job, "BsaleClient", return_value=MagicMock()),
        patch.object(
            job,
            "reconcile_open_purchase_orders_batch",
            return_value={"status": "completed", "errors": 2},
        ),
    ):
        assert job.main(["--execute"]) == 0
        assert job.main(["--execute", "--fail-on-item-error"]) == 1


def test_temporary_sync_conflict_exits_zero():
    with (
        patch.object(job, "load_dotenv_if_available"),
        patch.object(job, "require_bsale_token", return_value="token"),
        patch.object(job, "BsaleClient", return_value=MagicMock()),
        patch.object(
            job,
            "reconcile_open_purchase_orders_batch",
            return_value={
                "status": "skipped_due_to_active_sync",
                "errors": 0,
            },
        ),
    ):
        assert job.main(["--execute"]) == 0


def test_global_failure_exits_one():
    with (
        patch.object(job, "load_dotenv_if_available"),
        patch.object(job, "require_bsale_token", return_value="token"),
        patch.object(job, "BsaleClient", return_value=MagicMock()),
        patch.object(
            job,
            "reconcile_open_purchase_orders_batch",
            side_effect=RuntimeError("PostgreSQL unavailable"),
        ),
    ):
        assert job.main(["--execute"]) == 1
