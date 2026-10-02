"""Declaración de recursos ``bsale_raw``. El orden de import respeta las dependencias padre → hijo."""

from backend.services.bsale_raw.resources import configuration  # noqa: F401
from backend.services.bsale_raw.resources import catalog  # noqa: F401
from backend.services.bsale_raw.resources import pricing  # noqa: F401
from backend.services.bsale_raw.resources import inventory  # noqa: F401
from backend.services.bsale_raw.resources import documents  # noqa: F401
