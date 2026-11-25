"""Errores de dominio warehouse (sin HTTP)."""


class WarehouseError(Exception):
    def __init__(self, message: str, *, code: str = "warehouse_error"):
        super().__init__(message)
        self.code = code


class WarehouseNotFoundError(WarehouseError):
    def __init__(self, message: str, *, code: str = "not_found"):
        super().__init__(message, code=code)


class WarehouseConflictError(WarehouseError):
    def __init__(self, message: str, *, code: str = "conflict"):
        super().__init__(message, code=code)


class WarehouseValidationError(WarehouseError):
    def __init__(self, message: str, *, code: str = "validation_error"):
        super().__init__(message, code=code)
