"""Compatibility exports for the shared operator authentication boundary."""
from security.operator_auth import (  # noqa: F401
    ManagerAuthMiddleware,
    is_operator_route,
    manager_auth_active,
)
