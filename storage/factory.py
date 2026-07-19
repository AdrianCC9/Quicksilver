from __future__ import annotations

from config import settings
from storage.local_mysql_storage import LocalMySQLStorage


def build_storage(backend: str | None = None):
    selected_backend = (backend or settings.storage_backend).lower()

    if selected_backend in {"mysql", "local", "local_mysql"}:
        return LocalMySQLStorage()

    if selected_backend == "snowflake":
        from storage.snowflake_storage import SnowflakeStorage

        return SnowflakeStorage()

    raise ValueError(
        "Unsupported STORAGE_BACKEND. Expected mysql/local or snowflake; "
        f"got {selected_backend!r}."
    )
